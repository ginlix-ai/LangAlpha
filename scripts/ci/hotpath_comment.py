#!/usr/bin/env python3
"""Generate a GitHub PR comment body from hot path integration test JUnit XML.

Usage:
    python scripts/ci/hotpath_comment.py test-results/backend-integration.xml [--metrics test-results/session_metrics.json]

Reads the JUnit XML produced by pytest, filters for test_message_hot_path
tests, and prints a Markdown comment to stdout.  When --metrics is provided,
includes actual operation timings from the sandbox metrics collector alongside
pytest's total test durations.
"""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path

# Map test class names to display categories
_CLASS_DISPLAY = {
    "TestColdWarmSessionPath": "Cold / Warm Session Path",
    "TestHasReadySession": "`has_ready_session` Edge Cases",
    "TestUpdateWorkspaceActivityConditional": "Conditional `update_workspace_activity`",
    "TestMarkUserDataStale": "`mark_user_data_stale`",
}

# Map test class names to a description of what the category tests
_CLASS_DESCRIPTION = {
    "TestColdWarmSessionPath": "Session resolution with real PostgreSQL + Daytona sandbox",
    "TestHasReadySession": "Sync check accuracy for various session states",
    "TestUpdateWorkspaceActivityConditional": "60-second conditional SQL UPDATE behavior",
    "TestMarkUserDataStale": "User data invalidation across workspaces",
}

_MODULE = "tests.integration.test_message_hot_path"


@dataclass
class TestResult:
    name: str
    class_name: str
    duration_s: float
    passed: bool
    failure_message: str = ""


@dataclass
class CategorySummary:
    display_name: str
    description: str
    tests: list[TestResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.tests)

    @property
    def passed(self) -> int:
        return sum(1 for t in self.tests if t.passed)

    @property
    def failed(self) -> int:
        return self.total - self.passed

    @property
    def total_duration_s(self) -> float:
        return sum(t.duration_s for t in self.tests)


def _fmt_duration(seconds: float) -> str:
    if seconds < 0.001:
        us = seconds * 1_000_000
        if us < 1:
            return f"{us:.2f}\u00b5s"
        return f"{us:.0f}\u00b5s"
    if seconds < 1.0:
        return f"{seconds * 1000:.0f}ms"
    return f"{seconds:.2f}s"


def _humanize_test_name(name: str) -> str:
    """Convert test_cold_path_creates_session to 'cold path creates session'."""
    return name.removeprefix("test_").replace("_", " ")


@dataclass
class OperationTiming:
    operation: str
    test_name: str
    duration_s: float
    success: bool


def parse_session_metrics(path: str) -> list[OperationTiming]:
    """Parse the sandbox metrics JSON and extract session category operations."""
    with open(path) as f:
        data = json.load(f)

    timings: list[OperationTiming] = []
    for op in data.get("operations", []):
        if op.get("category") != "session":
            continue
        timings.append(OperationTiming(
            operation=op["operation"],
            test_name=op.get("test_name", ""),
            duration_s=op.get("duration_s", 0.0),
            success=op.get("success", True),
        ))
    return timings


def _fmt_operation_name(name: str) -> str:
    """Format operation name for display: cold_create -> cold create."""
    return name.replace("_", " ")


def parse_junit_xml(path: str) -> list[TestResult]:
    """Parse JUnit XML and extract hot path test results."""
    tree = ET.parse(path)
    root = tree.getroot()

    results: list[TestResult] = []

    # Handle both <testsuites><testsuite>... and <testsuite>... root
    suites = root.findall(".//testsuite") if root.tag == "testsuites" else [root]

    for suite in suites:
        for tc in suite.findall("testcase"):
            classname = tc.get("classname", "")
            if _MODULE not in classname:
                continue

            name = tc.get("name", "")
            duration = float(tc.get("time", "0"))

            failure = tc.find("failure")
            error = tc.find("error")
            passed = failure is None and error is None
            msg = ""
            if failure is not None:
                msg = failure.get("message", "")
            elif error is not None:
                msg = error.get("message", "")

            # Extract the test class from the full classname
            # e.g., "tests.integration.test_message_hot_path.TestColdWarmSessionPath"
            parts = classname.split(".")
            test_class = parts[-1] if parts else ""

            results.append(TestResult(
                name=name,
                class_name=test_class,
                duration_s=duration,
                passed=passed,
                failure_message=msg,
            ))

    return results


def group_by_category(results: list[TestResult]) -> list[CategorySummary]:
    """Group test results by class name into display categories."""
    categories: dict[str, CategorySummary] = {}

    for result in results:
        cls = result.class_name
        if cls not in categories:
            categories[cls] = CategorySummary(
                display_name=_CLASS_DISPLAY.get(cls, cls),
                description=_CLASS_DESCRIPTION.get(cls, ""),
            )
        categories[cls].tests.append(result)

    # Return in defined order
    ordered = []
    for cls in _CLASS_DISPLAY:
        if cls in categories:
            ordered.append(categories.pop(cls))
    # Append any unknown categories at the end
    for cat in categories.values():
        ordered.append(cat)

    return ordered


def generate_comment(
    results: list[TestResult],
    timings: list[OperationTiming] | None = None,
) -> str:
    """Generate the markdown comment body."""
    if not results:
        return ""

    categories = group_by_category(results)
    total_tests = sum(c.total for c in categories)
    total_passed = sum(c.passed for c in categories)
    total_duration = sum(c.total_duration_s for c in categories)

    # Build a lookup from test_name to total test duration for the timings table
    test_durations: dict[str, float] = {}
    for r in results:
        test_durations[r.name.removeprefix("test_")] = r.duration_s

    lines: list[str] = []
    lines.append("## Hot Path Integration Test Results")
    lines.append("")

    # Operation timings table (actual metrics, not pytest durations)
    if timings:
        lines.append("### :stopwatch: Operation Timings")
        lines.append(
            "<sub>Actual operation duration vs pytest total"
            " (which includes fixture setup and cold path prerequisites)</sub>"
        )
        lines.append("")
        lines.append("| Operation | Actual | Test Total |")
        lines.append("|-----------|--------|------------|")
        for t in timings:
            actual = _fmt_duration(t.duration_s)
            total = _fmt_duration(test_durations[t.test_name]) if t.test_name in test_durations else "—"
            op_name = _fmt_operation_name(t.operation)
            lines.append(f"| {op_name} | **{actual}** | {total} |")
        lines.append("")

    for cat in categories:
        status = ":white_check_mark:" if cat.failed == 0 else ":x:"
        lines.append(f"### {status} {cat.display_name}")
        if cat.description:
            lines.append(f"<sub>{cat.description}</sub>")
        lines.append("")
        lines.append("| Test | Duration | Result |")
        lines.append("|------|----------|--------|")

        for test in cat.tests:
            icon = ":white_check_mark:" if test.passed else ":x:"
            name = _humanize_test_name(test.name)
            dur = _fmt_duration(test.duration_s)
            lines.append(f"| {name} | {dur} | {icon} |")

        lines.append("")

    # Summary line
    all_pass = total_passed == total_tests
    status_text = "100% pass" if all_pass else f"{total_passed}/{total_tests} passed"
    lines.append(
        f"**Total:** {total_tests} tests, {_fmt_duration(total_duration)}, {status_text}"
    )
    lines.append("")
    lines.append("---")
    lines.append("<sub>Generated by backend-integration CI</sub>")

    return "\n".join(lines)


def main():
    if len(sys.argv) < 2:
        print(
            f"Usage: {sys.argv[0]} <junit.xml> [--metrics <session_metrics.json>]",
            file=sys.stderr,
        )
        sys.exit(1)

    # Parse args: positional JUnit XML paths + optional --metrics flag
    xml_paths: list[str] = []
    metrics_path: str | None = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--metrics" and i + 1 < len(args):
            metrics_path = args[i + 1]
            i += 2
        else:
            xml_paths.append(args[i])
            i += 1

    all_results: list[TestResult] = []
    for path in xml_paths:
        if not Path(path).exists():
            print(f"Warning: {path} not found, skipping", file=sys.stderr)
            continue
        all_results.extend(parse_junit_xml(path))

    if not all_results:
        print("No hot path test results found in JUnit XML.", file=sys.stderr)
        sys.exit(1)

    timings: list[OperationTiming] | None = None
    if metrics_path and Path(metrics_path).exists():
        timings = parse_session_metrics(metrics_path)

    comment = generate_comment(all_results, timings=timings)
    print(comment)


if __name__ == "__main__":
    main()
