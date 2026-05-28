#!/usr/bin/env python3
"""evi-toolkit / wakeup_check.py

EVI 流程的 **开发者诊断入口**。

⚠️  从 EVI 模板 v3.7.0 起，"扫产物 + 占位 + 持久化 + 缺件回喂 Agent" 的
    完整逻辑已经搬到框架层（``src/server/templates/finalize/runner.py``），
    在 ``TemplateOrchestrator._run_agent`` 里**自动**执行。
    Agent 正常使用 EVI 模板时**不需要手动调用本脚本**。

本脚本的剩余用途
----------------
1. **开发期手动诊断**：用 ``--check-only`` 看某个数据目录缺什么文件
2. **应急补救**：框架自动 finalize 异常（如 sandbox 断连）时，开发者手动
   登 sandbox / 本地跑一次 finalize 把 entry 推到 completed
3. **批量回填**：对老旧 entry 强制重做 finalize 时用 ``--force``

EXPECTED_FILES 仍保留一份本地副本，方便脚本独立运行（不依赖框架进程）；
但**模板的真理来源**是 ``src/server/templates/manifests/evi_strategy.py``
里的 ``_EVI_EXPECTED_FILES``。如果两边产生分歧，以模板里的为准 —— 框架
自动 finalize 用的就是那份。

Usage
-----
::

    # 仅看清单（最常用的开发者用法）
    python3 wakeup_check.py --data-dir data/{symbol_dir} --check-only

    # 手动跑一次 finalize（应急）
    python3 wakeup_check.py \\
        --entry-id <uuid> \\
        --data-dir data/{symbol_dir} \\
        --display-name "..." --symbol "..." --market "hk"

    # 强制 finalize（required 缺也推）
    python3 wakeup_check.py --entry-id <uuid> --data-dir ... --force

Exit codes
----------
  0  -- finalize 成功（completed 或 partial）
  1  -- finalize 失败（HTTP error 等）
  2  -- required 文件缺失，未尝试 finalize
  3  -- check-only 模式下发现 required 缺失
"""
from __future__ import annotations

import argparse
import json  # noqa: F401  -- kept for placeholder readability in spec
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


# ---------------------------------------------------------------------------
# Expected files spec
# ---------------------------------------------------------------------------

Level = Literal["required", "optional", "ignored"]


@dataclass(frozen=True)
class Expected:
    """A single expected artifact under data/{symbol_dir}/."""

    rel_path: str          # path relative to --data-dir
    level: Level           # required / optional / ignored
    description: str       # human-readable hint shown to Agent on miss
    placeholder: str | None = None  # auto-fill content for optional misses


# 顺序决定开发者看到 TODO 时的优先级（required 先列）
#
# ⚠️  这是本地副本，方便脚本独立运行。模板的真理来源是
#     src/server/templates/manifests/evi_strategy.py 的 _EVI_EXPECTED_FILES。
#     若有分歧以模板为准。框架自动 finalize 走的是模板那份清单。
EXPECTED_FILES: list[Expected] = [
    # ---- required ----（缺一不可，Agent 必须补齐才能 finalize）
    Expected(
        "facets.json",
        "required",
        "看板核心数据。必须有 fair_value / current_price / judgment 字段。",
    ),
    Expected(
        "reports/final.md",
        "required",
        "最终估值结论报告（前端「估值结论」Tab 主体内容）。",
    ),
    Expected(
        "reports/changelog.md",
        "required",
        "更新记录。即使是首次分析也要写一条『首次分析』。",
    ),
    Expected(
        "base/CHECKLIST.json",
        "required",
        "数据质量记分卡。跑 `build_checklist.py --data-dir <dir>` 生成。",
    ),

    # ---- optional ----（缺可以自动占位、finalize 为 partial）
    Expected(
        "quality.json",
        "optional",
        "4 维度定性分析结构化结论。强烈建议有 — 缺则前端定性卡为空。",
        placeholder='{"schema_version":"quality-1.0","dimensions":{},"summary":{}}',
    ),
    Expected(
        "reports/quality.md",
        "optional",
        "4 维度定性分析报告。",
        placeholder="# 定性分析\n\n> 本次未生成。请让 Agent 补 `evi-quality-analysis`。\n",
    ),
    Expected(
        "reports/company_overview.md",
        "optional",
        "公司产业调研总报告（single_segment 模式必需，multi_segment 模式仍建议有）。",
        placeholder="# 产业调研总报告\n\n> 本次未生成。\n",
    ),
    Expected(
        "reports/reverse_valuation.md",
        "optional",
        "反向估值报告。",
        placeholder="# 反向估值\n\n> 本次未生成。\n",
    ),
    Expected(
        "information/indexed_facts.json",
        "optional",
        "事实索引。跑 `format_facts.py --data-dir <dir>` 生成。",
        placeholder='{"facts":[]}',
    ),
    Expected(
        "business_segments.json",
        "optional",
        "业务分部识别结果。",
        placeholder='{"structure_type":"single_segment","segments":[],"n_segments":0}',
    ),
]


# ---------------------------------------------------------------------------
# Check logic
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    missing_required: list[Expected]
    missing_optional: list[Expected]
    present: list[Expected]

    @property
    def all_required_ok(self) -> bool:
        return not self.missing_required

    @property
    def is_fully_ok(self) -> bool:
        return not self.missing_required and not self.missing_optional


def check_files(data_dir: Path) -> CheckResult:
    """Walk EXPECTED_FILES against the filesystem."""
    missing_required: list[Expected] = []
    missing_optional: list[Expected] = []
    present: list[Expected] = []

    for spec in EXPECTED_FILES:
        if spec.level == "ignored":
            continue
        p = data_dir / spec.rel_path
        if p.exists() and (not p.is_file() or p.stat().st_size > 0):
            present.append(spec)
        elif spec.level == "required":
            missing_required.append(spec)
        else:
            missing_optional.append(spec)
    return CheckResult(
        missing_required=missing_required,
        missing_optional=missing_optional,
        present=present,
    )


def fill_optional_placeholders(data_dir: Path, missing: list[Expected]) -> list[str]:
    """Touch optional placeholders so the persist step has consistent inputs.

    Returns the list of rel_paths that got auto-filled (for changelog noting).
    """
    filled: list[str] = []
    for spec in missing:
        if spec.placeholder is None:
            continue
        p = data_dir / spec.rel_path
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(spec.placeholder, encoding="utf-8")
            filled.append(spec.rel_path)
        except Exception as e:
            print(
                f"[wakeup_check] WARN failed to write placeholder {spec.rel_path}: {e}",
                file=sys.stderr,
            )
    return filled


def append_wakeup_note_to_changelog(
    data_dir: Path,
    filled: list[str],
    missing_required: list[Expected],
    final_status: str,
) -> None:
    """Append a 1-line wakeup_check audit entry to reports/changelog.md."""
    changelog = data_dir / "reports" / "changelog.md"
    if not changelog.exists():
        return
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    parts = [f"\n### {ts} — wakeup_check"]
    parts.append(f"**finalize_status**: {final_status}")
    if filled:
        parts.append("**auto_filled_placeholders**:\n" + "\n".join(f"- {p}" for p in filled))
    if missing_required:
        parts.append("**unresolved_required_misses**:\n" + "\n".join(
            f"- {m.rel_path}" for m in missing_required
        ))
    if not filled and not missing_required:
        parts.append("(all expected artifacts present)")
    note = "\n".join(parts) + "\n"
    try:
        existing = changelog.read_text(encoding="utf-8")
        # 在顶部 H1 之后追加（changelog.md 习惯是新条目顶部追加）
        if existing.startswith("# "):
            head, _, rest = existing.partition("\n\n")
            changelog.write_text(head + "\n\n" + note + (rest or ""), encoding="utf-8")
        else:
            changelog.write_text(note + existing, encoding="utf-8")
    except Exception as e:
        print(
            f"[wakeup_check] WARN failed to update changelog: {e}",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Output formatting (Agent reads these)
# ---------------------------------------------------------------------------

def print_human_summary(res: CheckResult, data_dir: Path) -> None:
    total = len(res.present) + len(res.missing_required) + len(res.missing_optional)
    print(f"[wakeup_check] scanning {data_dir}")
    print(f"[wakeup_check] expected={total} present={len(res.present)} "
          f"missing_required={len(res.missing_required)} "
          f"missing_optional={len(res.missing_optional)}")

    if res.missing_required:
        print("\n❌ REQUIRED files missing — fix these before finalize:")
        for m in res.missing_required:
            print(f"   - {m.rel_path}\n       {m.description}")

    if res.missing_optional:
        print("\n⚠️  OPTIONAL files missing (will auto-fill placeholder + finalize partial):")
        for m in res.missing_optional:
            print(f"   - {m.rel_path}")

    if res.is_fully_ok:
        print("\n✅ All expected artifacts present.")


# ---------------------------------------------------------------------------
# Finalize via persist_evi_report.py
# ---------------------------------------------------------------------------

def run_persist(
    entry_id: str,
    data_dir: Path,
    display_name: str | None,
    symbol: str | None,
    market: str | None,
    status: str,
) -> tuple[bool, str]:
    """Call ``persist_evi_report.py`` as a subprocess. Same script as the
    one Agent is supposed to call manually — we just make sure it actually
    runs."""
    script = Path(__file__).parent / "persist_evi_report.py"
    if not script.exists():
        return False, f"persist_evi_report.py not found at {script}"

    cmd = [
        sys.executable, str(script),
        "--entry-id", entry_id,
        "--data-dir", str(data_dir),
        "--status", status,
    ]
    if display_name:
        cmd += ["--display-name", display_name]
    if symbol:
        cmd += ["--symbol", symbol]
    if market:
        cmd += ["--market", market]

    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        return False, "persist_evi_report.py timed out after 60s"
    except Exception as e:
        return False, f"persist_evi_report subprocess failed: {e}"

    out = (p.stdout or "") + (p.stderr or "")
    if p.returncode != 0:
        return False, f"persist_evi_report exit={p.returncode}:\n{out[:800]}"
    return True, out[-400:]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--entry-id", help="Template entry UUID (required unless --check-only)")
    ap.add_argument("--data-dir", required=True, help="data/{symbol_dir} path")
    ap.add_argument("--display-name")
    ap.add_argument("--symbol")
    ap.add_argument("--market")
    ap.add_argument(
        "--check-only", action="store_true",
        help="Only print the checklist, do not finalize",
    )
    ap.add_argument(
        "--force", action="store_true",
        help="Finalize even if required files are missing (use status=partial)",
    )
    args = ap.parse_args()

    # Friendly nudge — make it clear this is a dev tool, not a hot-path requirement.
    if not args.check_only:
        print(
            "[wakeup_check] NOTE: 从 EVI v3.7.0 起，framework 在 agent 跑完后会"
            "自动 finalize（见 src/server/templates/finalize/runner.py）。"
            "本脚本主要用于开发者诊断 / 应急补救。",
            file=sys.stderr,
        )

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        print(f"[wakeup_check] ERROR: data dir does not exist: {data_dir}", file=sys.stderr)
        return 2

    # Step 1: scan
    res = check_files(data_dir)
    print_human_summary(res, data_dir)

    if args.check_only:
        return 3 if res.missing_required else 0

    if not args.entry_id:
        print("\n[wakeup_check] ERROR: --entry-id required for finalize mode", file=sys.stderr)
        return 2

    # Step 2: decide
    if res.missing_required and not args.force:
        print(
            "\n❌ wakeup_check halted: required artifacts missing. "
            "Please generate them and re-run wakeup_check.\n"
            "   (Use --force to finalize anyway as 'partial'.)",
            file=sys.stderr,
        )
        return 2

    # Step 3: auto-fill optional placeholders
    filled = fill_optional_placeholders(data_dir, res.missing_optional)
    if filled:
        print(f"\n[wakeup_check] auto-filled {len(filled)} optional placeholder(s):")
        for f in filled:
            print(f"   ~ {f}")

    # Step 4: determine final status
    if res.missing_required:  # only reachable when --force
        final_status = "partial"
    elif res.missing_optional:
        final_status = "partial"
    else:
        final_status = "completed"

    # Step 5: log to changelog (best-effort)
    append_wakeup_note_to_changelog(
        data_dir, filled, res.missing_required, final_status,
    )

    # Step 6: actually finalize
    print(f"\n[wakeup_check] finalizing entry={args.entry_id} status={final_status} ...")
    ok, info = run_persist(
        args.entry_id, data_dir,
        args.display_name, args.symbol, args.market,
        final_status,
    )
    if ok:
        print(f"[wakeup_check] ✅ finalize OK\n{info}")
        return 0
    print(f"[wakeup_check] ❌ finalize FAILED:\n{info}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
