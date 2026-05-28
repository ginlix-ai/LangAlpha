"""Generic template finalize runner.

何时运行
--------
模板入口（``TemplateOrchestrator._run_agent``）在 agent generator drain
完之后调用本 runner。runner 负责：

1. 按模板声明的 ``FinalizeSpec.expected_files`` 检查 sandbox 内文件
2. 缺 ``optional`` 的自动写占位
3. 全齐 → 调模板声明的 ``persist_script`` 把 entry 推到 completed
4. 缺 ``required`` → **不**持久化，返回一段结构化消息让上层把它作为
   ``HumanMessage`` 重新喂给 Agent（再跑一轮，让 Agent 补齐）

设计哲学
--------
- **声明式**：runner 不知道具体业务，只读 spec
- **模板复用**：任何模板填 spec 都能用，不只是 EVI
- **可重入**：retry 时只看文件状态，不依赖 in-memory 状态
- **失败安全**：runner 自己任何异常都不影响主流程退出码
"""

from __future__ import annotations

import json
import shlex
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import structlog

from src.server.templates.registry import (
    FinalizeExpected,
    FinalizeSpec,
    TemplateDefinition,
)

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Outcome
# ---------------------------------------------------------------------------

FinalizeStatus = Literal["completed", "partial", "blocked", "failed", "skipped"]


@dataclass
class FinalizeOutcome:
    """Runner 的执行结果，供上层（template_orchestrator）决策。"""

    status: FinalizeStatus
    missing_required: list[FinalizeExpected] = field(default_factory=list)
    missing_optional: list[FinalizeExpected] = field(default_factory=list)
    auto_filled: list[str] = field(default_factory=list)
    agent_message: str | None = None  # 当 blocked 时，回填给 Agent 的提示
    detail: str = ""  # 人类可读的简短摘要（写日志用）

    @property
    def should_reinvoke(self) -> bool:
        """是否需要把 agent_message 注入再跑一轮 Agent。"""
        return self.status == "blocked" and bool(self.agent_message)


# ---------------------------------------------------------------------------
# Sandbox FS helpers (薄封装，便于测试 mock)
# ---------------------------------------------------------------------------


async def _file_exists_nonempty(sandbox: Any, path: str) -> bool:
    """sandbox 内文件存在且非空。

    用 bash ``test -s`` 一次 roundtrip，比下载整个文件快得多。
    """
    if sandbox is None:
        return False
    try:
        cmd = f"test -s {shlex.quote(path)}"
        res = await sandbox.execute_bash_command(
            command=cmd, timeout=15,
        )
        return int(res.get("exit_code") or 0) == 0
    except Exception:
        logger.debug("file_exists_check_failed", path=path, exc_info=True)
        return False


async def _write_text(sandbox: Any, path: str, content: str) -> bool:
    """sandbox 内写文本（覆盖）。会自动 mkdir 父目录。"""
    if sandbox is None:
        return False
    try:
        # 先 mkdir -p 父目录（best-effort，不阻断）
        import os
        parent = os.path.dirname(path)
        if parent:
            try:
                await sandbox.execute_bash_command(
                    command=f"mkdir -p {shlex.quote(parent)}",
                    timeout=10,
                )
            except Exception:
                pass
        ok = await sandbox.awrite_file_text(path, content)
        return bool(ok)
    except Exception:
        logger.debug("write_text_failed", path=path, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Core scan + decide
# ---------------------------------------------------------------------------


@dataclass
class _ScanResult:
    missing_required: list[FinalizeExpected]
    missing_optional: list[FinalizeExpected]
    present: list[FinalizeExpected]


async def _scan_expected(
    sandbox: Any,
    data_dir: str,
    expected: tuple[FinalizeExpected, ...],
) -> _ScanResult:
    missing_required: list[FinalizeExpected] = []
    missing_optional: list[FinalizeExpected] = []
    present: list[FinalizeExpected] = []

    for spec in expected:
        full = f"{data_dir.rstrip('/')}/{spec.rel_path}"
        ok = await _file_exists_nonempty(sandbox, full)
        if ok:
            present.append(spec)
        elif spec.level == "required":
            missing_required.append(spec)
        else:
            missing_optional.append(spec)
    return _ScanResult(missing_required, missing_optional, present)


async def _fill_placeholders(
    sandbox: Any,
    data_dir: str,
    missing: list[FinalizeExpected],
) -> list[str]:
    filled: list[str] = []
    for spec in missing:
        if spec.placeholder is None:
            continue
        full = f"{data_dir.rstrip('/')}/{spec.rel_path}"
        ok = await _write_text(sandbox, full, spec.placeholder)
        if ok:
            filled.append(spec.rel_path)
    return filled


async def _append_changelog_audit(
    sandbox: Any,
    data_dir: str,
    filled: list[str],
    missing_required: list[FinalizeExpected],
    final_status: str,
) -> None:
    """Best-effort：在 reports/changelog.md 追加一条 finalize 审计。"""
    cl_path = f"{data_dir.rstrip('/')}/reports/changelog.md"
    try:
        existing = await sandbox.aread_file_text(cl_path)
    except Exception:
        existing = None
    if not existing:
        return

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    lines = [f"\n### {ts} — finalize_check"]
    lines.append(f"**finalize_status**: {final_status}")
    if filled:
        lines.append(
            "**auto_filled_placeholders**:\n"
            + "\n".join(f"- {p}" for p in filled)
        )
    if missing_required:
        lines.append(
            "**unresolved_required_misses**:\n"
            + "\n".join(f"- {m.rel_path}" for m in missing_required)
        )
    if not filled and not missing_required:
        lines.append("(all expected artifacts present)")
    note = "\n".join(lines) + "\n"

    # 新条目插入到 H1 之后
    if existing.startswith("# "):
        head, _, rest = existing.partition("\n\n")
        new_content = head + "\n\n" + note + (rest or "")
    else:
        new_content = note + existing

    try:
        await _write_text(sandbox, cl_path, new_content)
    except Exception:
        logger.debug("changelog_audit_failed", path=cl_path, exc_info=True)


# ---------------------------------------------------------------------------
# Persist invocation
# ---------------------------------------------------------------------------


async def _run_persist(
    sandbox: Any,
    spec: FinalizeSpec,
    entry_id: str,
    entry_key: str,
    display_name: str | None,
    params: dict[str, Any],
    data_dir: str,
    status: str,
) -> tuple[bool, str]:
    """通过 sandbox bash 调持久化脚本。返回 (success, output_tail)。"""
    args = [
        "python3",
        spec.persist_script,
        "--entry-id", entry_id,
        "--data-dir", data_dir,
        "--status", status,
    ]
    if display_name:
        args.extend(["--display-name", display_name])

    if spec.persist_args_builder is not None:
        try:
            extra = spec.persist_args_builder(entry_key, display_name, params)
            args.extend(str(a) for a in extra)
        except Exception:
            logger.warning("persist_args_builder_failed", exc_info=True)

    cmd = " ".join(shlex.quote(a) for a in args)
    try:
        res = await sandbox.execute_bash_command(
            command=cmd, timeout=120,
        )
    except Exception as e:
        return False, f"persist subprocess raised: {e!r}"

    out = (res.get("stdout") or "") + (res.get("stderr") or "")
    exit_code = int(res.get("exit_code") or 0)
    if exit_code != 0:
        return False, f"exit={exit_code}\n{out[-800:]}"
    return True, out[-400:]


# ---------------------------------------------------------------------------
# Agent message builder (when blocked)
# ---------------------------------------------------------------------------


def _build_blocked_message(
    missing_required: list[FinalizeExpected],
    missing_optional: list[FinalizeExpected],
    data_dir: str,
) -> str:
    """组装回喂给 Agent 的 HumanMessage 内容。

    目标：让 Agent 一看就知道差什么、怎么补、补完后再做什么。
    """
    parts = [
        "[FINALIZE_CHECK] 你的任务还**没有真正完成**。"
        f"数据目录 `{data_dir}` 缺少以下 required 产物：",
        "",
    ]
    for m in missing_required:
        parts.append(f"- **{m.rel_path}** — {m.description}")

    if missing_optional:
        parts.append("")
        parts.append("以下 optional 产物也缺（不阻塞但建议补）：")
        for m in missing_optional:
            parts.append(f"- {m.rel_path} — {m.description}")

    parts.extend(
        [
            "",
            "**下一步**：",
            "1. 逐项生成上述缺失的 required 文件（可直接 Write，也可调对应的 skill 脚本）",
            "2. 完成后**不需要**手动调持久化脚本，系统会自动再次检查并 finalize",
            "3. 如果某个 required 文件因为外部原因无法生成（如数据源 404），"
            "请在 `reports/changelog.md` 顶部追加一条说明并 Write 一个最小可解释的占位文件",
            "",
            "注意：你正处于"
            "**finalize 重试轮**——补齐后即可结束本任务，不要重新跑全流程。",
        ]
    )
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Public entry
# ---------------------------------------------------------------------------


async def run_template_finalize(
    template: TemplateDefinition,
    entry_id: str,
    entry_key: str,
    display_name: str | None,
    params: dict[str, Any],
    sandbox: Any,
) -> FinalizeOutcome:
    """模板 finalize 主入口。

    Args:
      template: 模板定义（必须设置了 finalize_spec，否则返回 skipped）
      entry_id: 模板 entry UUID
      entry_key / display_name / params: 模板入参
      sandbox: PTCSandbox 实例（用来跑 bash / 读写文件）

    Returns:
      FinalizeOutcome — 上层根据 status / should_reinvoke 决定是否再跑一轮
    """
    spec = template.finalize_spec
    if spec is None:
        return FinalizeOutcome(status="skipped", detail="no finalize_spec")

    if sandbox is None:
        logger.warning(
            "finalize_runner_no_sandbox",
            template=template.id,
            entry_id=entry_id,
        )
        return FinalizeOutcome(status="failed", detail="no sandbox available")

    # 1. 解析 data_dir
    try:
        data_dir = spec.data_dir_builder(entry_key, display_name, params)
    except Exception as e:
        logger.warning(
            "finalize_data_dir_builder_failed",
            template=template.id,
            error=str(e),
            exc_info=True,
        )
        return FinalizeOutcome(status="failed", detail=f"data_dir_builder: {e}")

    logger.info(
        "[FINALIZE] scan starting",
        template=template.id,
        entry_id=entry_id,
        data_dir=data_dir,
    )

    # 2. 扫描
    scan = await _scan_expected(sandbox, data_dir, spec.expected_files)

    logger.info(
        "[FINALIZE] scan result",
        template=template.id,
        entry_id=entry_id,
        present=len(scan.present),
        missing_required=len(scan.missing_required),
        missing_optional=len(scan.missing_optional),
    )

    # 3. required 缺 → blocked，回填 agent 消息
    if scan.missing_required:
        msg = _build_blocked_message(
            scan.missing_required, scan.missing_optional, data_dir
        )
        # blocked 也写一条 changelog（如果 changelog 存在）
        await _append_changelog_audit(
            sandbox, data_dir, [], scan.missing_required, "blocked",
        )
        return FinalizeOutcome(
            status="blocked",
            missing_required=scan.missing_required,
            missing_optional=scan.missing_optional,
            agent_message=msg,
            detail=f"missing {len(scan.missing_required)} required artifact(s)",
        )

    # 4. optional 缺 → 自动占位
    filled = await _fill_placeholders(sandbox, data_dir, scan.missing_optional)

    # 5. 决定 final status
    final_status = "partial" if scan.missing_optional else "completed"

    # 6. 追加 changelog 审计
    await _append_changelog_audit(
        sandbox, data_dir, filled, [], final_status,
    )

    # 7. 调持久化脚本
    ok, info = await _run_persist(
        sandbox, spec,
        entry_id, entry_key, display_name, params, data_dir,
        final_status,
    )
    if not ok:
        logger.warning(
            "[FINALIZE] persist failed",
            template=template.id,
            entry_id=entry_id,
            info=info,
        )
        return FinalizeOutcome(
            status="failed",
            missing_optional=scan.missing_optional,
            auto_filled=filled,
            detail=f"persist failed: {info[:200]}",
        )

    logger.info(
        "[FINALIZE] success",
        template=template.id,
        entry_id=entry_id,
        final_status=final_status,
        auto_filled=len(filled),
    )
    return FinalizeOutcome(
        status=final_status,  # type: ignore[arg-type]
        missing_optional=scan.missing_optional,
        auto_filled=filled,
        detail=f"finalize_ok ({final_status})",
    )
