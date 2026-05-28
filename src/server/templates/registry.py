"""Template registry — central registration point for all templates.

Adding a new template:
  1. Create ``src/server/templates/manifests/your_template.py`` with a
     ``TemplateDefinition`` constant.
  2. Import + add it to ``TEMPLATE_REGISTRY`` below.
  3. Add the matching frontend folder under ``web/src/templates/<id>/``.

That's it. The orchestrator, REST routes, and dashboard UI are entirely
generic and read this registry.

Template definition contract (what a template author writes):
  - manifest          → public-facing form fields / description / metadata
  - initial_prompt_template  → what to say to the agent on first run
  - agent_md_template → what to put in the workspace's agent.md (optional)
  - workspace_name_builder   → how to name the workspace (optional)
  - params_enricher   → derive extra prompt variables from user input (optional)

All of these are pure data / pure functions. The orchestrator is 100% generic
and never inspects template-specific logic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from src.server.models.template import TemplateManifest


# ---------------------------------------------------------------------------
# Finalize spec — 模板的"最后必须做的事"
# ---------------------------------------------------------------------------
# 这是模板通用能力（框架层），但具体清单是模板自己声明的（声明式）。
# 框架的 finalize runner 读这份 spec 决定：
#   - 哪些文件必须有
#   - 缺哪些就告诉 Agent 怎么补
#   - 全齐时调哪个脚本把 entry 推到 completed
#
# 任何模板想接入"跑完自动持久化 + 缺件回填消息给 AI"机制，只要在
# TemplateDefinition.finalize_spec 里填这份 spec 即可，不需要自己写脚本。


FinalizeLevel = Literal["required", "optional"]


@dataclass(frozen=True)
class FinalizeExpected:
    """一项预期产出（在 sandbox 内、相对于 data_dir 的路径）。"""

    rel_path: str
    level: FinalizeLevel
    description: str
    placeholder: str | None = None  # 仅 optional 缺失时自动写入


@dataclass(frozen=True)
class FinalizeSpec:
    """模板 finalize 配置。

    Fields:
      data_dir_builder:
        ``(entry_key, display_name, params) -> sandbox_path``。
        返回 sandbox 内的数据根目录（如 ``data/{symbol_dir}``），
        所有 expected_files 的 rel_path 都相对这个目录解析。

      expected_files:
        预期产出清单（required + optional）。

      persist_script:
        sandbox 内调用的持久化脚本路径（如
        ``.agents/skills/evi-toolkit/scripts/persist_evi_report.py``）。
        runner 会用 ``python3 <persist_script> --entry-id ... --data-dir
        ... --status completed|partial`` 调它。

      persist_args_builder:
        可选 ``(entry_key, display_name, params) -> list[str]``，
        返回**额外**的命令行参数（如 ``--symbol ... --market ...``）。
        默认只传 entry-id / data-dir / status / display-name。

      max_retries:
        缺 required 时，最多让 Agent 再跑几轮（默认 1 = 主跑 + 1 次补救）。
    """

    data_dir_builder: Callable[[str, str | None, dict[str, Any]], str]
    expected_files: tuple[FinalizeExpected, ...]
    persist_script: str
    persist_args_builder: (
        Callable[[str, str | None, dict[str, Any]], list[str]] | None
    ) = None
    max_retries: int = 1


# Default agent.md template used when a template doesn't supply its own.
# Placeholders: {workspace_name}, {display_name}, {entry_key}, {template_name}
_DEFAULT_AGENT_MD = """\
---
workspace_name: {workspace_name}
description: [{template_name}] {entry_key}
---

# {workspace_name}

<!--
This is a starter template. Replace these comments with real content
as you work. The system prompt has full guidelines on what to maintain.
-->

## Thread Index

## Key Findings

## File Index
"""


@dataclass(frozen=True)
class TemplateDefinition:
    """Internal definition of a template (richer than the public manifest).

    Fields:
      manifest:
        The public-facing manifest exposed via /api/v1/templates.

      initial_prompt_template:
        ``str.format``-style template with ``{key}`` placeholders.
        Filled with ``display_name``, ``entry_key``, ``entry_id``,
        ``symbol_dir`` and any ``params`` keys (enriched by params_enricher).
        Sent as the first user message to the agent on instantiation.

      agent_md_template:
        Optional. ``str.format``-style template for the workspace's agent.md.
        Injected into every LLM call via WorkspaceContextMiddleware.
        Placeholders available: same as initial_prompt_template PLUS
        ``{workspace_name}``, ``{template_name}``.
        If None, _DEFAULT_AGENT_MD is used.

      workspace_name_builder:
        Optional. ``(entry_key, display_name, params) -> str``.
        Defaults to ``display_name or entry_key``.

      params_enricher:
        Optional. ``(entry_key, display_name, params) -> dict``.
        Produces *extra* derived keys for both prompt and agent.md templates.
        Merged AFTER user params, so it can override defaults.

      release_notes:
        Version upgrade release notes. Dict keyed by version string.
        Each value: {summary, changes: [...], suggested_actions: [{label, prompt}]}
        Used by the upgrade flow to tell user + agent what changed.

      allowed_skill_names:
        Optional skill whitelist for this template's workspace sandbox.
        - None (default): all local skills under ./skills/* are uploaded
          (legacy behavior, same as ordinary chat workspaces).
        - set[str]: only the listed skill directory names are uploaded
          to the template workspace's sandbox. Use this to keep the
          ``.agents/skills/`` directory clean (avoid mixing template-specific
          skills with generic ones that might confuse the agent).

          Names refer to the directory name under ``skills/``, e.g.
          ``"evi-data-orchestrator"``. Skills in SKILL_REGISTRY that match
          the names are still uploaded; skills outside the whitelist are
          skipped during the per-workspace upload.
    """

    manifest: TemplateManifest
    initial_prompt_template: str
    agent_md_template: str | None = field(default=None)
    workspace_name_builder: Callable[
        [str, str | None, dict[str, Any]], str
    ] | None = None
    params_enricher: Callable[
        [str, str | None, dict[str, Any]], dict[str, Any]
    ] | None = None
    seed_files_builder: Callable[
        [str, str | None, dict[str, Any]], list[tuple[str, str]]
    ] | None = None
    # Release notes for version upgrades
    # Format: {"3.1.0": {"summary": "...", "changes": [...], "suggested_actions": [...]}}
    release_notes: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Skill whitelist for the template workspace's sandbox.
    # None = upload everything (legacy). A set keeps the sandbox lean.
    allowed_skill_names: set[str] | None = field(default=None)

    # Finalize spec — 模板的"最后必须做的事"。
    # None = 模板不接入 finalize runner（保持旧行为：靠 Agent 自觉调 persist）。
    # 提供则在 _run_agent generator 结束后由 finalize.runner 接管。
    finalize_spec: FinalizeSpec | None = field(default=None)

    @property
    def id(self) -> str:
        return self.manifest.id

    def _build_ctx(
        self,
        entry_key: str,
        display_name: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Build the shared formatting context for prompt and agent.md."""
        ctx: dict[str, Any] = {
            "entry_key": entry_key,
            "display_name": display_name or entry_key,
            "workspace_name": self.build_workspace_name(entry_key, display_name, params),
            "template_name": self.manifest.name,
            **params,
        }
        if self.params_enricher:
            ctx.update(self.params_enricher(entry_key, display_name, params))
        return ctx

    def build_workspace_name(
        self, entry_key: str, display_name: str | None, params: dict[str, Any]
    ) -> str:
        if self.workspace_name_builder:
            return self.workspace_name_builder(entry_key, display_name, params)
        return display_name or entry_key

    def build_initial_prompt(
        self, entry_key: str, display_name: str | None, params: dict[str, Any]
    ) -> str:
        """Format the initial prompt with the context derived from entry params."""
        ctx = self._build_ctx(entry_key, display_name, params)
        try:
            return self.initial_prompt_template.format(**ctx)
        except KeyError as e:
            raise ValueError(
                f"Template {self.id!r} prompt expected {{{e.args[0]}}} but "
                f"it was not in context keys={list(ctx)}"
            ) from e

    def build_agent_md(
        self, entry_key: str, display_name: str | None, params: dict[str, Any]
    ) -> str:
        """Format the agent.md content for a new template workspace.

        Falls back to _DEFAULT_AGENT_MD when the template has no custom
        agent_md_template.
        """
        template = self.agent_md_template or _DEFAULT_AGENT_MD
        ctx = self._build_ctx(entry_key, display_name, params)
        try:
            return template.format(**ctx)
        except KeyError as e:
            raise ValueError(
                f"Template {self.id!r} agent_md_template expected {{{e.args[0]}}} "
                f"but it was not in context keys={list(ctx)}"
            ) from e

    def build_seed_files(
        self, entry_key: str, display_name: str | None, params: dict[str, Any]
    ) -> list[tuple[str, str]]:
        """Return ``[(sandbox_path, content), ...]`` extra seed files.

        Empty list when the template doesn't provide a seed_files_builder.
        Errors are caught here so a single bad seed file can't break workspace
        creation — the orchestrator only logs and skips.
        """
        if not self.seed_files_builder:
            return []
        try:
            return list(self.seed_files_builder(entry_key, display_name, params))
        except Exception:
            # Caller logs; we just bail.
            return []


# ---------------------------------------------------------------------------
# Registered templates
# ---------------------------------------------------------------------------

# Imports here are intentionally local (not at module top) so a malformed
# manifest doesn't blow up the whole server on import.
from src.server.templates.manifests.evi_strategy import (  # noqa: E402
    EVI_STRATEGY,
)


TEMPLATE_REGISTRY: dict[str, TemplateDefinition] = {
    EVI_STRATEGY.id: EVI_STRATEGY,
}


def get_template(template_id: str) -> TemplateDefinition | None:
    """Return the definition for ``template_id`` or None if unknown."""
    return TEMPLATE_REGISTRY.get(template_id)


def list_template_manifests() -> list[TemplateManifest]:
    """Return the public manifests of all registered templates."""
    return [d.manifest for d in TEMPLATE_REGISTRY.values()]

