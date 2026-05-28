"""Template finalize runner — 模板通用 "跑完检查 + 强制持久化" 能力。

具体清单（哪些文件 required、哪些 optional、持久化脚本路径等）由各模板
在 ``TemplateDefinition.finalize_spec`` 里声明。本模块只提供执行器。
"""

from .runner import FinalizeOutcome, run_template_finalize

__all__ = ["FinalizeOutcome", "run_template_finalize"]
