"""Seam: the usage ledger's sweep and delete-time fold, on this worker's providers.

One file of the ComputerManager split; see the package __init__. The ledger
itself is ``services/computer_usage``; this only lends it the providers the
manager already caches per backend.
"""

import logging
from typing import Any, Dict

from src.server.services.computer_manager._types import ComputerBinding
from src.server.services.computer_usage import (
    fold_before_delete,
    note_sandbox_replaced,
    run_usage_sweep,
)

logger = logging.getLogger(__name__)


class UsageMixin:
    def _usage_provider(self, run: Dict[str, Any]):
        """The provider a claimed run's metrics are read through."""
        return self._cached_provider(
            ComputerBinding(
                workspace_id="",
                computer_id=str(run["computer_id"]),
                kind=run["provider_kind"],
                root_dir=run.get("root_dir"),
                provider_config=run.get("provider_config") or None,
            )
        )

    async def sweep_usage(self) -> None:
        """Reconcile the run ledger and fold in metrics; never raises."""
        try:
            await run_usage_sweep(self._usage_provider)
        except Exception as e:
            logger.warning(f"Usage sweep failed: {type(e).__name__}: {e}")

    async def _fold_usage_before_delete(self, computer_id: str) -> None:
        """The machine's last metrics read, while its sandbox still exists.

        Called on the delete path, which must not fail for it; bounded and
        never raises (``fold_before_delete``).
        """
        await fold_before_delete(self._usage_provider, computer_id)

    async def _end_replaced_usage_run(self, computer_id: str, sandbox_id: str) -> None:
        """End the run on a sandbox a replacement deleted; never raises."""
        await note_sandbox_replaced(computer_id, sandbox_id)
