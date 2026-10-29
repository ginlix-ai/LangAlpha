"""Tell the price monitors that a thread's targets changed.

The Redis nudge reaches every worker but is a no-op without Redis; the
in-process request reaches this worker's monitor either way, so a single
worker without Redis still picks a change up at once. Both are hints: the
monitor's timed reload is what any change is guaranteed to reach.
"""

import logging

from src.server.services.market_targets.signals import publish_targets_changed

logger = logging.getLogger(__name__)


async def targets_changed(thread_id: str) -> None:
    await publish_targets_changed(thread_id)
    try:
        from src.server.services.price_monitor import PriceMonitorService

        PriceMonitorService.get_instance().request_targets_reload(thread_id)
    except Exception:
        logger.debug("[MarketTargets] in-process reload request failed", exc_info=True)

