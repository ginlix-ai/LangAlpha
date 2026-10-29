"""The reconciliation sweep's grace floor, pinned against the relay it waits on."""

import pytest
from pydantic import ValidationError

from src.config.models import OrderReconcileConfig
from src.server.services.egress.relay import WALL_CLOCK_S


def test_submitting_grace_floor_outlasts_the_relay_wall_clock():
    field = OrderReconcileConfig.model_fields["submitting_grace_seconds"]
    floor = next(m.ge for m in field.metadata if hasattr(m, "ge"))
    # A grace inside the wall clock lets the sweep judge a call still in flight.
    assert floor > WALL_CLOCK_S


def test_a_grace_inside_the_wall_clock_is_refused():
    with pytest.raises(ValidationError):
        OrderReconcileConfig(submitting_grace_seconds=int(WALL_CLOCK_S))
