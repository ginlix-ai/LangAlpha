"""``read_standing`` patched to an account under a given policy."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, patch

from src.server.services.account import Standing
from src.server.services.account_contract import Account
from src.server.services.policy import Policy


@contextmanager
def standing_under(policy: Policy | None):
    """Every policy ask answered as ``policy`` (an unknown account for None):
    its organization layer on the account and its deployment layer as the
    deployment's, so the merge reads it back with who set what. Yields the
    patched ``read_standing``."""
    layers = dict(policy.layers) if policy is not None else {}
    account = None if policy is None else Account(policy=layers.get("organization"))
    with (
        patch(
            "src.server.services.account.deployment_policy",
            return_value=layers.get("deployment"),
        ),
        patch(
            "src.server.services.account.read_standing",
            new_callable=AsyncMock,
            return_value=Standing(account, account),
        ) as read,
    ):
        yield read
