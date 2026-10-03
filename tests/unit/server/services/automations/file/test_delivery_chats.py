"""A file's ``delivery`` may name one chat. The save checks each chat it
newly names with the channel gateway before it locks anything, lists every
refusal with the file's other problems, and stores the gateway's spelling."""

from __future__ import annotations

import pytest

from src.server.services.automations import lifecycle
from tests.unit.server.services.automations._check_target import (  # noqa: F401 - fixtures
    CHAT,
    CHAT_SPELLED,
    DISCORD_DM,
    REFUSAL,
    REFUSED_CHAT,
    SLACK_DM,
)
from tests.unit.server.services.automations.file._support import (
    BRIEF,
    CREATED,
    NEW,
    _create,
    _refusal,
    _row,
    _shown,
    _write,
)


@pytest.fixture
def gateway(check_target, db):
    # Each check notes whether a save's transaction was open around it.
    check_target.at = lambda: db.depth
    return check_target


class TestDeliveryChats:
    @pytest.mark.asyncio
    async def test_a_create_stores_the_chat_as_the_gateway_files_it(
        self, db, backend, gateway
    ):
        report = await _create(backend, {**NEW, "delivery": ["slack", CHAT_SPELLED]})

        assert report.startswith('Saved new.json: created "A"')
        assert db.rows[CREATED]["delivery_config"] == {"methods": ["slack", CHAT]}
        assert gateway.asked == [CHAT_SPELLED]
        # Checked before the save opened its transaction, and not again inside it.
        assert gateway.seen == [0]
        assert lifecycle.create_automation.await_args.kwargs["delivery_checked"] is True

    @pytest.mark.asyncio
    async def test_a_dm_address_saves_beside_its_app_name(self, db, backend, gateway):
        delivery = ["discord", DISCORD_DM, SLACK_DM]

        await _create(backend, {**NEW, "delivery": delivery})

        assert db.rows[CREATED]["delivery_config"] == {"methods": delivery}
        assert gateway.asked == [DISCORD_DM, SLACK_DM]

    @pytest.mark.asyncio
    async def test_an_update_stores_the_canonical_chat(self, db, backend, gateway):
        db.add(_row(delivery_config={"methods": ["slack"]}))

        await _write(
            backend, {**_shown(db.list()[0]), "delivery": ["slack", CHAT_SPELLED]}
        )

        assert lifecycle.update_automation.await_args.args[2] == {
            "delivery_config": {"methods": ["slack", CHAT]}
        }
        assert db.rows[BRIEF]["delivery_config"] == {"methods": ["slack", CHAT]}
        assert gateway.seen == [0]
        assert lifecycle.update_automation.await_args.kwargs["delivery_checked"] is True

    @pytest.mark.asyncio
    async def test_respelling_the_saved_chat_changes_nothing(
        self, db, backend, gateway
    ):
        db.add(_row(delivery_config={"methods": [CHAT]}))

        report = await _write(
            backend, {**_shown(db.list()[0]), "delivery": [CHAT_SPELLED]}
        )

        assert report.startswith("No changes")
        lifecycle.update_automation.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_chat_is_listed_with_the_files_other_problems(
        self, db, backend, gateway
    ):
        db.add(_row())

        error = await _refusal(
            backend,
            db,
            {**_shown(db.list()[0]), "delivery": [REFUSED_CHAT], "instruction": None},
        )

        assert error.problems == [
            ("delivery", f"{REFUSED_CHAT!r}: {REFUSAL}"),
            ("instruction", "can't be null"),
        ]

    @pytest.mark.asyncio
    async def test_a_chat_the_writer_was_shown_is_not_checked_again(
        self, db, backend, gateway
    ):
        """It was checked when it was saved; a chat gone since must not
        block an edit to anything else."""
        db.add(_row(delivery_config={"methods": [REFUSED_CHAT]}))

        await _write(backend, {**_shown(db.list()[0]), "description": "renamed"})

        assert gateway.asked == []
        assert db.rows[BRIEF]["description"] == "renamed"

    @pytest.mark.asyncio
    async def test_without_a_gateway_a_chat_is_refused(self, db, backend, no_gateway):
        error = await _refusal(
            backend,
            db,
            {**NEW, "delivery": [CHAT]},
            path=f"{backend.root_prefix}new.json",
        )

        assert error.problems == [
            (
                "delivery",
                f"{CHAT!r}: naming a chat needs a connected messaging service, and this server "
                'has none; name an app such as "slack" instead',
            )
        ]

    @pytest.mark.asyncio
    async def test_without_a_gateway_an_app_saves_as_before(
        self, db, backend, no_gateway
    ):
        report = await _create(backend, {**NEW, "delivery": ["slack"]})

        assert report.startswith('Saved new.json: created "A"')
        assert db.rows[CREATED]["delivery_config"] == {"methods": ["slack"]}
