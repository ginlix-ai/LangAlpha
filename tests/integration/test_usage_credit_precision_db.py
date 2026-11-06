"""A turn's usage row takes a credit total of 10,000 or more, on a real Postgres.

The credit columns were NUMERIC(10,6), so a row worth 10 USD of model spend at
the default rate failed to insert, and with it the finalize that carries it.
"""

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.server.database.conversation import create_usage_record

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def test_a_usage_row_keeps_a_five_figure_credit_total(test_db_pool, cleanup_tables):
    usage_id = str(uuid.uuid4())
    async with test_db_pool.connection() as conn:
        await create_usage_record(
            {
                "conversation_usage_id": usage_id,
                "conversation_response_id": str(uuid.uuid4()),
                "user_id": "user-1",
                "conversation_thread_id": str(uuid.uuid4()),
                "workspace_id": str(uuid.uuid4()),
                "msg_type": "ptc",
                "status": "completed",
                "token_usage": {},
                "infrastructure_usage": None,
                "token_credits": 12345.678901,
                "infrastructure_credits": 250.5,
                "total_credits": 12596.178901,
                "is_byok": False,
                "created_at": datetime.now(timezone.utc),
            },
            conn=conn,
        )
        row = await (
            await conn.execute(
                "SELECT token_credits, infrastructure_credits, total_credits "
                "FROM conversation_usages WHERE conversation_usage_id = %s",
                (usage_id,),
            )
        ).fetchone()

    assert row == {
        "token_credits": Decimal("12345.678901"),
        "infrastructure_credits": Decimal("250.5"),
        "total_credits": Decimal("12596.178901"),
    }
