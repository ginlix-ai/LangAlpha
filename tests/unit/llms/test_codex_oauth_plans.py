"""A ChatGPT model a Free or Go plan can't run names the plans that can, so
the plan gate refuses it even when the account's own model list can't be read
(that read fails open)."""

import json
from pathlib import Path

import pytest

from src.server.services.llm.clients import plan_includes

_MODELS = json.loads((Path(__file__).resolve().parents[3] / "src/llms/manifest/models.json").read_text())

# Every paid plan code a ChatGPT sign-in token carries, as the Codex CLI reads
# them (`PlanType::from_raw_value` in codex-rs/protocol/src/auth.rs).
_PAID_PLANS = (
    "plus", "pro", "prolite", "promax", "team", "business", "self_serve_business_prolite",
    "self_serve_business_usage_based", "enterprise", "hc", "ent26", "enterprise_cbp_automation",
    "enterprise_cbp_usage_based", "edu", "education", "edu_plus", "edu_pro", "sci", "sci_plus",
)


@pytest.mark.parametrize("model", ["gpt-6.2-sol-oauth", "gpt-6.2-astra-oauth", "gpt-6.1-terra-oauth"])
def test_free_and_go_cannot_run_the_paid_chatgpt_models(model):
    info = _MODELS[model]
    assert not plan_includes(info, "free")
    assert not plan_includes(info, "go")
    for plan in _PAID_PLANS:
        assert plan_includes(info, plan), plan
    assert plan_includes(info, None)


def test_every_chatgpt_plan_runs_luna():
    assert plan_includes(_MODELS["gpt-6-luna-oauth"], "free")
