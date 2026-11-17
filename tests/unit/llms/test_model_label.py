"""The model as the agent is told it runs on: display name and the name
`la models list` uses, then reasoning effort."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.llms.model_identity import client_label, model_label

_MANIFEST = {
    "listed-model": {"display_name": "Listed Model"},
    # Off the model list, which leaves out ``get_model_metadata``; still named.
    "unlisted-model": {"display_name": "Unlisted Model", "visible": False},
    "unnamed-model": {},
}


@pytest.fixture(autouse=True)
def _manifest():
    models = SimpleNamespace(get_model_config=_MANIFEST.get)
    with patch("src.llms.llm.LLM.get_model_config", return_value=models):
        yield


@pytest.mark.parametrize(
    ("name", "effort", "label"),
    [
        ("listed-model", None, "Listed Model (listed-model)"),
        ("listed-model", "high", "Listed Model (listed-model), reasoning effort high"),
        ("listed-model", "none", "Listed Model (listed-model), reasoning off"),
        ("unlisted-model", "low", "Unlisted Model (unlisted-model), reasoning effort low"),
        ("unnamed-model", None, "unnamed-model"),
        # A user-defined model has no entry; its name is all there is.
        ("My BYOK Model", "medium", "My BYOK Model, reasoning effort medium"),
        ("", "high", ""),
        (None, None, ""),
    ],
)
def test_model_label(name, effort, label):
    assert model_label(name, effort) == label


def test_a_client_is_labelled_by_what_the_factory_stamped_on_it():
    client = SimpleNamespace(
        metadata={"manifest_model": "listed-model", "reasoning_effort": "xhigh"}
    )
    assert client_label(client, "unlisted-model") == "Listed Model (listed-model), reasoning effort xhigh"


@pytest.mark.parametrize("metadata", [None, {}])
def test_a_client_the_factory_never_built_falls_back_to_the_name(metadata):
    assert client_label(SimpleNamespace(metadata=metadata), "listed-model") == "Listed Model (listed-model)"
    assert client_label(object(), "listed-model") == "Listed Model (listed-model)"
    assert client_label(object()) == ""
