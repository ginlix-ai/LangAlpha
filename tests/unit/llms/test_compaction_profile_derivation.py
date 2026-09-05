"""Contract for the compaction preset a model gets when nobody has chosen one.

The preset used to be one YAML number for every model, so a 200k model and a
1M model compacted at the same point. These lock the properties that would put
that back silently: a band that stops covering a window, a derived name with no
bundle behind it, and a user's own choice losing to the derivation.
"""

import pytest

from src.llms import LLM
from src.llms.preferences import COMPACTION_PROFILE_BANDS
from ptc_agent.config.agent import COMPACTION_PROFILES


@pytest.fixture(scope="module")
def config():
    return LLM.get_model_config()


class TestBandTable:
    def test_every_band_names_a_real_preset(self):
        """The bands live in ``src.llms.preferences`` and the bundles in
        ``ptc_agent`` — that module boundary is deliberate, so nothing but this
        checks that a derived name can actually be looked up."""
        for _, profile in COMPACTION_PROFILE_BANDS:
            assert profile in COMPACTION_PROFILES

    def test_bands_descend_and_reach_zero(self):
        floors = [floor for floor, _ in COMPACTION_PROFILE_BANDS]
        assert floors == sorted(floors, reverse=True)
        assert floors[-1] == 0, "a window below every floor must still resolve"

    def test_wider_windows_never_compact_earlier(self):
        """The whole point of deriving: more context means a later trigger."""
        thresholds = [
            COMPACTION_PROFILES[p]["token_threshold"]
            for _, p in COMPACTION_PROFILE_BANDS
        ]
        assert thresholds == sorted(thresholds, reverse=True)

    @pytest.mark.parametrize(
        "context,expected",
        [
            (32_000, "aggressive"),
            (199_999, "aggressive"),
            (200_000, "moderate"),
            (399_999, "moderate"),
            (400_000, "extended"),
            (999_999, "extended"),
            (1_000_000, "relaxed"),
            (1_050_000, "relaxed"),
        ],
    )
    def test_boundaries(self, config, context, expected, monkeypatch):
        monkeypatch.setitem(config.llm_config, "_probe", {"context": context})
        assert config.get_compaction_profile("_probe") == expected


class TestDerivation:
    def test_declaration_beats_the_band(self, config, monkeypatch):
        """The manifest field is the escape hatch for a model the band gets
        wrong; it has to win or the hatch does nothing."""
        monkeypatch.setitem(
            config.llm_config,
            "_probe",
            {"context": 1_000_000, "compaction_profile": "aggressive"},
        )
        assert config.get_compaction_profile("_probe") == "aggressive"

    def test_no_window_declares_nothing(self, config, monkeypatch):
        """Silence, not a guess: a model whose capacity the manifest does not
        state keeps the deployment default rather than being sorted into a band."""
        monkeypatch.setitem(config.llm_config, "_probe", {"provider": "x"})
        assert config.get_compaction_profile("_probe") is None

    def test_unknown_model(self, config):
        assert config.get_compaction_profile("no-such-model") is None

    def test_every_model_with_a_window_resolves(self, config):
        for name, info in config.llm_config.items():
            if not isinstance(info, dict) or not isinstance(info.get("context"), int):
                continue
            assert config.get_compaction_profile(name) in COMPACTION_PROFILES, name


class TestMetadataPayload:
    def test_published_so_the_client_never_derives(self, config):
        """The frontend prints this to name a row's default. It must arrive
        resolved, or the band table gets a second implementation in TypeScript."""
        metadata = config.get_model_metadata()
        assert metadata, "no visible models to check"
        for name, entry in metadata.items():
            expected = config.get_compaction_profile(name)
            assert entry.get("compaction_profile") == expected, name
