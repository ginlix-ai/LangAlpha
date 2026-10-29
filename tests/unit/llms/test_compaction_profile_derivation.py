"""Contract for the compaction preset a model gets when nobody has chosen one.

The preset used to be one YAML number for every model, so a 200k model and a
1M model compacted at the same point. These lock the properties that would put
that back silently: a band that stops covering a window, a derived name with no
bundle behind it, and a user's own choice losing to the derivation.
"""

import pytest

from src.llms import LLM
from src.llms.preferences import COMPACTION_PROFILE_BANDS, COMPACTION_PROFILE_NAMES
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

    def test_names_match_the_bundles_in_order(self):
        """The names validate stored preferences and the bundles apply them;
        a name with no bundle would be accepted and then do nothing."""
        assert COMPACTION_PROFILE_NAMES == tuple(COMPACTION_PROFILES)
        thresholds = [p["token_threshold"] for p in COMPACTION_PROFILES.values()]
        assert thresholds == sorted(set(thresholds))

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
            (1_000_000, "spacious"),
            (1_050_000, "spacious"),
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

    def test_native_compaction_offered_where_the_provider_compacts(self, config):
        """The settings offer the choice only for a model that carries this, so
        it must name every model the middleware could compact natively."""
        from src.llms.provider_compaction import native_compaction_kind

        metadata = config.get_model_metadata()
        offered = {}
        for name, entry in metadata.items():
            info = config.llm_config[name]
            expected = native_compaction_kind(
                info, config.flat_providers.get(info.get("provider"))
            )
            assert entry.get("native_compaction") == expected, name
            if expected:
                offered[name] = expected
        assert set(offered.values()) == {"anthropic", "openai"}


class TestWindowCap:
    """An account-wide preset reaches every model, so it is capped per model
    rather than trusted to fit."""

    @staticmethod
    def _applied(profile, entry):
        from types import SimpleNamespace

        from src.llms.preferences import Tuning
        from src.server.services.llm.config import apply_compaction

        config = SimpleNamespace(
            compaction=SimpleNamespace(token_threshold=0, profile=None),
        )
        apply_compaction(config, Tuning(compaction_profile=profile), entry)
        return config.compaction

    def test_a_preset_wider_than_the_window_is_capped(self):
        applied = self._applied("maximal", {"context": 400_000})
        assert applied.token_threshold == 300_000
        assert applied.profile == "maximal"

    def test_a_preset_that_fits_is_untouched(self):
        applied = self._applied("expansive", {"context": 1_000_000})
        assert applied.token_threshold == COMPACTION_PROFILES["expansive"]["token_threshold"]

    def test_no_window_no_cap(self):
        applied = self._applied("maximal", {"provider": "x"})
        assert applied.token_threshold == COMPACTION_PROFILES["maximal"]["token_threshold"]


class TestNativeChoice:
    """On wherever it is offered, until the user turns it off for the model.
    The feature and the running client are weighed by the middleware."""

    @staticmethod
    def _native(tuning, entry, *, custom=False):
        from types import SimpleNamespace

        from src.server.services.llm.config import apply_compaction

        config = SimpleNamespace(
            compaction=SimpleNamespace(
                token_threshold=0, profile=None, native="stale"
            ),
        )
        apply_compaction(config, tuning, entry, custom=custom)
        return config.compaction.native

    @pytest.mark.parametrize(("choice", "expected"), [(None, True), (True, True), (False, False)])
    def test_only_an_explicit_off_stands(self, choice, expected):
        from src.llms.preferences import Tuning

        native = self._native(Tuning(native_compaction=choice), {"context": 200_000})
        assert native is expected

    def test_a_custom_model_never_compacts_natively(self):
        """Settings offer the choice only for manifest models, so a custom
        one, a shadow of a built-in included, would have no switch."""
        from src.llms.preferences import Tuning

        native = self._native(Tuning(native_compaction=True), {"context": 1_000_000}, custom=True)
        assert native is False

    def test_set_even_with_no_preset_to_apply(self):
        """A model that states no window keeps the YAML numbers, but the choice
        is the user's either way."""
        from src.llms.preferences import Tuning

        assert self._native(Tuning(native_compaction=False), None) is False
