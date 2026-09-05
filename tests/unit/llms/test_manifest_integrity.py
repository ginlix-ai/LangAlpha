"""Regression tests that load the REAL providers.json and models.json files.

These verify that the provider config v2 restructure (grouped format with
variants + flattening) didn't break any model-to-provider resolution.
No mocking -- these hit the actual manifest files on disk.
"""

from datetime import date

import pytest

from src.llms.llm import ModelConfig
from src.llms.pricing_utils import find_model_pricing


_SCHEDULE_KEYS = {
    "peak_utc",
    "off_peak",
    "schedule_anchor",
    "peak_days",
    "peak_days_utc_offset",
}


def _peak_hour_cards(manifest: dict) -> list[tuple[str, str, dict]]:
    """Every pricing block whose rate depends on the hour the call ran.

    Selected on any scheduling key, not on ``peak_utc`` alone: a card carrying the
    discount without the window is exactly the authoring slip these checks exist
    to catch, and keying on the window would let it skip every one of them.
    """
    return [
        (provider, entry.get("id", "<no id>"), entry["pricing"])
        for provider, entries in manifest.get("models", {}).items()
        for entry in entries
        if isinstance(entry.get("pricing"), dict)
        and _SCHEDULE_KEYS & set(entry["pricing"])
    ]


def _scheduled_repricings(manifest: dict) -> list[tuple[str, str, object]]:
    # Keyed on presence, not truthiness: an empty or null block is malformed,
    # and dropping it here would quietly disarm the due-date alarm below.
    return [
        (provider, entry.get("id", "<no id>"), entry["scheduled_pricing"])
        for provider, entries in manifest.get("models", {}).items()
        for entry in entries
        if "scheduled_pricing" in entry
    ]


class TestManifestIntegrity:
    @pytest.fixture
    def model_config(self):
        return ModelConfig()

    def test_every_model_resolves_to_valid_provider(self, model_config):
        """Every model in models.json must resolve to a usable provider after flatten.

        For each model entry that declares a ``provider`` field, the flattened
        provider info must:
        - exist (non-empty dict returned by get_provider_info)
        - contain a ``sdk`` key (required to instantiate the LLM client)
        - contain at least one of ``base_url`` or ``env_key`` so the provider
          is reachable (env_key may be None for oauth/dynamic providers, but
          the key itself should still be present in the dict)
        """
        failures: list[str] = []

        for model_name, model_def in model_config.llm_config.items():
            provider = model_def.get("provider")
            if provider is None:
                continue

            info = model_config.get_provider_info(provider)

            if not info:
                failures.append(
                    f"{model_name}: provider '{provider}' resolved to empty/None"
                )
                continue

            if "sdk" not in info:
                failures.append(
                    f"{model_name}: provider '{provider}' missing 'sdk' field"
                )

            has_base_url = "base_url" in info
            has_env_key = "env_key" in info
            if not (has_base_url or has_env_key):
                failures.append(
                    f"{model_name}: provider '{provider}' has neither "
                    "'base_url' nor 'env_key'"
                )

        assert not failures, (
            f"{len(failures)} model(s) failed provider resolution:\n"
            + "\n".join(f"  - {f}" for f in failures)
        )

    def test_coding_variant_resolves_pricing_via_parent(self, model_config):
        """z-ai-coding carries no pricing list, so it must either price a model
        through its parent (z-ai) or, while it ships no models, keep the parent
        resolution itself working.
        """
        variant, parent_name = "z-ai-coding", "z-ai"

        coding_model = None
        for model_name, model_def in model_config.llm_config.items():
            if model_def.get("provider") == variant:
                coding_model = (model_name, model_def)
                break

        if coding_model is not None:
            model_name, model_def = coding_model
            model_id = model_def.get("model_id", model_name)
            pricing = find_model_pricing(model_id, provider=variant)
            assert pricing is not None, (
                f"find_model_pricing('{model_id}', provider='{variant}') "
                f"returned None -- parent fallback to {parent_name} is broken"
            )
        else:
            parent = model_config.get_parent_provider(variant)
            assert parent == parent_name, (
                f"Expected parent provider of '{variant}' to be "
                f"'{parent_name}', got '{parent}'"
            )

    def test_every_model_with_input_modalities_has_text(self, model_config):
        """Every model entry with input_modalities must include 'text'."""
        for model_name, model_def in model_config.llm_config.items():
            modalities = model_def.get("input_modalities")
            if modalities is not None:
                assert "text" in modalities, (
                    f"{model_name}: input_modalities missing 'text': {modalities}"
                )


class TestScheduledRepricing:
    """Vendors announce price changes ahead of the date they take effect.

    ``scheduled_pricing`` parks the announced rates on the entry; these tests
    are the alarm that goes off on the day, so the manifest can't keep billing
    yesterday's numbers unnoticed.
    """

    @pytest.fixture
    def manifest(self):
        return ModelConfig().manifest

    def test_scheduled_repricing_is_well_formed(self, manifest):
        """A malformed block would silently disarm the alarm below."""
        failures: list[str] = []

        for provider, model_id, sched in _scheduled_repricings(manifest):
            where = f"{provider}/{model_id}"

            if not isinstance(sched, dict) or not sched:
                failures.append(
                    f"{where}: scheduled_pricing is {sched!r}, want a non-empty object"
                )
                continue

            effective_from = sched.get("effective_from")
            try:
                date.fromisoformat(effective_from)
            except (TypeError, ValueError):
                failures.append(
                    f"{where}: effective_from {effective_from!r} is not ISO YYYY-MM-DD"
                )

            if not any(k in sched for k in ("input", "output", "input_tiers")):
                failures.append(f"{where}: scheduled_pricing carries no rates")

        assert not failures, "Malformed scheduled_pricing:\n" + "\n".join(
            f"  - {f}" for f in failures
        )

    def test_no_scheduled_repricing_has_come_due(self, manifest):
        """Fails from the day an announced price change takes effect.

        Evaluated against the CI run's own date, so the build goes red on the
        day rather than whenever someone next reads the manifest.
        """
        today = date.today()
        overdue: list[str] = []

        for provider, model_id, sched in _scheduled_repricings(manifest):
            if not isinstance(sched, dict):
                continue  # shape is the other test's job
            try:
                effective = date.fromisoformat(sched.get("effective_from"))
            except (TypeError, ValueError):
                continue  # shape is the other test's job
            if effective <= today:
                overdue.append(
                    f"{provider}/{model_id}: new rates took effect {effective} "
                    f"({(today - effective).days} day(s) ago)"
                )

        assert not overdue, (
            f"{len(overdue)} announced price change(s) are now live but the "
            "manifest still bills the old rates:\n"
            + "\n".join(f"  - {o}" for o in overdue)
            + "\n\nFor each: move the scheduled_pricing rates into pricing, "
            "then delete the scheduled_pricing block."
        )


class TestTimeOfDayPricing:
    """Cards priced by the hour, checked for shape rather than for rates.

    The engine reads ``peak_utc`` to decide which card is in force and falls back
    to the top level when ``off_peak`` omits a key. Both are silent failures: a
    malformed window bills the wrong rate and a missing override bills the peak
    one, and each looks identical to a correct turn in every log we keep.
    """

    @pytest.fixture
    def manifest(self):
        return ModelConfig().manifest

    def test_peak_windows_are_well_formed(self, manifest):
        failures: list[str] = []

        for provider, model_id, pricing in _peak_hour_cards(manifest):
            where = f"{provider}/{model_id}"
            windows = pricing.get("peak_utc")

            if not isinstance(windows, list) or not windows:
                failures.append(f"{where}: peak_utc is {windows!r}, want a non-empty list")
                continue

            for window in windows:
                if not (isinstance(window, list) and len(window) == 2):
                    failures.append(f"{where}: window {window!r} is not a [start, end] pair")
                    continue
                start, end = window
                # ``type is int`` rather than isinstance, matching the engine: bool
                # subclasses int, so a JSON ``true``/``false`` bound would pass here
                # and install an unintended window at runtime.
                if not all(type(h) is int for h in window):
                    failures.append(f"{where}: window {window!r} has non-integer hours")
                elif not 0 <= start < end <= 24:
                    failures.append(f"{where}: window {window!r} is not 0 <= start < end <= 24")

        assert not failures, "Malformed peak_utc:\n" + "\n".join(f"  - {f}" for f in failures)

    def test_peak_day_restrictions_are_well_formed(self, manifest):
        """A day list the engine cannot read is dropped, and the card then bills
        peak on the days it meant to exclude -- the same silent overcharge the
        window checks above guard, one axis over.
        """
        failures: list[str] = []

        for provider, model_id, pricing in _peak_hour_cards(manifest):
            where = f"{provider}/{model_id}"
            days = pricing.get("peak_days")

            if days is not None:
                if not isinstance(days, list) or not days:
                    failures.append(f"{where}: peak_days is {days!r}, want a non-empty list")
                # ``type is int`` rather than isinstance, matching the engine: a
                # JSON ``true`` would install Monday and ``false`` nothing at all.
                elif not all(type(d) is int and 1 <= d <= 7 for d in days):
                    failures.append(f"{where}: peak_days {days!r} are not ISO weekdays 1-7")
                elif len(set(days)) != len(days):
                    failures.append(f"{where}: peak_days {days!r} repeats a day")

            offset = pricing.get("peak_days_utc_offset")
            if offset is not None and (type(offset) is not int or not -14 <= offset <= 14):
                failures.append(
                    f"{where}: peak_days_utc_offset {offset!r} is not an hour offset in -14..14"
                )
            if offset is not None and days is None:
                # The offset only ever shifts the clock the days are read on, so
                # on its own it changes nothing and reads as a restriction that
                # is not there.
                failures.append(f"{where}: peak_days_utc_offset without peak_days")

        assert not failures, "Malformed peak day rule:\n" + "\n".join(
            f"  - {f}" for f in failures
        )

    def test_peak_windows_do_not_overlap(self, manifest):
        """Overlapping blocks make the schedule ambiguous to read, even though
        the engine's any() would happen to resolve them to peak."""
        failures: list[str] = []

        for provider, model_id, pricing in _peak_hour_cards(manifest):
            # Not indexed: the selector deliberately matches a card carrying only
            # off_peak or schedule_anchor, and a KeyError here would replace the
            # sibling test's named failure with a traceback that says nothing.
            windows = sorted(
                w
                for w in (pricing.get("peak_utc") or [])
                if isinstance(w, list) and len(w) == 2
            )
            for earlier, later in zip(windows, windows[1:]):
                if later[0] < earlier[1]:
                    failures.append(
                        f"{provider}/{model_id}: {earlier} overlaps {later}"
                    )

        assert not failures, "Overlapping peak_utc:\n" + "\n".join(
            f"  - {f}" for f in failures
        )

    def test_off_peak_only_overrides_rates_the_peak_card_already_names(self, manifest):
        """A key present only in off_peak has nothing to fall back to, so the
        peak window would bill it at zero."""
        failures: list[str] = []

        for provider, model_id, pricing in _peak_hour_cards(manifest):
            off_peak = pricing.get("off_peak")
            if not isinstance(off_peak, dict) or not off_peak:
                failures.append(
                    f"{provider}/{model_id}: off_peak is {off_peak!r}, want a non-empty object"
                )
                continue

            orphans = sorted(set(off_peak) - set(pricing))
            if orphans:
                failures.append(
                    f"{provider}/{model_id}: off_peak names {orphans} absent from the peak card"
                )

        assert not failures, "Malformed off_peak:\n" + "\n".join(
            f"  - {f}" for f in failures
        )

    def test_off_peak_is_not_the_more_expensive_card(self, manifest):
        """Catches a swapped paste, which the shape checks above cannot see.

        Relational, so a repricing never churns it.
        """
        failures: list[str] = []

        for provider, model_id, pricing in _peak_hour_cards(manifest):
            off_peak = pricing.get("off_peak")
            if not isinstance(off_peak, dict):
                continue
            for rate, off in off_peak.items():
                peak = pricing.get(rate)
                if isinstance(off, (int, float)) and isinstance(peak, (int, float)):
                    if off > peak:
                        failures.append(
                            f"{provider}/{model_id}: off_peak {rate} {off} exceeds peak {peak}"
                        )

        assert not failures, "Inverted off_peak rates:\n" + "\n".join(
            f"  - {f}" for f in failures
        )

    def test_every_off_peak_override_is_a_rate_the_engine_would_read(self, manifest):
        """An override the engine never consults is worse than a missing one.

        ``get_input_cost``/``get_output_cost`` resolve matrix before tiers before
        flat rates, so a flat override on a card priced either of the other two
        ways is dead: the call bills the peak rate while the stored snapshot
        records ``window: off_peak``, corroborating a discount nobody got.
        """
        failures: list[str] = []

        for provider, model_id, pricing in _peak_hour_cards(manifest):
            off_peak = pricing.get("off_peak")
            if not isinstance(off_peak, dict):
                continue

            shadowed: set[str] = set()
            if pricing.get("pricing_mode") == "2d_matrix" and "matrix" in pricing:
                shadowed |= {"input", "output", "cached_input"}
            if "input_tiers" in pricing:
                shadowed |= {"input", "cached_input"}
            if "output_tiers" in pricing:
                shadowed.add("output")

            unreachable = sorted(shadowed & set(off_peak))
            if unreachable:
                failures.append(
                    f"{provider}/{model_id}: off_peak overrides {unreachable}, "
                    "which this card's pricing mode never reads"
                )

        assert not failures, "Unreachable off_peak overrides:\n" + "\n".join(
            f"  - {f}" for f in failures
        )

    def test_the_schedule_anchor_is_one_the_engine_knows(self, manifest):
        """An unrecognized value silently falls back to completion."""
        failures = [
            f"{provider}/{model_id}: schedule_anchor {pricing['schedule_anchor']!r}"
            for provider, model_id, pricing in _peak_hour_cards(manifest)
            if "schedule_anchor" in pricing
            and pricing["schedule_anchor"] not in ("completion", "request")
        ]

        assert not failures, "Unknown schedule_anchor:\n" + "\n".join(
            f"  - {f}" for f in failures
        )
