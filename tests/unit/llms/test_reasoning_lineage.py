"""Locks the reasoning-lineage partition declared in providers.json.

``reasoning_compat_group`` is declared on a provider *group* and inherited by its
variants, so adding it to the wrong group silently merges two routes that cannot
verify each other's reasoning — the failure mode the whole design exists to
prevent, and one that shows up in production as a hard 400 rather than a test
failure. These tests are the tripwire.
"""

from src.llms.llm import LLM
from src.llms.reasoning_lineage import (
    ANTHROPIC_LINEAGE,
    ROUTE_ENDPOINT_SEP,
    UNKNOWN_LINEAGE,
    lineage_for_model_id,
    lineage_for_route,
)

# Where an SDK points when a provider declares no base_url of its own. Only the
# SDKs appearing in a declared group need an entry; an unmapped one raises here
# rather than silently dropping that provider from the invariant below.
SDK_DEFAULT_BASE_URL = {
    "anthropic": "https://api.anthropic.com",
    "openai": "https://api.openai.com/v1",
}


def _providers() -> dict:
    return LLM.get_model_config().flat_providers


def _endpoints_by_group(providers: dict) -> dict[str, set[str]]:
    """Group → the set of endpoints its non-platform members actually reach."""
    by_group: dict[str, set[str]] = {}
    for name, cfg in providers.items():
        group = cfg.get("reasoning_compat_group")
        if not group or cfg.get("platform"):
            continue
        url = cfg.get("base_url")
        if not url:
            sdk = cfg.get("sdk")
            assert sdk in SDK_DEFAULT_BASE_URL, (
                f"{name!r} declares no base_url and sdk {sdk!r} has no known "
                f"default — add one so it is not skipped"
            )
            url = SDK_DEFAULT_BASE_URL[sdk]
        by_group.setdefault(group, set()).add(url)
    return by_group


class TestDeclaredMerges:
    def test_anthropic_api_and_oauth_share_a_lineage(self):
        """Both reach api.anthropic.com, so a signature from one verifies on the other."""
        assert lineage_for_route("anthropic") == ANTHROPIC_LINEAGE
        assert lineage_for_route("claude-oauth") == ANTHROPIC_LINEAGE

    def test_codex_oauth_is_split_from_the_openai_api(self):
        """Same brand group, but the two mint mutually unverifiable reasoning items."""
        assert lineage_for_route("codex-oauth") != lineage_for_route("openai")

    def test_unknown_provider_falls_back_to_its_own_key(self):
        assert lineage_for_route("not-a-provider") == "not-a-provider"
        assert lineage_for_route(None) == UNKNOWN_LINEAGE


class TestNoAccidentalMerges:
    def test_only_reviewed_direct_routes_declare_a_compat_group(self):
        """A new declaration must be a deliberate, reviewed decision.

        Widening this set means asserting the routes share an upstream API. If
        this fails, that is the question to answer — not a value to update.
        Platform variants are excluded so the assertion holds identically
        whether or not a deployment overlay adds proxy routes.
        """
        declared = {
            name
            for name, cfg in _providers().items()
            if cfg.get("reasoning_compat_group") and not cfg.get("platform")
        }
        assert declared == {"anthropic", "claude-oauth", "openai", "codex-oauth"}

    def test_sibling_shim_variants_stay_separate(self):
        """Sibling variants of one brand are usually *different* upstreams.

        These are the pairs the design explicitly refused to merge on
        ``parent_provider``; group-level inheritance would silently do it.
        """
        providers = _providers()
        for a, b in (
            ("moonshot", "moonshot-coding"),
            ("minimax", "minimax-coding"),
            ("doubao-anthropic", "doubao-coding"),
            ("z-ai", "z-ai-coding"),
        ):
            if a in providers and b in providers:
                assert lineage_for_route(a) != lineage_for_route(b), (
                    f"{a} and {b} were merged into one reasoning lineage"
                )

    def test_every_direct_lineage_group_shares_one_base_url(self):
        """The invariant a compat group actually asserts.

        Two direct routes can verify each other's reasoning only if they reach
        the same API, so a group spanning two endpoints is a merge bug. A
        provider that declares no ``base_url`` still has one — its SDK's default
        — and must be resolved, or the only group with such a member
        (``anthropic``) would compare a one-element set and assert nothing.
        Platform proxies are exempt: a proxy forwards to the group's upstream,
        so its own base_url is a routing detail rather than a different API.
        """
        for group, urls in _endpoints_by_group(_providers()).items():
            assert len(urls) == 1, f"lineage {group!r} spans endpoints {urls}"

    def test_the_anthropic_group_is_what_makes_that_test_bite(self):
        """Guards the guard: ``anthropic`` declares no base_url, ``claude-oauth`` does.

        If both ever declare the same literal, the resolution above becomes
        redundant and could be dropped without anyone noticing it was load-bearing.
        """
        providers = _providers()
        assert providers["anthropic"].get("base_url") is None
        assert providers["claude-oauth"]["base_url"] == "https://api.anthropic.com"
        assert _endpoints_by_group(providers)[ANTHROPIC_LINEAGE] == {
            "https://api.anthropic.com"
        }


class TestOffManifestRoutes:
    """A provider key redirected off its manifest endpoint is its own lineage.

    BYOK can repoint a built-in provider, and a custom Anthropic-shaped provider
    is rewritten onto its manifest parent (#221) — in both cases the key stops
    naming the upstream, so inheriting the group's trust would replay signatures
    to an endpoint that cannot verify them.
    """

    def test_a_redirected_route_does_not_inherit_the_group(self):
        redirected = f"anthropic{ROUTE_ENDPOINT_SEP}https://gateway.example/v1"
        assert lineage_for_route(redirected) == redirected
        assert lineage_for_route(redirected) != lineage_for_route("anthropic")
        assert lineage_for_route(redirected) != lineage_for_route("claude-oauth")

    def test_two_different_endpoints_never_merge(self):
        a = f"anthropic{ROUTE_ENDPOINT_SEP}https://one.example"
        b = f"anthropic{ROUTE_ENDPOINT_SEP}https://two.example"
        assert lineage_for_route(a) != lineage_for_route(b)

    def test_no_manifest_key_contains_the_separator(self):
        """The separator marks a redirect, so a key containing one would alias it."""
        assert not [k for k in _providers() if ROUTE_ENDPOINT_SEP in k]


class TestProviderRouteStamp:
    """``LLM._provider_route`` is the single input the whole feature reads.

    Everything downstream — the client stamp, the origin recorded on each
    AIMessage, both gates — is derived from this string, so it is tested against
    the real manifest rather than a fixture.
    """

    def test_a_manifest_route_stamps_its_bare_key(self):
        assert LLM("claude-opus-5")._provider_route() == "anthropic"

    def test_oauth_keeps_its_own_key_and_inherits_the_group(self):
        """Its declared base_url matches the manifest, so it is not a redirect."""
        route = LLM("claude-opus-5-oauth")._provider_route()
        assert route == "claude-oauth"
        assert lineage_for_route(route) == ANTHROPIC_LINEAGE

    def test_a_byok_base_url_override_qualifies_the_route(self):
        """The BYOK path that made the bare provider key unsafe as an identity."""
        route = LLM(
            "claude-opus-5",
            api_key="unused",
            base_url_override="https://gateway.example/v1",
        )._provider_route()
        assert route == f"anthropic{ROUTE_ENDPOINT_SEP}https://gateway.example/v1"
        assert lineage_for_route(route) != ANTHROPIC_LINEAGE


class TestModelIdFallback:
    def test_unambiguous_id_resolves(self):
        assert lineage_for_model_id("claude-opus-5") == ANTHROPIC_LINEAGE

    def test_unknown_and_missing_ids_fail_closed(self):
        assert lineage_for_model_id("some-unknown-model") == UNKNOWN_LINEAGE
        assert lineage_for_model_id(None) == UNKNOWN_LINEAGE

    def test_index_lifetime_follows_the_config_instance(self):
        """A rebuilt manifest gets a fresh index instead of a stale cached one."""
        from src.llms.llm import ModelConfig
        from src.llms.reasoning_lineage import _model_id_index

        config = LLM.get_model_config()
        assert _model_id_index(config) is _model_id_index(config)
        assert _model_id_index(ModelConfig()) is not _model_id_index(config)
