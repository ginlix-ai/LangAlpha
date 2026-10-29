"""Finding a user's stored key for a provider.

A stored BYOK key may sit under the model's own provider slug, its manifest
parent, or a sibling variant of that parent, as long as the family's vendor
issues them as one credential. The turn's resolver (``clients``), the model
lists (``provider_models``) and the published ``key_candidates`` all walk it
the same way.
"""

from __future__ import annotations


def byok_candidates(provider: str, mc) -> list[str]:
    """Return [provider → parent → sibling variants] in BYOK priority order.

    Shared by ``walk_byok_candidates`` (key lookup) and STEP-0 prefetch so the
    two can't drift. Excludes platform-only siblings (BYOK keys never live there).
    Whether a key found here may serve the model is ``key_fits``'s call, since
    it depends on what was saved with the key.
    """
    parent = mc.get_parent_provider(provider)
    candidates: list[str] = [provider]
    # Nested variants (e.g. z-ai-cn-coding under z-ai-cn) parent to the
    # provider itself, not the root — walk them before the parent's family.
    for child in mc.get_child_variants(provider):
        if child not in candidates:
            candidates.append(child)
    if parent and parent != provider:
        candidates.append(parent)
    root = parent if parent else provider
    for sibling in mc.get_child_variants(root):
        if sibling not in candidates:
            candidates.append(sibling)
    return candidates


def key_fits(provider: str, holder: str, *, has_base_url: bool, mc) -> bool:
    """Whether a key stored under ``holder`` may serve a model of ``provider``.

    Not when the two are separate credentials (``own_api_key`` in the
    manifest): the key would go to the model's endpoint, which never issued it
    and rejects it. A base URL saved with the key lifts that, since it wins over
    the model's endpoint, so the key goes where the user pointed it.
    """
    return has_base_url or mc.shares_api_key(provider, holder)


def call_endpoint(provider: str, holding: str, mc, *, custom: bool) -> str:
    """The provider whose endpoint a turn calls, for a model on ``provider``
    whose key the walk found under ``holding``.

    A key's own connection decides first: a model saved under another API of
    it (OpenCode Go's Chat Completions, the key under its Anthropic route or
    any other) is served on that API, and a region of the model's plan (``moonshot-cn-coding``
    for ``moonshot-coding``) is where the key was issued, so the call goes to
    its host. Past those, a manifest model keeps its own endpoint (a z-ai-coding
    model whose key sits under ``z-ai`` still calls the coding endpoint), while
    a custom model goes where the key is: it was tagged with a brand but saved
    for the variant the user configured.
    """
    if mc.connection_of(provider) == mc.connection_of(holding):
        return provider
    if mc.models_from(holding) == provider:
        return holding
    return holding if custom else provider


async def walk_byok_candidates(
    user_id,
    provider,
    mc,
    *,
    _byok_cache=None,
    any_credential: bool = False,
):
    """Walk [provider → parent → sibling variants] for a stored BYOK key.

    Returns ``(byok_config, holding_slug)`` — the first candidate (in priority
    order) that has a key ``key_fits`` lets serve the model, or ``(None, None)``.
    ``any_credential`` takes any key, for a walk that builds against the endpoint
    of whichever slug held it. Honors a request-scoped
    ``_byok_cache`` (``dict[str, dict | None] | None``): a slug mapping to a
    dict is a confirmed key, a slug mapping to ``None`` is confirmed-absent, and
    a slug absent from the cache is NOT prefetched — it MUST fall back to a
    direct ``get_byok_configs_for_providers`` lookup so a cache miss is never a
    silent false "no key". Direct-fetch results are written back into the cache.
    """
    from src.server.database.api_keys import get_byok_configs_for_providers

    candidates = byok_candidates(provider, mc)

    if _byok_cache is None:
        # Back-compat path: no request-scoped cache, batch-fetch all candidates.
        configs = await get_byok_configs_for_providers(user_id, candidates)
    else:
        # Tri-state cache: a slug absent from the cache was never prefetched, so
        # it must be fetched directly — only a recorded ``None`` counts as a
        # confirmed absence.
        missing = [c for c in candidates if c not in _byok_cache]
        if missing:
            fetched = await get_byok_configs_for_providers(user_id, missing)
            for slug in missing:
                _byok_cache[slug] = fetched.get(slug)
        configs = {c: _byok_cache.get(c) for c in candidates}

    for candidate in candidates:  # keep the provider → parent → sibling priority
        byok_config = configs.get(candidate)
        if byok_config and (
            any_credential
            or key_fits(provider, candidate, has_base_url=bool(byok_config.get("base_url")), mc=mc)
        ):
            return byok_config, candidate

    return None, None
