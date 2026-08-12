"""In-house web fetch engine — the zero-key ``inhouse`` provider.

Tiered scrapling crawler (``scrapling_crawler``) behind a health-isolating
wrapper (``safe_wrapper``), with specialised PDF/YouTube/X extractors routed
via ``content_router`` and sitemap discovery in ``sitemap``. Import from the
submodules directly; this package intentionally re-exports nothing so that
light consumers (e.g. sitemap summaries) don't drag in the crawler stack.
"""
