"""Flash-thread report-back pipeline, split by concern.

``keys`` (key builders + TTLs) · ``leases`` (retry/lease timing) ·
``wake`` (wake wire-protocol) · ``pointer`` (run-pointer lifecycle) ·
``reserve`` (dispatch slots, admission gate, orphan resolution) ·
``executor`` (the outbox job delivering one summary turn) · ``status``
(the ``/status`` read model) · ``core`` (watch composition + outbox
executor registration). Submodules import each other as modules, never
symbols, so test patches stay definition-site.
"""
