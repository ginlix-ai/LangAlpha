"""Report-back pipeline for the Chief of Staff's hand-offs, split by concern.

``keys`` (key builders + TTLs) · ``leases`` (retry/lease timing) ·
``wake`` (wake wire-protocol) · ``pointer`` (run-pointer lifecycle) ·
``reserve`` (dispatch slots, admission gate, orphan resolution) ·
``executor`` (the outbox job delivering one summary turn) · ``carry``
(the pair a turn after an interrupted summary releases) · ``status``
(the ``/status`` read model) · ``core`` (watch composition + outbox
executor registration). Submodules import each other as modules, never
symbols, so test patches stay definition-site.
"""
