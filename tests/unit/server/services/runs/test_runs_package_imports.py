"""Package-shape guard for src/server/services/runs/ (modeled on
test_chat_package_imports.py): the package and its submodules import cleanly,
and RunScope's ownership contract stays present."""

import importlib
import sys


class TestPackageImports:
    def test_import_package(self):
        mod = importlib.import_module("src.server.services.runs")
        assert mod is not None

    def test_import_admission(self):
        import src.server.services.runs.admission as mod

        assert hasattr(mod, "RunScope")

    def test_runscope_contract(self):
        from src.server.services.runs.admission import RunScope

        scope = RunScope(user_id="u-1", burst_slot_id=None)
        assert scope.slot_owned is True
        assert scope.owned_run_handle is None
        scope.attach_run(object())
        assert scope.owned_run_handle is not None
        # Handoff flips ownership: the executor owns cleanup from here.
        scope.transfer_to_executor()
        assert scope.slot_owned is False
        assert scope.owned_run_handle is None


class TestNoCircularImports:
    def test_fresh_import_succeeds(self):
        saved = {
            k: sys.modules.pop(k)
            for k in list(sys.modules)
            if k.startswith("src.server.services.runs")
        }
        try:
            mod = importlib.import_module("src.server.services.runs.admission")
            assert hasattr(mod, "RunScope")
        finally:
            # Evict the modules the fresh import created, restore the
            # originals, and rebind the parent package's ``runs`` attribute
            # — the fresh import replaced it with the new package object,
            # and sys.modules restoration alone leaves getattr-walkers
            # (monkeypatch.setattr string targets) on the stale object.
            for k in list(sys.modules):
                if k.startswith("src.server.services.runs"):
                    del sys.modules[k]
            sys.modules.update(saved)
            parent = sys.modules.get("src.server.services")
            original_pkg = saved.get("src.server.services.runs")
            if parent is not None:
                if original_pkg is not None:
                    parent.runs = original_pkg
                elif hasattr(parent, "runs"):
                    del parent.runs
