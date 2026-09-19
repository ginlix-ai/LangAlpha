"""Patch a module global across the ComputerManager mixin modules.

``src/server/services/computer_manager`` is a package, so a name like
``db_get_workspace`` is a separate global in every submodule whose methods call
it. Patching one submodule leaves the others bound to the real function, so
``cm_patch`` installs one mock in all of them. Names that only one submodule
imports are patched by their full dotted path instead of going through here.
"""

import functools
import inspect
from contextlib import ExitStack
from unittest.mock import DEFAULT, patch

_PKG = "src.server.services.computer_manager"

# Every name that more than one mixin module imports, with its modules.
CM_PATCH_TARGETS = {
    "SessionManager": ("_machines", "_provisioning", "_sessions"),
    "db_get_workspace": ("_lifecycle", "_machines", "_mcp", "_provisioning", "_sessions"),
    "db_get_workspace_identity": ("_mcp", "_provisioning", "_sessions"),
    "get_live_workspace_ids_for_computer": ("_machines", "_provisioning"),
    "try_bind_computer_provider_ref": ("_lifecycle", "_machines"),
    "try_claim_computer_for_start": ("_lifecycle", "_machines", "_provisioning"),
    "update_workspace_activity": ("_lifecycle", "_provisioning"),
}


def _wrap(func, patching):
    """Build the decorator wrapper mock's own patchings protocol expects."""

    def _apply(stack, args, kwargs):
        extra = []
        for entry in patched.patchings:
            value = stack.enter_context(entry)
            if entry.attribute_name is not None:
                kwargs.update(value)
            elif entry.new is DEFAULT:
                extra.append(value)
        return args + tuple(extra), kwargs

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def patched(*args, **kwargs):
            with ExitStack() as stack:
                args, kwargs = _apply(stack, args, kwargs)
                return await func(*args, **kwargs)

    else:

        @functools.wraps(func)
        def patched(*args, **kwargs):
            with ExitStack() as stack:
                args, kwargs = _apply(stack, args, kwargs)
                return func(*args, **kwargs)

    patched.patchings = [patching]
    return patched


class _CmPatch:
    """One mock installed in several submodule namespaces.

    ``attribute_name`` and ``new`` are the two fields ``unittest.mock`` reads off
    a decorator entry, so this stacks with plain ``patch`` in either order and
    pytest still discounts the injected argument.
    """

    attribute_name = None

    def __init__(self, name, new, kwargs):
        if name not in CM_PATCH_TARGETS:
            raise AssertionError(
                f"{name!r} is not bound in more than one computer_manager module; "
                "patch its dotted path directly"
            )
        self._name = name
        self.new = new
        self._kwargs = kwargs
        self._stack = None

    def __enter__(self):
        stack = ExitStack()
        try:
            modules = CM_PATCH_TARGETS[self._name]
            if self.new is DEFAULT:
                value = stack.enter_context(
                    patch(f"{_PKG}.{modules[0]}.{self._name}", **self._kwargs)
                )
                modules = modules[1:]
            else:
                value = self.new
            for module in modules:
                stack.enter_context(patch(f"{_PKG}.{module}.{self._name}", new=value))
        except BaseException:
            stack.close()
            raise
        self._stack = stack
        return value

    def __exit__(self, *exc_info):
        stack, self._stack = self._stack, None
        return stack.__exit__(*exc_info)

    def start(self):
        return self.__enter__()

    def stop(self):
        return self.__exit__(None, None, None)

    def copy(self):
        return _CmPatch(self._name, self.new, dict(self._kwargs))

    def __call__(self, func):
        if hasattr(func, "patchings"):
            func.patchings.append(self)
            return func
        return _wrap(func, self)


def cm_patch(name, new=DEFAULT, **kwargs):
    """Patch ``name`` in every ComputerManager module that binds it."""
    return _CmPatch(name, new, kwargs)
