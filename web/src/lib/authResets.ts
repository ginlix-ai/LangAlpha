/**
 * Registry for module-singleton caches that must be wiped on sign-out or
 * account switch (web/AGENTS.md: module singletons outlive React). Modules
 * register their own reset at init; AuthContext runs the registry in its
 * sign-out/account-switch batteries. Inverting the dependency keeps
 * AuthContext from statically importing heavy page modules — a module that
 * never loaded has nothing to reset.
 */
type AuthReset = () => void;

const resets = new Set<AuthReset>();

export function registerAuthReset(fn: AuthReset): void {
  resets.add(fn);
}

export function runAuthResets(): void {
  for (const fn of resets) fn();
}
