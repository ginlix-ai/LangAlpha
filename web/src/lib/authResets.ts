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
let epoch = 0;

export function registerAuthReset(fn: AuthReset): void {
  resets.add(fn);
}

export function runAuthResets(): void {
  epoch += 1;
  for (const fn of resets) fn();
}

/**
 * A check that holds until the next sign-out or account switch. Work that
 * outlives the reset (a fetch, a save) takes one as it starts and asks it as it
 * lands, so the last account's result is dropped rather than shown or kept.
 * Not `authGeneration()`: that also moves when the page adopts its first
 * session, which would drop work started a moment before.
 */
export function authSessionCheck(): () => boolean {
  const started = epoch;
  return () => started === epoch;
}
