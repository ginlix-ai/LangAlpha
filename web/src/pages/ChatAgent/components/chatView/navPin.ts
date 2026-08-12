// Shared nav panel state across ChatView instances — when switching threads,
// the newly active instance inherits this so the panel stays open.
// `pinned` is persisted and read synchronously at module load so a reload
// mounts the panel docked without a flash. Pinning is desktop-only; mobile
// keeps the hamburger/drawer flow and ignores `pinned`.
export const NAV_PIN_KEY = 'nav.pinned';
export function readNavPinned(): boolean {
  try {
    return localStorage.getItem(NAV_PIN_KEY) === 'true';
  } catch {
    return false;
  }
}
export const _sharedNav = { visible: false, locked: false, pinned: readNavPinned() };
