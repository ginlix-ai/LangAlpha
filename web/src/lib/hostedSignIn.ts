/**
 * Where a desktop window signs in, and where it goes once it has.
 *
 * A hosted desktop build enters its first run at the account portal's sign-in,
 * and the portal decides where a signed-in account goes next: into account
 * setup when it has not been through it, here when it has. Every sign-in after
 * that has to pass through the portal too. One that starts or finishes on this
 * app's own pages (a sign-out and a new registration, or an emailed link opened
 * while the app was closed, which the shell delivers here) lands a new account
 * in the app with setup never offered, and with no address bar the user has no
 * way to find the portal's page.
 *
 * In a browser, in the self-hosted edition, and in a build with no portal, this
 * app keeps its own pages.
 */
import type { NavigateFunction } from 'react-router';
import { PLATFORM_URL } from '@/config/supportLinks';
import { desktop } from '@/lib/desktop';

// When the window last left for the portal. Back here signed out moments later
// means the portal holds a session this app cannot read, and sending the window
// again would bounce it between the two for good, so this app's own page takes
// the sign-in instead. Only moments later: a Back or Home long after the
// hand-off is the user returning, not a bounce, and still goes to the portal.
const SENT_KEY = 'hosted-sign-in-sent-at';
const BOUNCE_MS = 30_000;

// The page the window was sent to sign in for (`?redirect=`), kept for the
// trip: the portal brings the window back to this app's root, not to it.
const RETURN_KEY = 'hosted-sign-in-return';

/**
 * Whether this is the hosted edition, which keeps the portal in this window. An
 * edition with a portal advertises the integration hand-backs that land on it
 * and one without leaves them out; `integration-install` is the one every such
 * release carries. The self-hosted edition signs in on this app's own pages,
 * whatever server it points at.
 */
function shellHasPortal(): boolean {
  return desktop?.capabilities?.includes('integration-install') ?? false;
}

/** The portal's sign-in page when this window should sign in there, otherwise null. */
export function hostedSignInUrl(): string | null {
  if (!PLATFORM_URL || !shellHasPortal()) return null;
  const url = `${PLATFORM_URL}/login`;
  try {
    const sentAt = Number(sessionStorage.getItem(SENT_KEY));
    if (Date.now() - sentAt < BOUNCE_MS) {
      console.warn('[auth] back from the account portal without a session; signing in here instead');
      return null;
    }
  } catch {
    // No storage, no way to tell a bounce from a first visit: stay here.
    return null;
  }
  return url;
}

/**
 * Send the window to `url`, noting when so a bounce back is caught, and where
 * this page was asked to return to. False, with the window left where it is,
 * when the note cannot be written: unrecorded, a bounce would go round for good.
 */
export function leaveForHostedSignIn(url: string): boolean {
  try {
    sessionStorage.setItem(SENT_KEY, String(Date.now()));
    const back = new URLSearchParams(window.location.search).get('redirect');
    if (back) sessionStorage.setItem(RETURN_KEY, back);
    else sessionStorage.removeItem(RETURN_KEY);
  } catch {
    return false;
  }
  window.location.replace(url);
  return true;
}

/**
 * Where the last hand-off was asked to return to, unchecked: the caller vets it
 * as it would a `?redirect=`. Read before `forgetHostedSignIn` clears it.
 */
export function hostedSignInReturn(): string | null {
  try {
    return sessionStorage.getItem(RETURN_KEY);
  } catch {
    return null;
  }
}

/**
 * Drop the hand-off notes once this window is signed in: the trip they recorded
 * came back with a session, so a sign-out moments later is a new sign-in that
 * belongs on the portal, not a bounce, and the page it was for has had its turn.
 */
export function forgetHostedSignIn(): void {
  try {
    sessionStorage.removeItem(SENT_KEY);
    sessionStorage.removeItem(RETURN_KEY);
  } catch {
    // No storage, so no note either.
  }
}

/**
 * Where a sign-in finished on this app's pages goes next. In a shell with a
 * portal that is the portal's sign-in, which finds the session and sends the
 * account on, into setup or back here; everywhere else the dashboard.
 */
export function landAfterSignIn(navigate: NavigateFunction): void {
  const url = hostedSignInUrl();
  if (url && leaveForHostedSignIn(url)) return;
  navigate('/dashboard', { replace: true });
}
