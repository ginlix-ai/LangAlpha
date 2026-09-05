/**
 * Carries an email auth link back into the desktop app that started it.
 *
 * Email confirmation is the one auth step that cannot finish in the window that
 * began it. The link is opened in whatever the OS calls the default browser,
 * minutes or hours later and quite possibly after the app has been closed, so a
 * signup started in the app completes in Chrome while the app sits on "check
 * your inbox" with no way to ever learn otherwise. The `langalpha://` scheme the
 * shell registers is the only handoff the OS holds open across that gap.
 *
 * Only one side may redeem the link, because `verifyOtp` consumes a single-use
 * token. So the browser hands it over WITHOUT verifying, and takes it back only
 * if the user says nothing opened.
 *
 * Movable at all only because our email templates carry `token_hash`, which
 * needs no PKCE verifier. A default template's `?code=` could not be handed
 * anywhere: its verifier stays in the cookie jar that asked for it.
 */

import { desktop, isDesktopShell } from './desktop';

/**
 * Every scheme we register, and the path segment that names it in a return URL.
 *
 * A path segment rather than a query parameter, and not by preference. The
 * email template is `{{ .RedirectTo }}?token_hash={{ .TokenHash }}&type=email`,
 * which writes its `?` unconditionally, so a return URL that already carried a
 * query came back with two of them: `…/auth/confirm?shell=1?token_hash=…`. That
 * parses as a single parameter whose value is `1?token_hash=…`, leaving no
 * token at all, and the page reports a valid link as expired.
 *
 * A table rather than one constant because the two editions install side by
 * side and answer on different schemes (`desktop/src/config.js`), and the two
 * halves of this handoff run in different processes: the app marks the link,
 * and a browser with no bridge reads it back. So the edition has to travel in
 * the URL — but as a segment looked up here, never as a scheme lifted out of
 * the link. Acting on one means asking the OS to launch an application, and a
 * scheme read off a crafted link would launch whatever that machine had
 * registered. A segment this table does not know is simply not a handoff.
 */
const SHELL_SEGMENTS: Record<string, string> = {
  langalpha: 'desktop',
  'langalpha-oss': 'desktop-oss',
};

const SCHEME_BY_SEGMENT: Record<string, string> = Object.fromEntries(
  Object.entries(SHELL_SEGMENTS).map(([scheme, segment]) => [segment, scheme])
);

/**
 * Mark a return URL when this window is the desktop app, so the browser that
 * eventually opens the link knows to pass it back rather than redeem it.
 *
 * Reads the scheme off the bridge rather than assuming one, so the link comes
 * back to the build that sent it. A shell too old to report a scheme marks
 * nothing and the browser redeems the link itself, which is what every browser
 * signup does and what this did before the shell had a say.
 *
 * Verified against the live project: both segments on both pages survive
 * Supabase's redirect allow-list matching, on prod and on localhost, so this
 * needs no allow-list entry of its own. Each marked path needs a route, since
 * the SPA now serves it.
 */
export function withShellReturn(url: string): string {
  const segment = SHELL_SEGMENTS[desktop?.scheme ?? ''];
  if (!segment) return url;
  return `${url}/${segment}`;
}

/**
 * The `langalpha://` URL carrying this link's payload into the app, or null
 * when there is nothing to hand over.
 *
 * Null inside the shell as well as in an unmarked browser: a marked link that
 * reaches the app has arrived where it was going, and bouncing it back out
 * through the OS would be a loop. Null without a `token_hash` too, which is how
 * a rejected link (used, expired) stays here and gets reported, since the
 * verify endpoint returns those as error params carrying no token.
 *
 * The host and path of what comes back are decoration. The shell resolves every
 * `langalpha://` URL onto its own `/callback` route and copies only the query
 * across, which is why that route forwards an email token on to the page that
 * owns one.
 */
export function shellHandoffUrl(pathname: string, search: string): string | null {
  const scheme = SCHEME_BY_SEGMENT[pathname.slice(pathname.lastIndexOf('/') + 1)];
  if (!scheme) return null;
  if (isDesktopShell()) return null;
  const params = new URLSearchParams(search);
  if (!params.get('token_hash')) return null;
  return `${scheme}://callback?${params.toString()}`;
}
