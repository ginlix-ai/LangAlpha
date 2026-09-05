/**
 * The one place the app reads an access token.
 *
 * `supabase.auth.getSession()` is not an accessor -- its own docs say it
 * "returns the session, refreshing it if necessary" -- so calling it per request
 * turns one page load into ~20 network refreshes and, on a device whose clock
 * is off, exhausts Supabase's per-IP token budget until a 429 destroys the
 * session. Supabase's documented practice is to hold the token in memory and
 * let `onAuthStateChange` keep it current; this module is that memory.
 *
 * It deliberately does NOT subscribe to `onAuthStateChange` itself: every
 * registration runs `_emitInitialSession`, which loads (and may refresh) a
 * session. AuthContext owns the one subscription and pushes here.
 */
import type { Session } from '@supabase/supabase-js';
import { isPlatformMode } from '../config/hostMode';
import { tokenEndpointCooldownRemainingMs } from './authCooldown';
import { supabase } from './supabase';

/**
 * Deliberately below auth-js's own 90s `EXPIRY_MARGIN_MS`. Its background timer
 * owns routine refresh and repopulates this cache via `TOKEN_REFRESHED` for
 * free; our slow path is only for when that timer did not run, such as a frozen
 * tab. Being the narrower of the two margins is also what lets the read path be
 * a single `getSession()`: anything this calls stale, auth-js calls stale too.
 */
const MARGIN_MS = 30_000;

/** Never re-derive the session from storage more than this often. */
const MIN_RENEW_INTERVAL_MS = 10_000;

/**
 * How far the wall clock may run backwards before a cached expiry stops being
 * believable. Below any real correction, which is minutes to hours, and above
 * the millisecond steps a time daemon takes when it cannot slew.
 */
const CLOCK_REWIND_TOLERANCE_MS = 1_000;

/**
 * Longest a caller waits on a renewal that is already in flight.
 *
 * auth-js answers a retryable failure with a local retry ladder bounded by its
 * own 30s tick, and the breaker turns a rate limit into exactly that. So the
 * first read to meet a 429 sits inside `getSession()` for ~39s, measured in
 * Chrome, with every other caller parked behind it and the tab's authenticated
 * traffic frozen for the whole window. Fake timers hide this, which is why no
 * unit test here caught it.
 *
 * The flight is deliberately left running rather than cancelled: it still
 * repopulates the cache, and it has already opened the cool-off, which is what
 * makes every read after this one cost nothing.
 */
const MAX_RENEWAL_WAIT_MS = 5_000;

/**
 * And never spend a forced rotation more often than this, counting the ones that
 * failed. Concurrent 401s are already collapsed by the single flight and
 * staggered ones by the "somebody already rotated" check, so what is left for
 * this floor is the case both of those miss: a token the server keeps refusing,
 * where every consumer's retry is a fresh, non-overlapping demand for a new one.
 */
const MIN_FORCE_INTERVAL_MS = 10_000;

/**
 * Three states, not a nullable token, because "we have not looked yet" and "we
 * looked and there is nobody signed in" want opposite handling: the first must
 * read storage, the second must not read anything at all. Collapsing them is
 * what makes a login screen spend a session read per outbound request.
 */
type CacheState =
  | { status: 'unknown' }
  | { status: 'signed-out' }
  | {
      status: 'token'; token: string; userId: string | null;
      expiresAtMs: number; stampedAt: number;
    };

let cache: CacheState = { status: 'unknown' };
let inFlightRead: Promise<string | null> | null = null;
let inFlightForce: Promise<string | null> | null = null;
let lastRenewAt = 0;
let lastForceAt = 0;

/**
 * Bumped whenever the signed-in user changes, sign-out included. A renewal that
 * was already in flight resolves long after it was started, so without this a
 * reply that lands after a sign-out re-arms the cache with a live token nobody
 * asked for -- and every request after it goes out authenticated as the user
 * who just left. A same-user refresh deliberately does not bump: it is the
 * renewal paths' own result coming back, and fencing that would discard it.
 */
let generation = 0;

/**
 * The auth client, but only in the mode that has one.
 *
 * `supabase` is built from the env vars alone, so an OSS build that happens to
 * carry them gets a real client while `AuthProvider` deliberately serves the
 * static local identity. Reading a session through it would put a platform
 * user's token on OSS requests, and refresh or sign out a session this mode has
 * no business touching. The mode is the switch, never client construction
 * (web/AGENTS.md).
 */
function authClient() {
  return isPlatformMode ? supabase : null;
}

function currentUserId(): string | null {
  return cache.status === 'token' ? cache.userId : null;
}

/** Adopt a session. Synchronous, so it is safe inside the auth-state callback. */
export function publishSession(session: Session | null): void {
  if ((session?.user?.id ?? null) !== currentUserId()) generation += 1;
  if (!session?.access_token) {
    cache = { status: 'signed-out' };
    return;
  }
  cache = {
    status: 'token',
    token: session.access_token,
    userId: session.user?.id ?? null,
    // An unstamped expiry is not a fresh one: re-derive from storage on the
    // next read rather than trusting it. That costs a storage read, not a
    // network refresh, and it self-heals once a stamped session lands.
    expiresAtMs: typeof session.expires_at === 'number' ? session.expires_at * 1000 : 0,
    stampedAt: Date.now(),
  };
}

/**
 * Adopt the result of an async session read the caller is about to start.
 *
 * Call it *before* the read and hand the returned function the session. It
 * captures who is signed in at that moment and discards a reply that lands
 * after that changed, which the synchronous `publishSession` cannot do: by the
 * time an awaited reply reaches it, the cache has already moved on. auth-js
 * broadcasts a sign-out across tabs, so a read started just before one lands
 * would otherwise re-arm the cache with the departed user's live token, and
 * every request after it would go out authenticated as them.
 */
export function sessionAdopter(): (session: Session | null) => void {
  const startedAt = generation;
  return (session) => publishIfCurrent(session, startedAt);
}

/**
 * Which signed-in user the cache is currently answering for.
 *
 * Only ever compared against a value read earlier, never interpreted: a caller
 * that holds a token stamps this beside it and checks the two still match
 * before acting on a reply, which is how work started for one user is kept from
 * completing as the next one.
 */
export function authGeneration(): number {
  return generation;
}

/** Module singletons outlive React (web/AGENTS.md), so sign-out must wipe this. */
export function clearAuthToken(): void {
  cache = { status: 'signed-out' };
  generation += 1;
  inFlightRead = null;
  inFlightForce = null;
  lastRenewAt = 0;
  lastForceAt = 0;
}

function heldToken(): string | null {
  return cache.status === 'token' ? cache.token : null;
}

/**
 * Whether `stampedAt` is still inside `floorMs`.
 *
 * A negative elapsed can only mean the wall clock moved backwards, and this
 * module exists for the one device population that gets its clock corrected:
 * the advice to a user whose machine is an hour fast is to turn time sync on,
 * which lands a backwards jump of exactly the skew. Read literally, the floor
 * would then hold every rotation off for the size of the jump while the cache
 * kept serving a token the server had already expired, so the tab would 401 on
 * everything with no way to heal. Treating the floor as spent instead costs at
 * most one extra rotation, and the 401 escalation it unblocks is what restamps
 * the expiry on the corrected clock.
 */
function withinFloor(stampedAt: number, floorMs: number): boolean {
  const elapsed = Date.now() - stampedAt;
  return elapsed >= 0 && elapsed < floorMs;
}

/**
 * The cached token, while this clock still agrees it is fresh.
 *
 * The expiry below was stamped on the same clock that reads it, which is the
 * whole point of `authFetch` dropping the server's `expires_at` -- but only
 * while that clock keeps its word. The one device population this module exists
 * for is the one we ask to turn time synchronisation on, and that correction
 * walks `Date.now()` backwards by the size of the skew without moving the
 * stamp. Read literally, the comparison would then call the token fresh for the
 * length of the jump past its real expiry, and auth-js's cookie carries the
 * same stamp so its own timer would not rotate it either. A clock that has
 * moved backwards has disowned what it wrote: re-derive rather than trust it,
 * which costs one storage read and restamps everything on the corrected clock.
 */
function expiryDisowned(): boolean {
  return cache.status === 'token'
    && Date.now() < cache.stampedAt - CLOCK_REWIND_TOLERANCE_MS;
}

function unexpiredToken(): string | null {
  if (cache.status !== 'token') return null;
  if (expiryDisowned()) return null;
  return Date.now() < cache.expiresAtMs - MARGIN_MS ? cache.token : null;
}

/**
 * The token the cache holds, but only if it still belongs to `startedFor`.
 *
 * Refusing a stale reply is half the fence; the answer is the other half. A
 * renewal that resolves after an account switch reads back whoever now occupies
 * the cache, and handing that token to a caller which already built a request
 * for the previous user sends their mutation as the new account -- a
 * preferences save, a workspace create, anything whose target is the token
 * rather than the URL. Answering with nothing costs a 401, which is cheaper
 * than a write to the wrong account by a wide margin.
 *
 * Identity, not `generation`, is deliberately the thing compared. Adopting the
 * first session of a page load moves the generation too, and a read that
 * started before it is the ordinary warm-up path, not a switch: there was no
 * user to build a request for, so there is nobody to send it as by mistake.
 */
function accountChanged(startedFor: string | null): boolean {
  return startedFor !== null && currentUserId() !== startedFor;
}

function tokenIfStill(startedFor: string | null): string | null {
  return accountChanged(startedFor) ? null : heldToken();
}

/**
 * Adopt a session obtained by one of the renewal paths below, unless the
 * signed-in user changed while the request was in flight.
 */
function publishIfCurrent(session: Session | null, startedAt: number): void {
  if (generation !== startedAt) return;
  // A null session means either "signed out" or "we could not tell" -- a
  // rate-limited refresh answers with both a null session and an error. Only an
  // auth event is allowed to conclude the former, so keep what we hold.
  if (session) publishSession(session);
}

/**
 * Re-derive the token from storage.
 *
 * One `getSession()` is the whole read path. It re-reads the cookie, so it
 * picks up a token a sibling tab already rotated, and auth-js refreshes it
 * itself when it is inside the library's own 90s margin -- which is wider than
 * ours, so a token this module calls stale is one auth-js has already renewed
 * by the time it answers.
 */
async function renewFromStorage(): Promise<string | null> {
  const sb = authClient();
  if (!sb) return null;
  // Nobody is signed in. Reading storage would confirm that at the cost of a
  // storage read per request, and under skew a network refresh with it.
  if (cache.status === 'signed-out') return null;
  // The endpoint is rate-limited and auth-js would spend ~25s on a local retry
  // ladder before giving up. Hand back what we have instead.
  if (tokenEndpointCooldownRemainingMs() > 0) return heldToken();
  if (withinFloor(lastRenewAt, MIN_RENEW_INTERVAL_MS)) return heldToken();

  const startedAt = generation;
  try {
    const { data } = await sb.auth.getSession();
    publishIfCurrent(data.session, startedAt);
  } catch {
    /* keep whatever we still hold */
  }
  lastRenewAt = Date.now();
  return heldToken();
}

/**
 * Rotate because the server refused `refused`.
 *
 * `getSession()` first, and usually only: it rotates an expired session in one
 * request where `refreshSession()` costs two, because `_useSession` always runs
 * `__loadSession` before looking at what it was handed. The escalation covers
 * what the cheap path cannot -- a token this clock still reads as fresh, which
 * is why it went out and was refused -- and answering `null` rather than the
 * dead token is deliberate: the caller is a 401 retry.
 */
async function forceRefresh(refused: string | null): Promise<string | null> {
  const sb = authClient();
  if (!sb || tokenEndpointCooldownRemainingMs() > 0) return null;
  // Nobody is signed in, so there is nothing to rotate. The read path already
  // knows this; without it here a 401 on the login screen still asks the network.
  if (cache.status === 'signed-out') return null;
  // Counted before the attempt, not after, so a rotation that fails still holds
  // the next one off. Stamping on success only would let a token the server
  // keeps refusing spend one rotation per retry, forever.
  if (withinFloor(lastForceAt, MIN_FORCE_INTERVAL_MS)) return null;
  lastForceAt = Date.now();

  const startedAt = generation;
  const startedFor = currentUserId();
  try {
    const { data, error } = await sb.auth.getSession();
    publishIfCurrent(data.session, startedAt);
    const rotated = tokenIfStill(startedFor);
    if (rotated && rotated !== refused) {
      lastRenewAt = Date.now();
      return rotated;
    }
    // The escalation below is only for the case the cheap call answered: a
    // token this clock still reads as fresh, which is why it went out and was
    // refused. If the call itself failed, `refreshSession()` is the same
    // request over the same transport, so it buys nothing and costs a second
    // auth-js retry ladder -- roughly 25s more with every concurrent read
    // parked on this flight, since a read only falls back to the held token
    // once the rotation settles.
    if (error) return null;
    // The account moved while the cheap call was out, so the escalation would
    // rotate whoever is signed in now: `refreshSession()` takes no argument and
    // refreshes the stored session. That spends a token request out of the one
    // budget this whole module exists to conserve, on a session this caller
    // never asked about, and `publishIfCurrent` would refuse the result anyway.
    if (accountChanged(startedFor)) return null;

    const { data: forced } = await sb.auth.refreshSession();
    publishIfCurrent(forced.session, startedAt);
  } catch {
    return null;
  }
  lastRenewAt = Date.now();
  const token = tokenIfStill(startedFor);
  return token && token !== refused ? token : null;
}

/**
 * Wait for `flight`, but not past `MAX_RENEWAL_WAIT_MS`, and answer with what
 * `fallback` holds instead.
 *
 * Only ever short-circuits to something real: if there is nothing held, this
 * keeps waiting, because answering a read with `null` puts an unauthenticated
 * request on the wire and that is a guaranteed 401. So the bound can only ever
 * downgrade a fresh answer to a held one, never to no answer, which is what
 * keeps the exact number above from being load-bearing.
 */
function bounded(
  flight: Promise<string | null>,
  fallback: () => string | null,
): Promise<string | null> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      const held = fallback();
      if (held) resolve(held);
    }, MAX_RENEWAL_WAIT_MS);
    const settle = (token: string | null) => {
      clearTimeout(timer);
      resolve(token ?? fallback());
    };
    flight.then(settle, () => settle(null));
  });
}

/** Drop an answer that arrived after the cache moved to a different account. */
function fenced(startedFor: string | null, p: Promise<string | null>): Promise<string | null> {
  return p.then((token) => (accountChanged(startedFor) ? null : token));
}

/** The token for an outbound request. Cached; only renews when near expiry. */
export function getAccessToken(): Promise<string | null> {
  const token = unexpiredToken();
  if (token) return Promise.resolve(token);
  // While the cool-off is open a renewal cannot succeed, so any flight still
  // running is only auth-js walking out its local ladder. Waiting on it, even
  // bounded, would put the wait on every read for the length of that ladder;
  // what we hold is already the best answer available.
  if (tokenEndpointCooldownRemainingMs() > 0) {
    const held = heldToken();
    if (held) return Promise.resolve(held);
  }
  // A forced rotation in flight produces a fresh token, which satisfies a read.
  // Fall back to what we hold when it answers null: it does that without asking
  // the network whenever the cool-off is open or the force floor is closed, and
  // a read wants the best token available, not a different one. Only the margin
  // rejected the held token, so it can still have most of a minute left in it.
  // Captured before anything is awaited, so every exit below can tell whether
  // the account it was asked for is still the one the cache answers for. The
  // fence sits here, at the read path's one exit, rather than inside each
  // renewal: `bounded` can answer from the cache without the flight resolving
  // at all, so fencing only the flights would leave the fallback unguarded.
  const startedFor = currentUserId();
  // A clock that moved backwards has disowned the expiry it wrote, and storage
  // cannot repair that: auth-js reads the same stamp, sees it sitting further in
  // the future than ever, and hands the session back without refreshing it. Only
  // a rotation yields an expiry measured on the corrected clock, so ask for one
  // the way a 401 does, naming the held token as the one not to be trusted.
  // Bounded like every other read: the floors and the cool-off still apply, and
  // a rotation that cannot run leaves the held token, which is what we had.
  if (expiryDisowned()) {
    return fenced(startedFor, bounded(refreshAccessToken(heldToken()), heldToken));
  }
  if (inFlightForce) return fenced(startedFor, bounded(inFlightForce, heldToken));
  if (inFlightRead) return fenced(startedFor, bounded(inFlightRead, heldToken));
  // Clear the slot only if it still holds THIS flight. `clearAuthToken` detaches
  // a flight without cancelling it, so a bare `inFlightRead = null` here would
  // fire late and wipe the slot a newly signed-in user had since filled -- and
  // the next reads would then run in parallel, which is the storm this bounds.
  const flight: Promise<string | null> = renewFromStorage().finally(() => {
    if (inFlightRead === flight) inFlightRead = null;
  });
  inFlightRead = flight;
  return fenced(startedFor, bounded(flight, heldToken));
}

/**
 * Rotate after the server refused `refused` -- the token that request actually
 * carried, which is not necessarily the one held now.
 *
 * Taking it as an argument is what makes a staggered burst free: the first 401
 * rotates T to U, and every straggler is then holding an answer already. Read
 * off the cache instead, each straggler would conclude U had been refused and
 * rotate again. Single-flighted for the overlapping case on top of that.
 */
// Deliberately not bounded, unlike the read paths above: a 401 retry needs a
// token the server has not already refused, and the only thing this could fall
// back to is the one it just did. Answering early would fail a request that the
// rotation behind it was about to make work. It is also one request hanging,
// not the whole tab: reads never park on this, they take the bounded path.
export function refreshAccessToken(refused: string | null): Promise<string | null> {
  const held = heldToken();
  if (held && held !== refused) return Promise.resolve(held);
  if (inFlightForce) return inFlightForce;
  const flight: Promise<string | null> = forceRefresh(refused).finally(() => {
    if (inFlightForce === flight) inFlightForce = null;
  });
  inFlightForce = flight;
  return flight;
}

/**
 * Bearer headers for the raw `fetch()` calls axios cannot make (SSE, WS).
 * Empty when there is no token, so an unauthenticated call sends no header
 * rather than an empty one.
 */
export async function getAuthHeaders(): Promise<Record<string, string>> {
  const token = await getAccessToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * The token an outgoing `Authorization` header actually carried, for handing to
 * `refreshAccessToken` after a 401. Lives here so the two 401 paths cannot
 * disagree about how a header is taken apart.
 */
export function bearerTokenOf(header: unknown): string | null {
  return typeof header === 'string' ? header.replace(/^Bearer /, '') : null;
}
