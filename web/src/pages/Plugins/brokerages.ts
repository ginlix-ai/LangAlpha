/**
 * The join between a catalog row and the brokerage it belongs to, and what
 * each vendor's quirks mean for the surfaces that draw them.
 *
 * The registry itself is the API layer's (`Brokerage`, `getBrokerages`) and the
 * hooks over it are `useMcpServers`', beside every other MCP query -- a
 * brokerage row is an ordinary catalog row and shares their invalidation
 * radius. What is left here is the part that is neither: which row is which
 * vendor, and whether this build can start a connect against one at all.
 */
import { canBeginMcpOAuth, isDesktopShell } from '@/lib/desktop';
import { needsOauthConnect } from '@/pages/ChatAgent/components/mcp/mcpState';
import type {
  Brokerage,
  CapabilityGroup,
  McpOauthStatus,
} from '@/pages/ChatAgent/utils/api';

// Re-exported because this module is where the concept lives for every surface
// that draws one; the wire shape is declared with the call that fetches it.
export type { Brokerage, CapabilityGroup };

/**
 * The brokerage a server address belongs to, matched on host.
 *
 * Host and not the full URL: a row is the user's to edit once it exists, and
 * moving to a sibling path on the same vendor host keeps every reason the
 * vendor quirks applied. A different host is a different server, and claiming
 * a vendor's constraints for it would be a guess.
 */
export function brokerageForUrl(
  url: string | null | undefined,
  brokerages: readonly Brokerage[],
): Brokerage | null {
  if (!url) return null;
  const host = hostOf(url);
  if (!host) return null;
  return brokerages.find((b) => hostOf(b.url) === host) ?? null;
}

function hostOf(url: string): string | null {
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return null;
  }
}

/** Why a connect cannot start here, or null when the click is live. */
export type ConnectBlock = 'shell-outdated' | 'native-only' | 'unknown' | null;

/**
 * Whether this build can begin a connect against a vendor at all.
 *
 * One answer for every surface that asks. The two rows and the tab intro each
 * used to spell this out themselves, and the copy and the button drifted apart:
 * both said "desktop only" while only one refused the click, so the other led
 * straight into the dead end `connectReturn` exists to apologise for. A vendor
 * with no quirk, and every vendor at all once the shell is holding the
 * listener, answers null and behaves exactly as before.
 */
export function connectBlock(vendor: Brokerage | null | undefined): ConnectBlock {
  // Asked before the vendor, because it does not depend on one. In a shell with
  // no loopback listener NOTHING can connect: the consent screen opens in the
  // system browser and its reply comes home to a cookie jar that never saw the
  // flow. That is every row on the page, brokerage or not, so this is the first
  // and broadest answer rather than a special case inside the vendor's.
  if (isDesktopShell() && !canBeginMcpOAuth()) return 'shell-outdated';
  // `undefined` is the registry not answered yet, and it is not the same as
  // `null`, which is a row that resolved to no vendor. Reading the two alike is
  // how a backend too old to serve the registry -- or simply the moment before
  // the query settles -- turns every row into one with no constraints, and a
  // native-only vendor gets a live button into the dead end.
  if (vendor === undefined) return 'unknown';
  return vendor?.native_callback_only && !canBeginMcpOAuth() ? 'native-only' : null;
}

/**
 * The grant a row behaves as, which is not always the one it stored.
 *
 * Null means two different things and only one of them is "nothing to read".
 * With no connection it is the offer, drawn unsettled. On a live connection it
 * is a brokerage connected before its tools were curated: the dialog had no
 * groups to store, and the relay refuses every call. Reading that as the offer
 * would draw every group granted on a connection that permits nothing.
 *
 * Shared because three surfaces answer it -- the row's badges, the row's note
 * and the detail overlay -- and a row whose rungs read "offered" beside its own
 * note reading "granted nothing" was telling the reader both at once.
 */
export function settledGrant(
  granted: string[] | null | undefined,
  oauthStatus: McpOauthStatus | null | undefined,
): string[] | null {
  if (granted != null) return granted;
  return oauthStatus && !needsOauthConnect(oauthStatus) ? [] : null;
}

/**
 * The groups a connect starts with ticked.
 *
 * Everything the vendor offers except the ones that place real orders. A
 * brokerage connected with nothing ticked is a broker that does nothing, which
 * reads as broken rather than careful; ticking everything is what this change
 * exists to stop being the only option. The line is drawn at `danger` because
 * that is the tone's whole meaning, so a group added later lands on the right
 * side of it without anyone remembering to come back here.
 */
export function defaultGrant(vendor: Brokerage | null | undefined): string[] {
  return (vendor?.capabilities ?? [])
    .filter((group) => group.tone !== 'danger')
    .map((group) => group.key);
}

/**
 * Whether connecting this vendor has to ask something first.
 *
 * Both questions, because they are asked in one place: what the connection may
 * do, and -- for a vendor allowing one connected AI platform per account --
 * what it costs elsewhere. A vendor with neither is connected on the click, as
 * every ordinary OAuth server always has been.
 */
export function connectAsks(vendor: Brokerage | null | undefined): boolean {
  // `?? []` for the same reason the wire model names the field at all: a build
  // that reaches a backend which predates it gets undefined, and the question
  // that must still be asked is the vendor's own terms.
  return !!vendor && (vendor.exclusive_connection || (vendor.capabilities ?? []).length > 0);
}

/**
 * The steps between reading and placing an order that this vendor has, in
 * ladder order.
 *
 * Read off the group's own `rung` rather than a list of keys held here: which
 * groups are order steps is the backend's fact, and the display order is the
 * one the registry already sends. What this owns is only the question -- what
 * can this broker do about orders -- which the row, the badges and the detail
 * all ask in exactly these words.
 */
export function orderRungs(vendor: Brokerage | null | undefined): CapabilityGroup[] {
  return (vendor?.capabilities ?? []).filter((group) => group.rung);
}

/**
 * What a brokerage can do about orders right now, and how sure we are.
 *
 * `granted` is null before there is a connection, and the honest answer then is
 * what the broker offers rather than what it may do -- nothing may be done yet.
 * Once a connection exists the grant is the answer, and a group the vendor
 * dropped since is not one of them: the stored keys are the user's intent, and
 * a key the registry no longer offers expands to no tools at all.
 */
export function activeRungs(
  vendor: Brokerage | null | undefined,
  granted: string[] | null | undefined,
): { rungs: CapabilityGroup[]; settled: boolean } {
  const offered = orderRungs(vendor);
  if (granted == null) return { rungs: offered, settled: false };
  return { rungs: offered.filter((g) => granted.includes(g.key)), settled: true };
}
