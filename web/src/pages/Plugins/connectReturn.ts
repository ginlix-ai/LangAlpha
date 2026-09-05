/**
 * What is owed to someone whose connect left this tab and never came back.
 *
 * Some authorization servers allowlist only the RFC 8252 native-app profile: a
 * loopback callback on the user's own machine. They refuse a hosted one at the
 * authorize step, and a refusal there is silent by construction — the vendor's
 * page renders its own error and never redirects, so no code and no error ever
 * reaches us. The desktop shell can hold that listener and complete the flow; a
 * browser tab has nothing to offer.
 *
 * *Which* connectors those are is the brokerage registry's answer and whether
 * this build could finish one is `canBeginMcpOAuth`'s. Neither lives here. What
 * does is the part no advance note can cover: a provider can refuse for reasons
 * nobody listed, and the user is owed a sentence either way.
 */
import { useEffect, useRef, useState } from 'react';
import { readConnectOutcome } from './connectOutcome';

const PENDING_KEY = 'mcp:connect-started';

/**
 * The connects that have left, as a set rather than a single name.
 *
 * More than one can be out at a time. Inside the shell the authorize URL opens
 * in the system browser, so the page never leaves and the tab keeps the other
 * rows clickable on purpose. A single slot meant the second connect overwrote
 * the first's marker, and then whichever flow answered first consumed a marker
 * belonging to the other -- so an abandoned connect went unreported, or a
 * timeout announced a broker that was still live.
 */
interface Pending {
  server: string;
  /**
   * This connect is what brought the row live, so a return that says the
   * connect failed owes the user the row back the way they left it.
   *
   * It has to survive the navigation because that is where the undo became
   * unreachable: the rollback the lifecycle holds runs from a catch that sits
   * before the jump to the vendor, and everything the vendor can refuse
   * happens after it.
   */
  broughtLive: boolean;
}

function readPending(): Pending[] {
  try {
    const raw = sessionStorage.getItem(PENDING_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    if (!Array.isArray(parsed)) return [];
    return parsed.flatMap((v) => {
      const e = v as Partial<Pending> | null;
      return e && typeof e.server === 'string'
        ? [{ server: e.server, broughtLive: e.broughtLive === true }]
        : [];
    });
  } catch {
    // Unreadable or not the shape this build writes. Either way there is
    // nothing here to explain to anyone.
    return [];
  }
}

function writePending(pending: Pending[]): void {
  try {
    if (pending.length) sessionStorage.setItem(PENDING_KEY, JSON.stringify(pending));
    else sessionStorage.removeItem(PENDING_KEY);
  } catch {
    // Private windows, blocked site data.
  }
}

/**
 * Record that a connect left this tab, so the page can tell "never tried" from
 * "tried and never came back" when it next loads.
 *
 * Session-scoped and same-tab on purpose: that is exactly the span of the flow.
 * A closed tab took its unfinished connect with it, and there is nobody left to
 * explain it to.
 */
export function markConnectStarted(server: string, broughtLive: boolean): void {
  const pending = readPending();
  if (pending.some((p) => p.server === server)) {
    // A second connect for a row that already has one open. Nothing coming back
    // can say which of the two it settled: the callback carries the server name
    // and nothing of the flow, so both attempts read the same marker. The undo
    // is given up rather than guessed at. Guessing wrong switches off a row the
    // newer flow has just brought live, which costs the user a connection;
    // giving it up leaves an enabled row with nothing behind it, which they can
    // switch off themselves. The shell is where this happens at all -- it keeps
    // the page alive while the consent screen is elsewhere, so the second click
    // is reachable.
    writePending(
      pending.map((p) => (p.server === server ? { ...p, broughtLive: false } : p)),
    );
    return;
  }
  writePending([...pending, { server, broughtLive }]);
}

/**
 * What a landing owes the connects that left this tab.
 *
 * The marker is read on every landing, not only an unexplained one: the
 * callback's own params mean a flow ended, and a marker left behind would have
 * some later, ordinary visit to this page announce a failure that resolved long
 * ago.
 */
export interface ConnectReturnHandlers {
  /** A connect that left and never came back, with nothing to say for itself. */
  onAbandoned: (server: string) => void;
  /**
   * A connect that came back refused, having brought its row live on the way
   * out. Only the refusals get this: a landing with nothing to say could as
   * easily be a connect that finished in another tab, and switching off a live
   * broker on a guess is worse than leaving one switched on.
   */
  onStandDown: (server: string) => void;
}

export function useConnectReturn(handlers: ConnectReturnHandlers): void {
  // Captured during render, ahead of every effect. The component that owns the
  // callback params strips them from the URL in an effect of its own, and
  // whether that has happened yet is not a thing this hook should have to know.
  const [landedWith] = useState(() => window.location.search);
  const handler = useRef(handlers);
  handler.current = handlers;

  useEffect(() => {
    const settle = (search: string) => {
      const pending = readPending();
      if (!pending.length) return;
      const outcome = readConnectOutcome(new URLSearchParams(search));
      if (outcome) {
        // One flow answered. Retire that flow's marker and leave the rest
        // alone; on the desktop another broker may still be out. An answer
        // that names no server settles the only outstanding connect when
        // there is exactly one, and otherwise names nobody, so it retires
        // nobody.
        const settled =
          outcome.server ?? (pending.length === 1 ? pending[0].server : null);
        if (!settled) return;
        const entry = pending.find((p) => p.server === settled);
        writePending(pending.filter((p) => p.server !== settled));
        // The row this connect switched on has nothing behind it now, and a
        // brokerage left switched on is inherited by every workspace, where it
        // fails on first use with no connection to explain it.
        if (entry?.broughtLive && outcome.kind === 'failed') {
          handler.current.onStandDown(settled);
        }
        return;
      }
      // A landing with nothing to say about any flow, which is the case this
      // hook exists for: backing out of the vendor's page. In a browser the
      // page left, so there is exactly one connect it could belong to. With
      // several out, nothing here names which one, and blaming a live broker
      // reads worse than staying quiet -- so they are left to their own
      // callbacks.
      if (pending.length !== 1) return;
      writePending([]);
      handler.current.onAbandoned(pending[0].server);
    };
    settle(landedWith);
    // Backing out of the vendor's page is usually a bfcache restore, which
    // returns the tab without remounting anything. The single most common way
    // to come back from a refused connect is the one a mount effect misses.
    const onShow = (e: PageTransitionEvent) => {
      if (e.persisted) settle(window.location.search);
    };
    window.addEventListener('pageshow', onShow);
    return () => window.removeEventListener('pageshow', onShow);
  }, [landedWith]);
}
