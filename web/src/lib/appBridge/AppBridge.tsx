import React, { useCallback, useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { useTheme } from '@/contexts/ThemeContext';
import { useLocale } from '@/hooks/useLocale';
import { buildMarketViewUrl } from '@/pages/MarketView/utils/marketRoute';
import { messageOrigin } from '@/pages/ChatAgent/components/viewers/frameHost';
import { ConsentBar } from './ConsentBar';
import { kitStrings, readTheme } from './hostContext';
import { BadRequest, methodEntry, type QuoteArgs } from './methods';
import {
  ASK_FIRST,
  PROTOCOL_VERSION,
  parseAppMessage,
  type AppMessage,
  type ErrorCode,
  type OpenTarget,
  type PageMessage,
  type Quote,
  type Scope,
} from './protocol';
import { StreamPump } from './StreamPump';
import { useAppScopes } from './useAppScopes';

// A token bucket per frame: a dashboard's first paint asks for a burst, and a
// loop gone wrong in agent-written code is held to a trickle.
const BURST = 40;
const REFILL_PER_SEC = 2;

/** Takes one token, or says there is none. */
function tokenBucket(): () => boolean {
  let tokens = BURST;
  let at = 0;
  return () => {
    const now = Date.now();
    tokens = Math.min(BURST, tokens + ((now - at) / 1000) * REFILL_PER_SEC);
    at = now;
    if (tokens < 1) return false;
    tokens -= 1;
    return true;
  };
}

/** A page message as written here; `la` is added on the way out. */
type Unversioned<T> = T extends unknown ? Omit<T, 'la'> : never;
type Call = Extract<AppMessage, { type: 'req' | 'sub' }>;
type Hello = Extract<AppMessage, { type: 'hello' }>;

/** A frame's origin; a path (a version served on the API origin) is read against the page. */
function originOf(src: string): string | null {
  try { return new URL(src, window.location.href).origin; } catch { return null; }
}

/**
 * How many quote streams one document may hold at once. The kit shares one
 * between the tiles it opens together, so a page holds a handful; without a
 * bound, one that never ends its streams grows the fan-out by the second.
 */
const MAX_STREAMS = 32;

/**
 * Whether the user just acted. A click inside the frame activates every
 * ancestor, whatever its origin (checked in Chromium, WebKit and Firefox). A
 * browser without the API cannot say, so it is answered no: every browser the
 * app supports has it, and this is all that stands between a page and a
 * navigation the user never asked for.
 */
function userActive(): boolean {
  const activation = (navigator as { userActivation?: UserActivation }).userActivation;
  return activation ? activation.isActive : false;
}

/** How a frame is answered, whichever surface frames it. */
export interface BridgeOptions {
  /** What the user's grants to the frame are kept under (`appGrantKey`); null answers it but keeps none. */
  grantKey: string | null;
  /** What the consent bar calls the frame. */
  subject?: 'app' | 'page';
  /**
   * On screen. A frame kept mounted out of sight (a layer loading behind the
   * one shown, a tab not picked) reads its data but opens nothing.
   */
  active?: boolean;
  /**
   * The consent bar's "Not now", held by an owner whose frames replace each
   * other (a renewed address is a new bridge), so the bar stays away.
   */
  consent?: { dismissed: boolean; onDismiss: () => void };
  /** Told what the frame's document asked to read, and null once it is gone. Only while `active`. */
  onAsked?: (scopes: Scope[] | null) => void;
}

interface AppBridgeProps extends BridgeOptions {
  /** The element the app lays out: the frame, or the frame host holding it. */
  frameRef: React.RefObject<HTMLIFrameElement | null>;
  /** The frame's current URL; only its origin may talk to this page. */
  src: string;
  /** The frame's sandbox, which decides the origin its messages carry. */
  sandbox: string;
  /**
   * The frame's document had loaded before this bridge was mounted (a
   * document put back on screen): that load is not seen here, and counts.
   */
  loaded?: boolean;
}

/**
 * Answers the dashboard kit inside an app's frame (see `useAppFrame`). It
 * shows nothing until the app asks for something the user has to allow, and
 * then a one-line bar asking them.
 *
 * Keyed on the frame's address: a new one (a renewed signed URL, a version
 * the file panel swaps in) is a new document, which says hello again to a
 * bridge that remembers nothing.
 */
export function AppBridge(props: AppBridgeProps): React.ReactElement {
  return <Bridge key={`${props.src}\n${props.sandbox}`} {...props} />;
}

/**
 * The document a welcome went to: its window, the id its kit gave it (null
 * from a kit before ids), what it asked to read, and what the page last told
 * it, so a change is sent once and only when it is one.
 */
interface Session {
  win: MessageEventSource | null;
  id: string | null;
  asked: Scope[];
  sent: { grant: string; theme: string; locale: string };
}

/**
 * `gone` once the document loaded here went away (nothing reaches the frame
 * until it loads another address), `unmounted` once the bridge did (a read
 * still in flight must not land in whatever frame the ref holds next).
 */
type Phase = 'open' | 'gone' | 'unmounted';

function Bridge({
  frameRef, src, sandbox, loaded = false, grantKey, subject = 'app', active = true, consent, onAsked,
}: AppBridgeProps): React.ReactElement | null {
  const { i18n } = useTranslation();
  const locale = useLocale();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { theme } = useTheme();
  const { granted, grant } = useAppScopes(grantKey);
  // An app on an origin known only at runtime sits in a frame host, which
  // relays the app's own origin only and speaks for it with the page's.
  const origin = originOf(src) ? messageOrigin(src, sandbox) : null;
  // Every document in an opaque frame posts as 'null', so the origin cannot
  // tell the page loaded here from one it navigated the frame to.
  const opaque = origin === 'null';

  // A hello starts a session: the app reloaded under the same origin (its
  // reload button, its own navigation) numbers its calls afresh, so a call
  // answers only while the session it started under is the current one. The
  // handler reads the ref, since the app's first requests can land before
  // the render the hello set off; the bar reads `asked`, which the same two
  // functions write. Grants are derived, so one from another tab (or the bar
  // below) reaches the frame as it lands.
  const session = useRef<Session | null>(null);
  const [asked, setAsked] = useState<Scope[] | null>(null);
  const phase = useRef<Phase>('open');
  // The opaque frame's loads: whether a hello can be from the document
  // loaded here depends on how many there have been.
  const loads = useRef(loaded ? 1 : 0);
  const [dismissedHere, setDismissedHere] = useState(false);
  const [subs, setSubs] = useState<ReadonlyMap<string, QuoteArgs>>(() => new Map());
  // The ids held, known before the render that shows them: two streams asked
  // for in one task both count.
  const held = useRef(new Set<string>());
  const [takeToken] = useState(tokenBucket);
  const allowed = (s: Scope) => !ASK_FIRST.has(s) || granted.has(s);

  const post = useCallback((msg: Unversioned<PageMessage>) => {
    const win = frameRef.current?.contentWindow;
    const to = session.current;
    if (!win || !origin || phase.current !== 'open') return;
    if (to?.win && to.win !== win) return;
    win.postMessage({ la: PROTOCOL_VERSION, ...(to?.id ? { doc: to.id } : {}), ...msg }, origin === 'null' ? '*' : origin);
  }, [frameRef, origin]);

  const fail = (id: string, code: ErrorCode, message: string) => {
    post({ type: 'res', id, ok: false, error: { code, message } });
  };

  const clearSubs = () => {
    held.current.clear();
    setSubs((prev) => (prev.size ? new Map() : prev));
  };

  /** Everything the document is told, once, recorded as what it now knows. */
  const welcome = (s: Session) => {
    const scopes = s.asked.filter(allowed);
    s.sent = { grant: scopes.join(','), theme, locale };
    post({
      type: 'welcome',
      scopes,
      pending: s.asked.filter((x) => !allowed(x)),
      theme: readTheme(theme),
      locale,
      strings: kitStrings(i18n, locale),
    });
  };

  const begin = (s: Session) => {
    session.current = s;
    setAsked(s.asked);
    clearSubs();
    welcome(s);
  };

  const end = () => {
    session.current = null;
    setAsked(null);
    clearSubs();
  };

  /** The document loaded here went away: nothing reaches the frame until it loads another address. */
  const leave = () => {
    phase.current = 'gone';
    end();
  };

  const hello = (msg: Hello, source: MessageEventSource | null) => {
    if (!takeToken()) return; // the kit says it again shortly
    const known = session.current;
    if (known && msg.doc && known.id === msg.doc) {
      // The same document again, its hello crossing the welcome: it keeps its session.
      welcome(known);
      return;
    }
    // The document loaded into an opaque frame greets before its load (the
    // kit says hello as it runs, which the load waits for), or by then if the
    // kit came late. Any other hello is from a document it navigated to.
    if (opaque && (known ? loads.current > 0 : loads.current > 1)) {
      leave();
      return;
    }
    begin({ win: source, id: msg.doc, asked: msg.scopes, sent: { grant: '', theme: '', locale: '' } });
  };

  const open = (target: OpenTarget) => {
    if ('thread' in target) {
      navigate(`/chat/t/${encodeURIComponent(target.thread)}`, {
        state: target.workspace ? { workspaceId: target.workspace } : undefined,
      });
    } else if ('workspace' in target) {
      navigate(`/chat/${encodeURIComponent(target.workspace)}`);
    } else {
      navigate(buildMarketViewUrl({ symbol: target.symbol.toUpperCase() }));
    }
  };

  // Leaving the page is the user's to start: only from the layer on screen,
  // on a click or a key in it, and within the bucket. The activation alone
  // is the whole page's, so a click on the app's own controls around the
  // frame would pass for one; the frame holding the focus is what says the
  // user was in it.
  const openFor = (target: OpenTarget) => {
    const inFrame = document.activeElement === frameRef.current;
    if (!active || !inFrame || !userActive() || !takeToken()) return;
    open(target);
  };

  const unsub = (id: string) => {
    held.current.delete(id);
    setSubs((prev) => {
      if (!prev.has(id)) return prev;
      const next = new Map(prev);
      next.delete(id);
      return next;
    });
  };

  /** A read or a subscription: scope and rate are checked before any data is. */
  const call = async (msg: Call, mine: Session) => {
    const { id } = msg;
    const live = () => session.current === mine;
    const entry = methodEntry(msg.method);
    if (!entry) return fail(id, 'bad_request', `Unknown method ${msg.method}`);
    const refused = () => fail(id, 'scope', `This app has no access to ${entry.scope}.`);
    // Asked again at the answer: a scope taken back while the read was under
    // way is no longer the page's, and the session holds what it was last told.
    const answer = (data: unknown) => {
      if (!live()) return;
      if (!mine.sent.grant.split(',').includes(entry.scope)) return refused();
      post({ type: 'res', id, ok: true, data });
    };
    // A scope counts only if the app declared it: a grant is to what the user
    // was shown, not to whatever the app asks for later.
    if (!mine.asked.includes(entry.scope) || !allowed(entry.scope)) return refused();
    if (!takeToken()) return fail(id, 'rate', 'Too many requests; slow down.');
    try {
      if (entry.kind === 'read') {
        if (msg.type === 'sub') throw new BadRequest(`${msg.method} cannot be streamed.`);
        answer(await entry.run(msg.params, queryClient));
        return;
      }
      if (msg.type === 'req') throw new BadRequest(`${msg.method} is a subscription.`);
      try {
        // Held at once, so an unsub can land before the answer; the answer is
        // the quotes as they stand, and the rows follow as they change.
        const args = entry.parse(msg.params);
        if (!held.current.has(id) && held.current.size >= MAX_STREAMS) {
          return fail(id, 'rate', 'Too many live streams; end one first.');
        }
        held.current.add(id);
        setSubs((prev) => new Map(prev).set(id, args));
        answer(await entry.first(args, queryClient));
      } catch (e) {
        // The kit drops a refused stream, so nothing is streamed for it. A new
        // session can reuse the id, hence only within this one.
        if (live()) unsub(id);
        throw e;
      }
    } catch (e) {
      if (!live()) return;
      fail(id, e instanceof BadRequest ? 'bad_request' : 'failed', (e instanceof Error && e.message) || 'Request failed');
    }
  };

  const onMessage = useEffectEvent((event: MessageEvent) => {
    // The frame's window and its origin both: a navigation inside the frame
    // keeps the window but not the origin, and nothing but the app's own
    // origin speaks for it (a frame host relays only that one).
    if (phase.current !== 'open' || event.source !== frameRef.current?.contentWindow || event.origin !== origin) return;
    const msg = parseAppMessage(event.data);
    if (!msg) return;
    if (msg.type === 'hello') return hello(msg, event.source);
    // Nothing is answered outside a session; a kit names its document on
    // every message, and one naming another was not welcomed.
    const known = session.current;
    if (!known || (known.id && msg.doc !== known.id)) return;
    switch (msg.type) {
      case 'bye':
        // Whatever loads next in an opaque frame is not the page loaded here.
        return opaque ? leave() : end();
      case 'open': return openFor(msg.target);
      case 'unsub': return unsub(msg.id);
      case 'req':
      case 'sub': return void call(msg, known);
    }
  });

  useEffect(() => {
    if (phase.current === 'unmounted') phase.current = 'open';
    return () => { phase.current = 'unmounted'; };
  }, []);

  // The first load is the document this address put in the frame; another is
  // the frame navigating itself (a link, a script), which no grant covers.
  const onFrameLoad = useEffectEvent(() => {
    loads.current += 1;
    if (loads.current > 1 && phase.current === 'open') leave();
  });
  // Heard from the commit that puts the frame in the page, so no load can come
  // first: one missed would leave the next document counted as this one. On
  // the document, in capture, since this runs before a frame later in the
  // tree has its ref.
  useLayoutEffect(() => {
    if (!opaque) return;
    const handle = (event: Event) => {
      if (event.target === frameRef.current) onFrameLoad();
    };
    document.addEventListener('load', handle, true);
    return () => document.removeEventListener('load', handle, true);
  }, [frameRef, opaque]);

  useEffect(() => {
    if (!origin) return;
    const handle = (event: MessageEvent) => onMessage(event);
    window.addEventListener('message', handle);
    return () => window.removeEventListener('message', handle);
  }, [origin]);

  const onQuote = useCallback((id: string, quote: Quote) => post({ type: 'evt', id, data: quote }), [post]);

  // What changed since the session was last told: a grant, the theme, the language.
  const sync = useEffectEvent(() => {
    const s = session.current;
    if (!s) return;
    const scopes = s.asked.filter(allowed);
    const grantNow = scopes.join(',');
    if (grantNow !== s.sent.grant) {
      s.sent.grant = grantNow;
      post({ type: 'grant', scopes });
    }
    if (theme !== s.sent.theme) {
      s.sent.theme = theme;
      post({ type: 'theme', theme: readTheme(theme) });
    }
    if (locale !== s.sent.locale) {
      s.sent.locale = locale;
      post({ type: 'locale', locale, strings: kitStrings(i18n, locale) });
    }
  });
  useEffect(() => { sync(); }, [granted, theme, locale]);

  const reportAsked = useEffectEvent(() => onAsked?.(asked));
  useEffect(() => {
    if (active) reportAsked();
  }, [asked, active]);

  const pending = (asked ?? []).filter((s) => !allowed(s));
  const dismissed = consent ? consent.dismissed : dismissedHere;
  const showBar = pending.length > 0 && !dismissed && !!grantKey;

  return (
    <>
      {subs.size > 0 && <StreamPump subs={subs} onQuote={onQuote} />}
      {showBar && (
        <ConsentBar
          subject={subject}
          pending={pending}
          visible={active}
          onAllow={() => grant(pending)}
          onDismiss={consent ? consent.onDismiss : () => setDismissedHere(true)}
        />
      )}
    </>
  );
}
