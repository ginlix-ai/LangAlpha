import React, { useEffect, useEffectEvent, useReducer, useRef, useState } from 'react';
import { Loader } from '@/components/ui/loader';
import { DURATION } from '@/lib/motion';
import { useTranslation } from 'react-i18next';
import { HostedFrame } from '../HostedFrame';
import { messageOrigin } from '../frameHost';
import { frameSwap, isReady, isShown, NO_SWAP, type FramedDoc, type FrameSwap, type FrameSwapAction } from './frameSwap';
import { SETTLE_CAP_MS, type PageToViewer } from './pageProtocol';
import { usePageBridge, type PageBridge } from './usePageBridge';
import './BridgedFrame.css';

/**
 * How long a loaded document may keep building itself before it replaces the
 * one on screen anyway: a document that never goes still (a chart that
 * animates) would otherwise never be shown. Past the bridge's own cap on
 * waiting, so one that settles at all settles first; the one on screen stays
 * readable meanwhile.
 */
const REVEAL_BACKSTOP_MS = SETTLE_CAP_MS + 500;
/**
 * How long a loaded document has to say hello before it is taken for one
 * without the bridge. A document's bridge says it as its first script runs,
 * before the load; this covers the relay through a frame host.
 */
const UNBRIDGED_GRACE_MS = 2000;
/**
 * How long a ready document waits before it fades in. A frame drawn at all
 * but no opacity is a few frames behind on its own picture: faded in at
 * once, it first showed the document from before it was scrolled back, for a
 * frame. Measured gone at 40ms; this leaves room.
 */
const REVEAL_HOLD_MS = 80;
/** The document being replaced is dropped once the next has faded in over it. */
const PREVIOUS_HOLD_MS = (DURATION.quick + DURATION.exit) * 1000;
/** The loading cover stays mounted while it fades out. */
const COVER_HOLD_MS = DURATION.exit * 1000;

/**
 * The ground each file last reported, so the next frame of it opens on the
 * file's colour rather than the app's: a dark report under the light theme
 * otherwise flashes white behind its loading cover each time it is opened.
 */
const knownGrounds = new Map<string, string | null>();

/** What to frame, and how. */
export interface FrameSource {
  /**
   * Which document this is: the live file at one content, or one version. A
   * new key loads in a second frame over the one on screen; a new `src` under
   * the same key (a renewed grant) reloads nothing.
   */
  key: string;
  src: string;
  /** Which origin the document posts from follows from it: opaque without `allow-same-origin`. */
  sandbox: string;
}

/** The document the frame holds. */
export interface FrameView {
  /** The `FrameSource.key` it was loaded for, which can trail the source while the next one loads. */
  key: string;
  /**
   * The document is the one on screen: loaded, and past the cover or the
   * document it replaces. Until then what it reports is not what the reader
   * sees, so nothing is drawn or opened against it.
   */
  shown: boolean;
}

interface BridgedFrameProps {
  /** Null while what to frame is not known yet: the cover shows. */
  source: FrameSource | null;
  title: string;
  /** The file the ground is remembered for. */
  groundKey: string;
  /** Shown instead of the frame: a failure, or a version that does not exist. */
  notice?: React.ReactNode;
  /**
   * Drawn over the frame, in the app's DOM rather than the document's, with
   * the document the frame holds. The layer passes pointer events through;
   * what it draws takes them back where it needs them. Its box is the frame's,
   * so a rect the document reports needs no conversion.
   */
  overlay?: (bridge: PageBridge, view: FrameView) => React.ReactNode;
  /** Hears every message the document sends, for whoever holds the frame. */
  onBridge?: (bridge: PageBridge) => void;
  /**
   * A document the frame loaded, a link inside it followed included, that
   * never said hello: not a bridged document, so most likely an error page
   * (a version's files answer 404 once an Update or an unshare ended it).
   */
  onUnbridged?: () => void;
}

/**
 * A document that speaks the page bridge, framed so another document of the
 * same file (the agent's edit reloading the live file, a version picked)
 * builds itself in a second frame over the one on screen, all but
 * transparent, until it has been scrolled to where the reader was; then it
 * fades in. A swap that blanked the frame showed the reader a bare sheet and
 * the top of the document each time.
 *
 * The frame's address is pinned when the key changes, not whenever the
 * source's `src` does: the document in the frame is already loaded, and a new
 * address would reload it under the reader.
 */
export function BridgedFrame({
  source, title, groundKey, notice, overlay, onBridge, onUnbridged,
}: BridgedFrameProps): React.ReactElement {
  const { t } = useTranslation();
  const frameKey = source?.key ?? null;
  const [swapState, dispatch] = useReducer(frameSwap, NO_SWAP);
  // Read from here on as it is about to be, so this pass does not act on a
  // frame already replaced (it would mount the first load's cover).
  let swap: FrameSwap = swapState;
  if (source && swap.current?.doc.key !== source.key) {
    const action: FrameSwapAction = {
      type: 'frame',
      doc: { key: source.key, src: source.src, origin: messageOrigin(source.src, source.sandbox), sandbox: source.sandbox },
    };
    swap = frameSwap(swap, action);
    dispatch(action);
  }
  const frame = frameKey && swap.current?.doc.key === frameKey ? swap.current.doc : null;
  const current = frame?.key ?? null;
  const behind = swap.behind;
  const loaded = !!frame && !!swap.current?.loaded;
  const ready = !!frame && isReady(swap);
  const shown = !!frame && isShown(swap);

  useEffect(() => {
    if (!ready || !behind || !current) return;
    const timer = setTimeout(() => dispatch({ type: 'held', key: current }), REVEAL_HOLD_MS);
    return () => clearTimeout(timer);
  }, [ready, behind, current]);
  useEffect(() => {
    if (!loaded || !behind || ready || !current) return;
    const timer = setTimeout(() => dispatch({ type: 'overdue', key: current }), REVEAL_BACKSTOP_MS);
    return () => clearTimeout(timer);
  }, [loaded, behind, ready, current]);
  const behindKey = behind?.key ?? null;
  useEffect(() => {
    if (!shown || !behindKey) return;
    const timer = setTimeout(() => dispatch({ type: 'drop', key: behindKey }), PREVIOUS_HOLD_MS);
    return () => clearTimeout(timer);
  }, [shown, behindKey]);

  const frameRef = useRef<HTMLIFrameElement>(null);
  const bridge = usePageBridge({ frameRef, origin: frame?.origin ?? null, frameKey: current });
  const reportBridge = useEffectEvent((b: PageBridge) => onBridge?.(b));
  useEffect(() => { reportBridge(bridge); }, [bridge]);

  // Where the reader was, carried to the next document, and the ground the
  // document paints, drawn behind the frame so a swap or a load on a dark
  // document does not flash the app's own.
  const lastScroll = useRef<{ x: number; y: number } | null>(null);
  const carry = useRef<{ key: string; at: { x: number; y: number } | null; done: boolean } | null>(null);
  useEffect(() => {
    if (current) carry.current = { key: current, at: lastScroll.current, done: false };
  }, [current]);
  const [ground, setGround] = useState<string | null>(() => knownGrounds.get(groundKey) ?? null);
  const { subscribe, post } = bridge;
  // A load waiting for its hello, or a hello that came before its load. The
  // flags answer at once, since a hello can follow its load before React
  // renders again; `loads` restarts the wait for each load.
  const greeted = useRef(false);
  const waiting = useRef(false);
  const [loads, setLoads] = useState(0);
  const onFrameLoad = (key: string) => {
    dispatch({ type: 'loaded', key });
    if (greeted.current) {
      greeted.current = false;
      return;
    }
    waiting.current = true;
    setLoads((n) => n + 1);
  };
  // A link in the document led somewhere that speaks no bridge (an image, a
  // raw error): what is drawn over the document no longer sits on anything.
  const [offPageOf, setOffPageOf] = useState<string | null>(null);
  const unbridged = useEffectEvent(() => {
    setOffPageOf(current);
    onUnbridged?.();
  });
  useEffect(() => {
    if (!waiting.current) return;
    const timer = setTimeout(() => {
      if (!waiting.current) return;
      waiting.current = false;
      unbridged();
    }, UNBRIDGED_GRACE_MS);
    return () => clearTimeout(timer);
  }, [loads]);
  useEffect(() => {
    greeted.current = false;
    waiting.current = false;
  }, [current]);

  const onMessage = useEffectEvent((message: PageToViewer) => {
    switch (message.type) {
      case 'page:scroll': {
        lastScroll.current = { x: message.x, y: message.y };
        const at = carry.current?.key === current ? carry.current.at : null;
        if (current && at && at.y > 0 && !message.moving && Math.abs(message.y - at.y) <= 1 && Math.abs(message.x - at.x) <= 1) {
          dispatch({ type: 'landed', key: current });
        }
        return;
      }
      case 'page:settled':
        if (current) dispatch({ type: 'settled', key: current });
        return;
      case 'page:ground':
        knownGrounds.set(groundKey, message.color);
        setGround(message.color);
        return;
      case 'page:ready': {
        setOffPageOf(null);
        if (waiting.current) waiting.current = false;
        else greeted.current = true;
        // The first hello of a new frame, not a link inside it to another file.
        const pending = carry.current;
        if (!pending || pending.key !== current || pending.done) return;
        pending.done = true;
        if (pending.at) post({ type: 'page:restore-scroll', x: pending.at.x, y: pending.at.y });
        return;
      }
      default:
    }
  });
  useEffect(() => subscribe((message) => onMessage(message)), [subscribe]);

  // A first load has nothing to hold on screen, so it waits under a cover,
  // which fades once the document is in.
  const loading = !notice && !behind && (!frame || !loaded);
  const [coverHeld, setCoverHeld] = useState(loading);
  if (loading && !coverHeld) setCoverHeld(true);
  useEffect(() => {
    if (loading || !coverHeld) return;
    const timer = setTimeout(() => setCoverHeld(false), COVER_HOLD_MS);
    return () => clearTimeout(timer);
  }, [loading, coverHeld]);

  // Appended in order and never moved: a frame moved in the DOM reloads.
  const frames = notice ? [] : [behind, frame].filter((f): f is FramedDoc => f !== null);

  return (
    <div
      className="bridged-frame"
      style={ground ? ({ '--page-ground': ground } as React.CSSProperties) : undefined}
    >
      {frames.map((f) => {
        const isCurrent = f.key === current;
        return (
          <HostedFrame
            // A new frame per load rather than a new address on the same one:
            // changing a frame's src adds a step to the browser's history, and
            // Back would then walk the file's documents instead of the app.
            key={f.key}
            ref={isCurrent ? frameRef : undefined}
            src={f.src}
            className="bridged-frame-doc"
            data-state={!isCurrent ? 'leaving' : shown || !behind ? 'shown' : 'pending'}
            title={title}
            sandbox={f.sandbox}
            relay
            tabIndex={isCurrent ? undefined : -1}
            aria-hidden={isCurrent ? undefined : true}
            onLoad={isCurrent ? () => onFrameLoad(f.key) : undefined}
          />
        );
      })}
      {frame && !notice && overlay && (
        <div className="bridged-frame-overlay">{overlay(bridge, { key: frame.key, shown: shown && offPageOf !== frame.key })}</div>
      )}
      {coverHeld && !notice && (
        <div className="bridged-frame-cover" data-state={loading ? 'open' : 'closed'}>
          <Loader size={24} className="text-(--color-text-tertiary)" label={t('common.loading')} />
        </div>
      )}
      {notice && <div className="bridged-frame-cover">{notice}</div>}
    </div>
  );
}
