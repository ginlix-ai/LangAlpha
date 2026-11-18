import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { isImeComposing } from '@/lib/ime';
import { cn } from '@/lib/utils';
import type { Modifiers, MouseButton } from '../filePanel/browserProtocol';
import { Globe, Hand } from 'lucide-react';
import { isBlankPage, type BrowserPhase } from '../filePanel/browserPhase';
import { PHASE_CARDS } from './browserPhaseCards';
import type { BrowserViewSession } from '../filePanel/browserStream';
import { useBrowserViewState } from '../filePanel/useBrowserView';
import { keyInput, modifiersOf, mouseButton, pagePoint } from './browserInput';
import { StatusCard } from './StatusCard';
import './BrowserViewer.css';

const NUDGE_MS = 4000;

/** A pointer sweep or a flick of the wheel is a stream of events; the page needs at
 *  most one a frame. Returns the scheduler and a flush that runs what is pending now. */
function useFrameCoalesced(run: () => void): [schedule: () => void, flush: () => void] {
  const pending = useRef<number | null>(null);
  const latest = useRef(run);
  useEffect(() => { latest.current = run; });
  useEffect(() => () => { if (pending.current !== null) cancelAnimationFrame(pending.current); }, []);
  const schedule = useCallback(() => {
    if (pending.current !== null) return;
    pending.current = requestAnimationFrame(() => {
      pending.current = null;
      latest.current();
    });
  }, []);
  const flush = useCallback(() => {
    if (pending.current === null) return;
    cancelAnimationFrame(pending.current);
    pending.current = null;
    latest.current();
  }, []);
  return [schedule, flush];
}

/** The browser's live frames, with the reader's mouse and keys going to the page. */
export function BrowserViewer({ view }: { view: BrowserViewSession }): React.ReactElement {
  const { t } = useTranslation();
  const { phase, control, pageUrl, agent } = useBrowserViewState(view);
  const holding = control === 'held';
  // A click on a page that is not theirs yet says how to make it so, for a moment.
  const [nudgedAt, setNudgedAt] = useState<number | null>(null);
  useEffect(() => {
    if (nudgedAt === null) return;
    const timer = setTimeout(() => setNudgedAt(null), NUDGE_MS);
    return () => clearTimeout(timer);
  }, [nudgedAt]);
  const nudged = nudgedAt !== null && !holding;

  const canvas = useRef<HTMLCanvasElement | null>(null);
  const attachCanvas = (element: HTMLCanvasElement | null) => {
    canvas.current = element;
    view.attachCanvas(element);
  };

  // The frame as of the event, which may be newer than this render's.
  const point = (clientX: number, clientY: number) => {
    const { frame } = view.getSnapshot();
    const target = canvas.current;
    if (!frame || !target) return null;
    return pagePoint(clientX, clientY, target.getBoundingClientRect(), frame);
  };

  const held = useRef<MouseButton | undefined>(undefined);
  // Where the pointer last was on the page, for a release that lands off the
  // frame: the page lets go where the drag left it, not where it began.
  const last = useRef<{ x: number; y: number } | null>(null);
  const moved = useRef<{ x: number; y: number; modifiers: Modifiers } | null>(null);
  const [scheduleMove, flushMove] = useFrameCoalesced(() => {
    if (!moved.current) return;
    view.input({ kind: 'mouse', event: 'move', button: held.current, ...moved.current });
    moved.current = null;
  });

  const wheel = useRef<{ x: number; y: number; deltaX: number; deltaY: number; modifiers: Modifiers } | null>(null);
  const [scheduleWheel] = useFrameCoalesced(() => {
    if (!wheel.current) return;
    view.input({ kind: 'mouse', event: 'wheel', ...wheel.current });
    wheel.current = null;
  });

  // Attached by hand: React's wheel listener is passive, and scrolling the page
  // inside the frame must not scroll the panel around it.
  useEffect(() => {
    const target = canvas.current;
    if (!target) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      if (!holding) return;
      const at = point(e.clientX, e.clientY);
      if (!at) return;
      const line = e.deltaMode === WheelEvent.DOM_DELTA_LINE ? 16 : 1;
      wheel.current = {
        ...at,
        deltaX: (wheel.current?.deltaX ?? 0) + e.deltaX * line,
        deltaY: (wheel.current?.deltaY ?? 0) + e.deltaY * line,
        modifiers: modifiersOf(e),
      };
      scheduleWheel();
    };
    target.addEventListener('wheel', onWheel, { passive: false });
    return () => target.removeEventListener('wheel', onWheel);
  });

  const keys = useRef<HTMLTextAreaElement>(null);
  // Keys go to the page from the moment it is theirs, with no click first.
  useEffect(() => {
    if (holding) keys.current?.focus({ preventScroll: true });
  }, [holding]);

  // Each button's release, listened for on the window while the hold it was
  // pressed under lasts. Once the hold ends the session has let the buttons go
  // itself, and a release arriving under a later hold would be another press's.
  const releases = useRef(new Map<MouseButton, (up: MouseEvent) => void>());
  useEffect(() => {
    if (!holding) return;
    const pressed = releases.current;
    return () => {
      for (const onUp of pressed.values()) window.removeEventListener('mouseup', onUp);
      pressed.clear();
      held.current = undefined;
    };
  }, [holding]);

  const onMouseDown = (e: React.MouseEvent<HTMLCanvasElement>) => {
    // The default would move focus off the field the keys arrive in.
    e.preventDefault();
    if (!holding) {
      if (control === 'none' && phase === 'live') setNudgedAt(e.timeStamp);
      return;
    }
    keys.current?.focus({ preventScroll: true });
    const at = point(e.clientX, e.clientY);
    if (!at) return;
    const button = mouseButton(e.button);
    // A move still waiting for its frame went before this press, without the button.
    flushMove();
    held.current = button;
    last.current = at;
    view.input({ kind: 'mouse', event: 'down', button, clickCount: e.detail || 1, modifiers: modifiersOf(e), ...at });
    // On the window: a drag that ends off the frame must still let go. Each
    // button lets go of its own press, whatever else is down.
    const pressed = releases.current;
    const onUp = (up: MouseEvent) => {
      if (mouseButton(up.button) !== button) return;
      window.removeEventListener('mouseup', onUp);
      pressed.delete(button);
      // The drag's last move lands where the drag ended, under the button.
      flushMove();
      held.current = pressed.keys().next().value;
      const end = point(up.clientX, up.clientY) ?? last.current ?? at;
      view.input({ kind: 'mouse', event: 'up', button, clickCount: up.detail || 1, modifiers: modifiersOf(up), ...end });
    };
    const earlier = pressed.get(button);
    if (earlier) window.removeEventListener('mouseup', earlier);
    pressed.set(button, onUp);
    window.addEventListener('mouseup', onUp);
  };

  const onMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!holding) return;
    const at = point(e.clientX, e.clientY);
    if (!at) return;
    last.current = at;
    moved.current = { ...at, modifiers: modifiersOf(e) };
    scheduleMove();
  };

  const sendText = (text: string) => {
    if (text) view.input({ kind: 'text', text });
  };

  // Keys arrive in a hidden field rather than on the canvas, because text an input
  // method composes comes with no key of its own and only reaches something editable.
  // Everything goes to the page, which is what a reader expects of a focused frame,
  // except paste: it arrives as an event of its own only if its key is let through.
  const isPasteKey = (e: React.KeyboardEvent) => (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'v';
  // The one key kept from the page, so a keyboard alone can always leave it: it
  // hands the page back, and focus stays on the viewer for the next Tab to move on.
  const isHandBackKey = (e: React.KeyboardEvent) =>
    e.key === 'Escape' && e.shiftKey && !e.altKey && !e.ctrlKey && !e.metaKey;
  const stage = useRef<HTMLDivElement>(null);
  const onKey = (eventType: 'down' | 'up') => (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    e.stopPropagation();
    if (isImeComposing(e)) return;
    if (isHandBackKey(e)) {
      e.preventDefault();
      if (eventType === 'up') return;
      // The Shift the page saw go down is let go with the hold.
      view.releaseControl();
      stage.current?.focus({ preventScroll: true });
      return;
    }
    if (isPasteKey(e)) return;
    e.preventDefault();
    view.input(keyInput(eventType, e.nativeEvent));
  };
  const onPaste = (e: React.ClipboardEvent<HTMLTextAreaElement>) => {
    e.preventDefault();
    sendText(e.clipboardData.getData('text/plain'));
  };
  // A key's text went with the key, so what lands in the field is only what came
  // without one. Browsers disagree on whether input follows compositionend, so the
  // field is emptied there and a late input finds nothing to send twice.
  const onInput = (e: React.FormEvent<HTMLTextAreaElement>) => {
    if ((e.nativeEvent as InputEvent).isComposing) return;
    sendText(e.currentTarget.value);
    e.currentTarget.value = '';
  };
  const onCompositionEnd = (e: React.CompositionEvent<HTMLTextAreaElement>) => {
    sendText(e.data);
    e.currentTarget.value = '';
  };

  const blank = phase === 'live' && isBlankPage(pageUrl);
  const showing = (phase === 'live' || phase === 'reconnecting') && !blank;
  return (
    <div className="browser-viewer">
      {holding && (
        <div className="browser-viewer-strip" role="status">
          <Hand className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
          <span className="min-w-0 truncate">
            {t('filePanel.browser.heldNote', 'You have the browser. The agent waits until you hand it back.')}
          </span>
          <span className="browser-viewer-strip-keys">{t('filePanel.browser.heldKeys', 'Shift+Esc to hand back')}</span>
        </div>
      )}
      <div ref={stage} className="browser-viewer-stage" tabIndex={-1}>
      <canvas
        ref={attachCanvas}
        className={cn('browser-viewer-canvas', phase === 'reconnecting' && 'is-stale', !holding && 'is-watching')}
        title={showing && !holding ? t('filePanel.browser.watchingHint') : undefined}
        style={showing ? undefined : { visibility: 'hidden' }}
        role="img"
        aria-label={t('filePanel.browser.label')}
        onMouseDown={onMouseDown}
        onMouseMove={onMouseMove}
        onContextMenu={(e) => e.preventDefault()}
      />
      {holding && (
        <textarea
          ref={keys}
          className="browser-viewer-keys"
          aria-label={t('filePanel.browser.label')}
          autoCapitalize="off"
          autoComplete="off"
          autoCorrect="off"
          spellCheck={false}
          onKeyDown={onKey('down')}
          onKeyUp={onKey('up')}
          // A key let go after focus left goes to wherever it went.
          onBlur={() => view.releaseKeys()}
          onPaste={onPaste}
          onInput={onInput}
          onCompositionEnd={onCompositionEnd}
        />
      )}
      {blank ? (
        <StatusCard
          icon={Globe}
          title={t('filePanel.browser.blankTitle', 'No page open')}
          body={t(
            holding ? 'filePanel.browser.blankBodyHeld' : 'filePanel.browser.blankBody',
            holding
              ? 'Enter an address above to open a page.'
              : 'Pages the agent opens show here. Take control to go to one yourself.',
          )}
        />
      ) : (
        <BrowserNotice view={view} phase={phase} />
      )}
      {(nudged || (agent && phase === 'live')) && (
        <div className="browser-viewer-foot">
          {nudged && (
            <span className="browser-viewer-pill">
              {t('filePanel.browser.watchingHint')}
              <button type="button" className="browser-viewer-pill-action tap-target" onClick={() => view.takeControl()}>
                {t('filePanel.browser.takeControl')}
              </button>
            </span>
          )}
          {agent && phase === 'live' && (
            <span className="browser-viewer-pill" role="status">
              {/* Running, as a thread's dot says it. */}
              <span className="browser-viewer-dot animate-pulse motion-reduce:animate-none" aria-hidden="true" />
              <span className="browser-viewer-who">{t('filePanel.browser.agent', 'Agent')}</span>
              <span className="min-w-0 truncate">
                {t(`filePanel.browser.doing.${agent.key}`, { detail: agent.detail ?? '' })}
              </span>
            </span>
          )}
        </div>
      )}
      </div>
    </div>
  );
}

function BrowserNotice({ view, phase }: { view: BrowserViewSession; phase: BrowserPhase }): React.ReactElement | null {
  const { t } = useTranslation();
  const card = PHASE_CARDS[phase];
  if (!card) return null;
  const { action } = card;
  return (
    <StatusCard
      icon={card.icon}
      title={t(card.titleKey)}
      body={card.bodyKey && t(card.bodyKey)}
      action={action && { label: t(action.labelKey), run: action.run === 'open' ? () => view.open() : () => view.retry() }}
    />
  );
}
