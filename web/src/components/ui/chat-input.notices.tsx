import {
  createContext, use, useEffect, useEffectEvent, useMemo, useRef, useState,
  type KeyboardEvent as ReactKeyboardEvent, type ReactNode, type RefObject,
} from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, Info } from 'lucide-react';
import type { DueSummary } from '@/pages/ChatAgent/hooks/useNextSummaryHint';
import { userSessionStorage } from '@/lib/userStorage';
import { cn } from '@/lib/utils';
import { Popover, PopoverAnchor, PopoverContent } from './popover';

/** How long an offer stays up while the pointer and focus are elsewhere. */
export const OFFER_EXPIRES_MS = 8000;

interface NoticeBase {
  /** Who the notice is. A key not seen before opens its popover once on its own. */
  key: string;
  /** The sentence, which the marked control also carries for a screen reader. */
  text: string;
  /** The sentence and its actions, drawn with `NoticeBody`, which reads the
   *  sentence from the frame. */
  content: ReactNode;
}

/**
 * Something about the next send, said on the composer control it is about.
 * A warning holds while its condition does, marking the control and heading
 * the model picker; an offer follows a change the user just made and goes
 * once it closes, answered or not, so it must say what each way out does.
 */
export type ComposerNotice =
  | (NoticeBase & { tone: 'warning'; onDismiss?: never; onExpire?: never })
  | (NoticeBase & {
    tone: 'offer';
    /** Answered: Not now, or Escape. */
    onDismiss: () => void;
    /** Let go: its time ran out, or it closed any other way. */
    onExpire: () => void;
  });

/** What a thread host says on its composer's controls (`useComposerNotices`). */
export interface ComposerNotices {
  /** On the model pill: the most urgent of the model's notices. */
  model: ComposerNotice | null;
  /** On the subagents toggle. */
  subagents: ComposerNotice | null;
  /** On the context ring, while a summary is due. */
  summary: DueSummary | null;
}

interface NoticeFrame {
  tone: ComposerNotice['tone'];
  text: string;
  /** One line with the action at its end, as a row of the picker it heads;
   *  otherwise the actions go under the sentence, as in a popover. */
  row?: boolean;
  /** Present where the frame offers the way out (the popover, not the picker). */
  notNow?: () => void;
  /** Shuts whatever the notice is drawn in, for an action that settles it. */
  close?: () => void;
}

const NoticeFrameContext = createContext<NoticeFrame>({ tone: 'offer', text: '' });

/** A notice's sentence, with its actions under it, Not now first where the frame has one. */
export function NoticeBody({ children }: { children?: ReactNode }) {
  const { t } = useTranslation();
  const { tone, text, notNow, row } = use(NoticeFrameContext);
  const Icon = tone === 'warning' ? AlertTriangle : Info;
  return (
    <div className={row ? 'flex items-start gap-3' : 'flex flex-col gap-2.5'}>
      <p role="status" className="flex items-start gap-2 text-[0.8125rem] leading-snug text-(--color-text-secondary)">
        <Icon
          aria-hidden
          className={cn('mt-0.5 h-3.5 w-3.5 flex-none', tone === 'warning' ? 'text-(--color-warning)' : 'text-(--color-text-tertiary)')}
        />
        <span className="min-w-0 text-pretty">{text}</span>
      </p>
      {(children || notNow) && (
        <div className={cn('flex flex-wrap items-center justify-end gap-1.5', row && 'ml-auto flex-none')}>
          {notNow && (
            <button
              type="button"
              onClick={notNow}
              className="tap-target [--tap-target-w:100%] rounded-md px-2 py-1 text-xs font-medium text-(--color-text-tertiary) hover:bg-(--color-bg-hover) hover:text-(--color-text-secondary)"
            >
              {t('chat.composerNotice.notNow')}
            </button>
          )}
          {children}
        </div>
      )}
    </div>
  );
}

/** A notice's action: the one filled button in it. */
export function NoticeAction({ onClick, disabled = false, autoFocus = false, closes = false, children }: {
  onClick: () => void;
  disabled?: boolean;
  autoFocus?: boolean;
  /** It settles the notice, so it shuts what the notice is drawn in (the picker too). */
  closes?: boolean;
  children: ReactNode;
}) {
  const { close, row } = use(NoticeFrameContext);
  return (
    <button
      type="button"
      onClick={() => {
        onClick();
        if (closes) close?.();
      }}
      disabled={disabled}
      autoFocus={autoFocus}
      // In a row of the picker, a quiet text action like the picker's own;
      // in a popover, the one filled button.
      className={cn(
        'press tap-target [--tap-target-w:100%] whitespace-nowrap rounded-md text-xs font-medium disabled:opacity-50',
        // In the picker's header the search sits close below.
        row && '[--tap-target-h:2rem]',
        row
          ? '-my-0.5 px-1.5 py-0.5 text-(--color-text-primary) underline decoration-(--color-border-default) underline-offset-2 hover:bg-(--color-bg-hover) hover:decoration-current'
          : 'bg-(--color-btn-primary-bg) px-2.5 py-1 text-(--color-btn-primary-text) hover:opacity-90',
      )}
    >
      {children}
    </button>
  );
}

/** A warning at the top of the model picker, so it stays in reach once its popover has closed. */
export function NoticeHeader({ notice, onClose }: { notice: ComposerNotice; onClose: () => void }) {
  const { tone, text } = notice;
  const frame = useMemo<NoticeFrame>(() => ({ tone, text, close: onClose, row: true }), [tone, text, onClose]);
  // Untinted: the pill under the picker already carries the warning's color,
  // and a tinted block here read as a banner pasted over the list.
  return (
    <div data-composer-notice className="rule-b flex-none px-3 py-2.5">
      <NoticeFrameContext value={frame}>{notice.content}</NoticeFrameContext>
    </div>
  );
}

/** Warnings open on their own once a session, since each holds until its
 *  condition clears and the control stays marked meanwhile. Offers follow
 *  something the user just did, so each one opens. */
const SEEN_PREFIX = 'composer-notice-seen:';

/** Whether the element is laid out where it can be seen; a cached view's
 *  composer is in the page but hidden, and a popover there would float free. */
function isShown(el: Element): boolean {
  if (el.checkVisibility) return el.checkVisibility();
  // Safari before 17.4 has no `checkVisibility`: what it reports is whether
  // the element or an ancestor is `display: none`.
  for (let at: Element | null = el; at; at = at.parentElement) {
    if (getComputedStyle(at).display === 'none') return false;
  }
  return true;
}

const TABBABLE = 'a[href], button, input, select, textarea, [tabindex], [contenteditable="true"]';

/** Whether Tab stops on the element: in the tab order, enabled, and drawn. */
const canTab = (el: HTMLElement) => el.tabIndex >= 0 && !el.matches(':disabled')
  && isShown(el) && getComputedStyle(el).visibility !== 'hidden';

const tabbables = (root: ParentNode) => Array.from(root.querySelectorAll<HTMLElement>(TABBABLE)).filter(canTab);

/** Where Tab goes from the anchor when the popover is not there: the next
 *  stop after it in the page, leaving out the popover, portaled at the end. */
function nextAfter(anchor: HTMLElement, popover: HTMLElement): HTMLElement | null {
  return Array.from(document.querySelectorAll<HTMLElement>(TABBABLE)).find((el) => !popover.contains(el)
    && !anchor.contains(el) && !!(anchor.compareDocumentPosition(el) & Node.DOCUMENT_POSITION_FOLLOWING)
    && canTab(el)) ?? null;
}

/** The popover's widest, as wide as a notice's sentence reads well. */
const POPOVER_MAX_W = 320;

/** The composer's edges across the window, which the popover keeps within:
 *  in a narrow composer (a side panel, the chat beside a file) the window's
 *  edge would let it spill over the page beside it. */
interface BoxEdges { left: number; right: number; width: number }

function composerEdges(anchor: Element): BoxEdges | null {
  const box = anchor.closest('[data-composer-box]')?.getBoundingClientRect();
  if (!box) return null;
  return { left: box.left, right: window.innerWidth - box.right, width: box.width };
}

/**
 * A notice's popover, anchored to its control. It opens on its own when the
 * notice first appears, without taking focus from the draft, and Tab from
 * the control steps into it. It closes on Not now, Escape from inside it, a
 * press elsewhere, or focus leaving the composer. Closing it lets an offer
 * go; a warning stays on its control.
 */
export function NoticePopover({ notice, anchorRef, blocked = false, align = 'end', side = 'top' }: {
  notice: ComposerNotice | null;
  /** The control it is about. A new ref object moves it, as a toggle folding into ⋯ does. */
  anchorRef: RefObject<HTMLElement | null>;
  /** Something else is open on the control (the model picker); it waits for that. */
  blocked?: boolean;
  align?: 'start' | 'end';
  side?: 'top' | 'bottom';
}) {
  // What it showed last, so it can shrink away still showing it once the
  // notice is gone.
  const [shown, setShown] = useState(notice);
  if (notice && notice !== shown) setShown(notice);
  const [openKey, setOpenKey] = useState<string | null>(null);
  // A notice that goes is closed, so the same one coming back opens only as a
  // new one would: a warning already seen stays shut. Kept while it is only
  // blocked, so it resumes once the picker shuts.
  if (!notice && openKey !== null) setOpenKey(null);
  const [edges, setEdges] = useState<BoxEdges | null>(null);
  const [pointerOn, setPointerOn] = useState(false);
  const [focusOn, setFocusOn] = useState(false);
  const contentRef = useRef<HTMLDivElement>(null);
  // The picker opening over it (a refusal's "Choose a model" opens it with no
  // press) hides it until the picker shuts.
  const open = !!notice && openKey === notice.key && !blocked;

  const key = notice?.key ?? null;
  const tone = notice?.tone ?? null;
  useEffect(() => {
    if (!key || !tone) return;
    if (tone === 'warning' && userSessionStorage.getItem(SEEN_PREFIX + key) !== null) return;
    const anchor = anchorRef.current;
    if (!anchor) return;
    let frame = 0;
    const show = () => {
      // The picker heads with the warning while it is open, which says it,
      // once it is in view: a hidden cached view's picker says nothing.
      if (blocked) {
        if (tone === 'warning') userSessionStorage.setItem(SEEN_PREFIX + key, '1');
        return;
      }
      // A frame on, so the anchor has its place before the popover takes it.
      frame = requestAnimationFrame(() => {
        // Read again here: a hidden cached view waits for its anchor to show,
        // and another view may have shown or answered the warning meanwhile.
        if (tone === 'warning' && userSessionStorage.getItem(SEEN_PREFIX + key) !== null) return;
        setEdges(composerEdges(anchor));
        setPointerOn(false);
        setFocusOn(false);
        setOpenKey(key);
        if (tone === 'warning') userSessionStorage.setItem(SEEN_PREFIX + key, '1');
      });
    };
    if (isShown(anchor)) {
      show();
      return () => cancelAnimationFrame(frame);
    }
    const observer = new IntersectionObserver((entries) => {
      if (!entries.some((entry) => entry.isIntersecting)) return;
      observer.disconnect();
      show();
    });
    observer.observe(anchor);
    return () => {
      observer.disconnect();
      cancelAnimationFrame(frame);
    };
  }, [key, tone, blocked, anchorRef]);

  const close = (how: 'dismiss' | 'expire') => {
    setOpenKey(null);
    if (notice?.tone !== 'offer') return;
    if (how === 'dismiss') notice.onDismiss();
    else notice.onExpire();
  };

  // Its anchor hidden with its view (a cached thread left by Back or Forward),
  // the popover would float over the view in its place, so it closes.
  const anchorGone = useEffectEvent(() => close('expire'));
  useEffect(() => {
    const anchor = anchorRef.current;
    if (!open || !anchor) return;
    const observer = new IntersectionObserver(() => {
      if (!isShown(anchor)) anchorGone();
    });
    observer.observe(anchor);
    return () => observer.disconnect();
  }, [open, anchorRef]);

  // The popover is portaled to the end of the page, so Tab would never reach
  // it. It is read as coming right after its control: Tab there steps in.
  useEffect(() => {
    if (!open) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== 'Tab' || e.shiftKey || e.altKey || e.ctrlKey || e.metaKey) return;
      if (!anchorRef.current || e.target !== anchorRef.current || !contentRef.current) return;
      const first = tabbables(contentRef.current)[0];
      if (!first) return;
      e.preventDefault();
      first.focus();
    };
    document.addEventListener('keydown', onKeyDown, true);
    return () => document.removeEventListener('keydown', onKeyDown, true);
  }, [open, anchorRef]);

  // And steps out the same way: back to the control before it, or on to
  // whatever follows the control, never to the end of the page.
  const stepOut = (e: ReactKeyboardEvent<HTMLDivElement>) => {
    const anchor = anchorRef.current;
    if (e.key !== 'Tab' || e.altKey || e.ctrlKey || e.metaKey || !anchor) return;
    const stops = tabbables(e.currentTarget);
    const edge = e.shiftKey ? stops[0] : stops.at(-1);
    if (edge && document.activeElement !== edge) return;
    const to = e.shiftKey ? anchor : nextAfter(anchor, e.currentTarget) ?? anchor;
    e.preventDefault();
    to.focus();
  };

  // An offer's clock stops while the pointer or focus is on it, so its action
  // stays in reach for as long as it is being read or reached for, and starts
  // over when both leave. A host re-renders with every streamed token, so the
  // clock must not restart when the callbacks' identity does.
  const expires = open && notice?.tone === 'offer';
  const expire = useEffectEvent(() => close('expire'));
  useEffect(() => {
    if (!expires || pointerOn || focusOn) return;
    const timer = setTimeout(expire, OFFER_EXPIRES_MS);
    return () => clearTimeout(timer);
  }, [expires, pointerOn, focusOn]);

  const frame: NoticeFrame | null = shown
    ? { tone: shown.tone, text: shown.text, notNow: () => close('dismiss'), close: () => close('expire') }
    : null;

  return (
    // Shut by `blocked`, it is not let go: it shows again once that clears.
    <Popover open={open} onOpenChange={(next) => { if (!next && open) close('expire'); }}>
      <PopoverAnchor virtualRef={anchorRef} />
      {shown && frame && (
        <PopoverContent
          ref={contentRef}
          side={side}
          align={align}
          sideOffset={6}
          collisionPadding={edges
            ? { top: 16, bottom: 16, left: Math.max(16, edges.left), right: Math.max(16, edges.right) }
            : 16}
          style={edges ? { width: Math.min(POPOVER_MAX_W, edges.width) } : undefined}
          aria-label={shown.text}
          // The draft keeps focus: the notice is read beside it, not instead.
          onOpenAutoFocus={(e) => e.preventDefault()}
          // Focus goes back only from inside the popover, where its removal
          // drops it to the page; anywhere else it is where the user put it.
          // Radix would return it to the trigger, but a notice is anchored on
          // the control it is about, not opened by one, so it goes there here.
          onCloseAutoFocus={(e) => {
            e.preventDefault();
            if (document.activeElement && document.activeElement !== document.body) return;
            anchorRef.current?.focus();
          }}
          // Escape answers it only from inside it. Radix hears Escape on the
          // whole page, where it is the draft's (shutting the @ or / list),
          // so there it stays open and the key goes on to the draft. Radix
          // keeps it open only for a prevented key, so the key is marked.
          onEscapeKeyDown={(e) => {
            e.preventDefault();
            if (contentRef.current?.contains(document.activeElement)) close('dismiss');
          }}
          // Focus moving on within the composer (Tab from the draft toward the
          // control, or back onto it from the picker) keeps it open; leaving
          // the composer lets it go, so it never stays up over another surface
          // that Escape here would be taken from.
          onFocusOutside={(e) => {
            const box = anchorRef.current?.closest('[data-composer-box]');
            if (!box || (e.target instanceof Node && box.contains(e.target))) e.preventDefault();
          }}
          // Ahead of Radix's own Tab handling, which loops focus inside.
          onKeyDownCapture={stepOut}
          onPointerEnter={() => setPointerOn(true)}
          onPointerLeave={() => setPointerOn(false)}
          onFocus={() => setFocusOn(true)}
          onBlur={(e) => { if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setFocusOn(false); }}
          // A click here still bubbles through the portal to the composer,
          // which answers any click by focusing its draft.
          onClick={(e) => e.stopPropagation()}
          data-composer-notice
          // Part of the composer for a floating composer that collapses on a
          // press outside itself (a phone's market view); it is portaled out.
          data-click-outside-ignore
          className="notice-pop w-80 max-w-[calc(100vw-32px)] rounded-lg p-3"
        >
          <NoticeFrameContext value={frame}>{shown.content}</NoticeFrameContext>
        </PopoverContent>
      )}
    </Popover>
  );
}
