import { useEffect, useEffectEvent, useLayoutEffect, useRef, useState, type RefObject } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { Check, Copy, MessageSquarePlus, MoreHorizontal, Quote } from 'lucide-react';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useToast } from '@/components/ui/use-toast';
import { copyText } from '@/lib/clipboard';
import { isImeComposing } from '@/lib/ime';
import { focusWithoutRing } from '@/lib/inputModality';
import { isQuoteSurface, surfaceProps, type QuoteAnchor, type TextAnchor } from './quoteAnchor';
import { placeBeside, useAnchoredPosition, type Place } from './quotePlacement';
import './SelectionQuote.css';

const NOTE_MAX_HEIGHT = 200;
/** More than two lines of the popover's width hold; they clamp it. */
const QUOTE_PREVIEW_CHARS = 400;
const COPIED_MS = 1500;

const canHighlight = () => typeof CSS !== 'undefined' && 'highlights' in CSS;

function useOutsidePress(onPress: () => void, enabled = true) {
  const press = useEffectEvent((event: PointerEvent) => {
    if (!isQuoteSurface(event.target)) onPress();
  });
  useEffect(() => {
    if (!enabled) return;
    document.addEventListener('pointerdown', press, true);
    return () => document.removeEventListener('pointerdown', press, true);
  }, [enabled]);
}

export function SelectionToolbar<A extends TextAnchor>({
  anchor, boundsRef, place, label, addLabel, onReply, onAdd, onLost,
}: {
  anchor: A;
  boundsRef: RefObject<HTMLElement | null>;
  place: Place<A>;
  label: string;
  addLabel: string;
  onReply: () => void;
  onAdd: () => void;
  onLost: () => void;
}) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const ref = useRef<HTMLDivElement>(null);
  const [copied, setCopied] = useState(false);
  const { position, offscreen } = useAnchoredPosition(anchor, ref, boundsRef, place, onLost);

  useEffect(() => {
    if (!copied) return;
    const timer = setTimeout(() => setCopied(false), COPIED_MS);
    return () => clearTimeout(timer);
  }, [copied]);

  return createPortal(
    <div
      ref={ref}
      {...surfaceProps}
      role="toolbar"
      aria-label={label}
      className="selection-quote-toolbar pop-in"
      style={{
        left: position?.left ?? 0,
        top: position?.top ?? 0,
        visibility: position && !offscreen ? 'visible' : 'hidden',
      }}
      // Pressing a button must not collapse the selection it acts on.
      onMouseDown={(e) => e.preventDefault()}
    >
      <button type="button" className="selection-quote-icon" title={t('context.quote.quoteAction')} aria-label={t('context.quote.quoteAction')} onClick={onReply}>
        <Quote className="h-4 w-4" />
      </button>
      <button
        type="button"
        className="selection-quote-icon"
        title={t('context.quote.copy')}
        aria-label={copied ? t('context.quote.copied') : t('context.quote.copy')}
        onClick={() => {
          copyText(anchor.text).then(
            () => setCopied(true),
            () => toast({ variant: 'destructive', title: t('context.quote.copyFailed') }),
          );
        }}
      >
        {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
      </button>
      <DropdownMenu modal={false}>
        <DropdownMenuTrigger asChild>
          <button type="button" className="selection-quote-icon" title={t('context.quote.more')} aria-label={t('context.quote.more')}>
            <MoreHorizontal className="h-4 w-4" />
          </button>
        </DropdownMenuTrigger>
        <DropdownMenuContent {...surfaceProps} align="start" onCloseAutoFocus={(e) => e.preventDefault()}>
          <DropdownMenuItem onSelect={onAdd}>{addLabel}</DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </div>,
    document.body,
  );
}

/**
 * A popover to write a plain reply to a quote. It adds to the next message rather
 * than sending, so several quotes can go out together. Portaled because both
 * surfaces clip, and fixed so it can follow the selection across them.
 */
export function QuoteComposer({ anchor, boundsRef, label, placeholder, hidden = false, onAdd, onDismiss }: {
  anchor: QuoteAnchor;
  /** The surface the popover stays inside. */
  boundsRef: RefObject<HTMLElement | null>;
  /** Its accessible name, saying where the quote is from. */
  label: string;
  placeholder: string;
  /** Out of sight for now, keeping what was written. */
  hidden?: boolean;
  onAdd: (reply?: string) => void;
  onDismiss: () => void;
}) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const pendingCaret = useRef<number | null>(null);
  const [reply, setReply] = useState('');
  // Stays put if what it quotes goes, so as not to lose what was written.
  const { position } = useAnchoredPosition(anchor, ref, boundsRef, placeBeside);
  const trimmed = reply.trim();
  const range = anchor.kind === 'selection' || anchor.kind === 'block' ? anchor.range : null;
  const addTitle = t('context.quote.addToMessage');

  // A press on the page is no reason to throw away what was written.
  useOutsidePress(() => {
    if (!trimmed) onDismiss();
  });

  // Not before it is placed: until then it is hidden, and refuses focus.
  const placed = !!position;
  useEffect(() => {
    if (placed) focusWithoutRing(textareaRef.current, { preventScroll: true });
  }, [placed]);

  // A page whose selection went with the focus still has to show what is quoted.
  useEffect(() => {
    if (!range || !canHighlight()) return;
    CSS.highlights.set('selection-quote', new Highlight(range));
    return () => { CSS.highlights.delete('selection-quote'); };
  }, [range]);

  useLayoutEffect(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    textarea.style.height = 'auto';
    // Rounded up: a line is a fraction of a pixel tall, and `scrollHeight` rounds,
    // so what fits exactly can still come up short and show a scrollbar.
    textarea.style.height = `${Math.min(textarea.scrollHeight + 1, NOTE_MAX_HEIGHT)}px`;
    if (pendingCaret.current != null) {
      textarea.setSelectionRange(pendingCaret.current, pendingCaret.current);
      pendingCaret.current = null;
    }
  }, [reply]);

  return createPortal(
    <div
      ref={ref}
      {...surfaceProps}
      role="dialog"
      aria-label={label}
      className="selection-quote-popover owns-its-edge pop-in"
      style={{
        display: hidden ? 'none' : undefined,
        left: position?.left ?? 0,
        top: position?.top ?? 0,
        visibility: position ? 'visible' : 'hidden',
      }}
      onKeyDown={(e) => {
        if (e.key === 'Escape') {
          e.stopPropagation();
          onDismiss();
        }
      }}
    >
      {/* The quote is highlighted right beside this box, so saying it again only
          makes the box bigger. Reading by ear, or with no way to highlight it,
          nothing else says what is quoted. An image has no highlight to show. */}
      {anchor.kind === 'image'
        ? <img src={anchor.image.currentSrc || anchor.image.src} alt={anchor.image.alt} className="selection-quote-thumb mb-2" />
        : <blockquote className={range && !canHighlight() ? 'selection-quote-text mb-2' : 'sr-only'}>{anchor.text.slice(0, QUOTE_PREVIEW_CHARS).replace(/\s+/g, ' ').trim()}</blockquote>}
      <div className="flex items-end gap-2">
        <textarea
          ref={textareaRef}
          value={reply}
          rows={1}
          aria-label={placeholder}
          placeholder={placeholder}
          className="font-content block min-w-0 flex-1 resize-none self-center overflow-y-auto border-0 bg-transparent text-sm leading-relaxed text-(--color-text-primary) placeholder:text-(--color-text-tertiary)"
          onChange={(e) => setReply(e.target.value)}
          onKeyDown={(e) => {
            if (isImeComposing(e) || e.key !== 'Enter') return;
            if (e.metaKey || e.ctrlKey) {
              e.preventDefault();
              const { selectionStart: start, selectionEnd: end } = e.currentTarget;
              pendingCaret.current = start + 1;
              setReply(`${reply.slice(0, start)}\n${reply.slice(end)}`);
            } else if (!e.shiftKey) {
              e.preventDefault();
              onAdd(trimmed || undefined);
            }
          }}
        />
        <button
          type="button"
          className="selection-quote-icon selection-quote-icon-sm"
          title={addTitle}
          aria-label={addTitle}
          onClick={() => onAdd(trimmed || undefined)}
        >
          <MessageSquarePlus className="h-4 w-4" />
        </button>
      </div>
    </div>,
    document.body,
  );
}

/** The floating controls for a text selection: a small toolbar over the text, then the reply popover. */
export function SelectionQuote({
  anchor, composing, hidden = false, boundsRef, label, placeholder, addLabel, onReply, onAdd, onDismiss,
}: {
  anchor: TextAnchor;
  composing: boolean;
  /** The surface can't take a quote just now. A menu goes; a reply being written waits. */
  hidden?: boolean;
  /** The surface the controls stay inside. */
  boundsRef: RefObject<HTMLElement | null>;
  /** The popover's accessible name, saying where the quote is from. */
  label: string;
  placeholder: string;
  /** The menu's way of adding the quote with no reply. */
  addLabel: string;
  onReply: () => void;
  onAdd: (reply?: string) => void;
  onDismiss: () => void;
}) {
  const { t } = useTranslation();
  useOutsidePress(onDismiss, !composing);

  const dismissMenu = useEffectEvent(onDismiss);
  const menuHidden = hidden && !composing;
  useEffect(() => {
    if (menuHidden) dismissMenu();
  }, [menuHidden]);

  const onSelectionChange = useEffectEvent(() => {
    if (window.getSelection()?.isCollapsed) onDismiss();
  });
  const followsSelection = !composing && anchor.kind === 'selection';
  useEffect(() => {
    if (!followsSelection) return;
    document.addEventListener('selectionchange', onSelectionChange);
    return () => document.removeEventListener('selectionchange', onSelectionChange);
  }, [followsSelection]);

  if (composing) {
    return <QuoteComposer anchor={anchor} boundsRef={boundsRef} label={label} placeholder={placeholder} hidden={hidden} onAdd={onAdd} onDismiss={onDismiss} />;
  }
  if (hidden) return null;
  return (
    <SelectionToolbar
      anchor={anchor}
      boundsRef={boundsRef}
      place={placeBeside}
      label={t('context.quote.toolbar')}
      addLabel={addLabel}
      onReply={onReply}
      onAdd={() => onAdd()}
      onLost={onDismiss}
    />
  );
}
