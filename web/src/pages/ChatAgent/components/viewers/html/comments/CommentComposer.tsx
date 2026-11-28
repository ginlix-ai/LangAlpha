import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Plus, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { CheckboxChoice } from '@/components/ui/choice';
import { InPlaceSwap, SwapCell } from '@/components/ui/crossfade';
import { Disclosure } from '@/components/ui/Disclosure';
import { usePreferences } from '@/hooks/usePreferences';
import { readCommentEnter } from '@/lib/artifactPreferences';
import { clientOs } from '@/lib/clientOs';
import { cn } from '@/lib/utils';
import { CommentField, FieldFrame } from './CommentField';
import { FieldFocus } from './fieldFocus';
import { useSubmitting } from './useSubmitting';
import { INTENT_LABEL, enterFor, footerLayout } from './composerFooter';
import { useCommentSurface } from './commentSurface';
import type { CommentIntent } from '../../../artifactComments/destinations';

/** What a new comment's composer is handed by the layer, which holds the words so they move from box to box. */
interface NewComment {
  /** "For the agent", held with the words. */
  forAgent: boolean;
  onForAgentChange: (forAgent: boolean) => void;
  /** The "To:" line, shown while the words are for the agent: where they go. */
  destination: React.ReactNode;
  /**
   * The owner asked to leave with words written: the footer gives way to
   * whether to throw them away. Leaving an empty box never asks.
   */
  confirmDiscard: boolean;
  onKeepWriting: () => void;
  onDiscard: () => void;
  /** The box's ×: an empty draft goes at once, words ask first (`confirmDiscard`). */
  onClose: () => void;
  /** False for a draft taken up again from where it was kept (`InHand`): the keyboard stays where it is. */
  takesFocus: boolean;
}

/**
 * Where the composer is. A new comment's field takes the focus, and the
 * version picker hands it back after a pick (`data-page-draft`).
 */
export type ComposerMode =
  /** A new comment in its box over the page, which names the spot over the field. */
  | ({
    kind: 'over';
    head: string;
    /** What the spot holds, under its name: the selected words, an area's parts. */
    quote: string | null;
    /** The quote whole, about four lines tall and scrolling past that, as selected words are shown; else two lines, cut. */
    fullQuote: boolean;
    /**
     * The parts the comment is on, by their labels, each with a way out of
     * the selection, for an area that lists every part. Absent for one
     * element, and for an area too large to list.
     */
    parts?: { labels: string[]; onDrop?: (index: number) => void };
    /**
     * "Add a part", on a touch screen, which has no modifier to hold: the next
     * tap adds to the comment rather than starting a new one. `on` while it waits.
     */
    addPart?: { on: boolean; onToggle: () => void };
  } & NewComment)
  /** A new comment waiting in the list, whose card names the spot. */
  | ({ kind: 'list' } & NewComment)
  /** A reply under a card, which holds its own tick. */
  | { kind: 'reply' };

interface CommentComposerProps {
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  /** The comment's thread can hold a queue: false where it would start a new thread, so it is sent or kept. */
  canQueue: boolean;
  /** Where the words go is known, so they can go to the agent; a note can always be kept. */
  canSend: boolean;
  /** Resolves once the words are kept; a rejection is shown under the box. */
  onSubmit: (route: CommentIntent) => Promise<void>;
  mode: ComposerMode;
}

/**
 * The owner's words and where they go, one compact card: the spot it is on,
 * the field, and a footer whose layout and keys are `composerFooter`'s.
 * Unticking "For the agent" keeps the words on the page as a note.
 */
export function CommentComposer({
  value, onChange, placeholder, canQueue, canSend, onSubmit, mode,
}: CommentComposerProps): React.ReactElement {
  const { t } = useTranslation();
  const rootRef = useRef<HTMLDivElement>(null);
  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const { busy, failed, run } = useSubmitting();
  const [ownTick, setOwnTick] = useState(true);
  const drafting = mode.kind === 'reply' ? null : mode;
  const over = mode.kind === 'over' ? mode : null;
  // A field under a card, or in the list's draft card, rather than the box itself.
  const embedded = mode.kind !== 'over';
  const autoFocus = drafting?.takesFocus ?? false;
  const confirmDiscard = drafting?.confirmDiscard ?? false;
  const forAgent = drafting ? drafting.forAgent : ownTick;
  const setForAgent = drafting ? drafting.onForAgentChange : setOwnTick;
  const head = over?.head;
  const empty = !value.trim();
  const enter = enterFor(useCommentSurface(), readCommentEnter(usePreferences().preferences));
  // A queue does not take the filled button's place while the pointer is on
  // the buttons. In a new chat the filled button is Send now; the first send
  // makes the thread, after which the same place says Add to queue, and a
  // second click meant to send again would queue instead. Once the pointer
  // leaves it follows the routes as they are. A queue that stops being one
  // goes at once: Send now is never a route that went away.
  const [pointedQueue, setPointedQueue] = useState<boolean | null>(null);
  // The question takes the buttons' place, and they never hear the pointer leave.
  if (confirmDiscard && pointedQueue !== null) setPointedQueue(null);
  const shownQueue = canQueue && (pointedQueue ?? true);
  const layout = footerLayout(forAgent, shownQueue, enter);
  // The filled button's two names, both held so it keeps one width as the box is ticked.
  const tickedPrimary = footerLayout(true, shownQueue, enter).primary;
  const untickedPrimary = footerLayout(false, shownQueue, enter).primary;

  // Words carried in from another box (a new spot picked, or the list a
  // version change sent the draft to) are written on from their end; a field
  // focused on mount puts the caret at the start.
  useEffect(() => {
    const field = fieldRef.current;
    if (!autoFocus || !field || document.activeElement !== field) return;
    field.setSelectionRange(field.value.length, field.value.length);
  }, [autoFocus]);

  // A part added or dropped is a press on the page or on a chip, which takes
  // the focus from the words; it comes back so typing goes on. Not on a touch
  // screen, where it would raise the keyboard over the page being picked on.
  const partsKey = over ? [over.head, ...(over.parts?.labels ?? [])].join('\n') : null;
  const partsSeen = useRef(partsKey);
  useEffect(() => {
    if (partsSeen.current === partsKey) return;
    partsSeen.current = partsKey;
    const active = document.activeElement;
    const left = !active || active === document.body || active instanceof HTMLIFrameElement;
    if (left && !window.matchMedia?.('(pointer: coarse)').matches) fieldRef.current?.focus({ preventScroll: true });
  }, [partsKey]);

  // The question going (Esc, or writing on) takes its buttons with it: the
  // focus they held, or the ×'s, comes back to the words rather than the page.
  const asked = useRef(confirmDiscard);
  useEffect(() => {
    const was = asked.current;
    asked.current = confirmDiscard;
    if (!was || confirmDiscard) return;
    const active = document.activeElement;
    if (active === document.body || (active && active !== fieldRef.current && rootRef.current?.contains(active))) {
      fieldRef.current?.focus({ preventScroll: true });
    }
  }, [confirmDiscard]);

  // Held, not hidden, while the destination is read: it comes in a moment.
  const held = (route: CommentIntent) => empty || busy || (route !== 'note' && !canSend);
  const submit = (route: CommentIntent) => {
    if (!held(route)) void run(() => onSubmit(route));
  };

  const quiet = layout.quiet;
  const buttonSize = embedded ? 'h-6 rounded-md' : 'h-7 rounded-lg';

  return (
    <FieldFrame ref={rootRef} data-comment-composer="" inline={embedded}>
      {over && (
        <div className="flex min-w-0 items-center gap-2">
          <div className="min-w-0 flex-1 truncate text-xs" style={{ color: 'var(--color-text-tertiary)' }} title={over.head}>
            {over.head}
          </div>
          {/* Its 28 px reach the box's padding, so the row stays the line's height. */}
          <button
            type="button"
            className="page-comment-icon-btn page-comment-draft-close tap-target -my-1.5 -mr-1.5 shrink-0"
            onClick={over.onClose}
            aria-label={t('pageViewer:comments.cancelDraft')}
            aria-keyshortcuts="Escape"
            title={t('pageViewer:comments.cancelDraftTitle')}
          >
            <X className="h-[15px] w-[15px]" aria-hidden />
          </button>
        </div>
      )}
      {over?.parts && <PartChips labels={over.parts.labels} onDrop={over.parts.onDrop} />}
      {over?.quote && <p className={cn('page-comment-quote text-xs', over.fullQuote ? 'page-comment-quote-full' : 'line-clamp-2')}>{over.quote}</p>}
      {over?.addPart && (
        <button
          type="button"
          className="page-comment-add-part tap-target hidden pointer-coarse:inline-flex"
          aria-pressed={over.addPart.on}
          onClick={over.addPart.onToggle}
        >
          <Plus className="h-3.5 w-3.5" aria-hidden />
          {t('pageViewer:comments.addPart')}
        </button>
      )}
      <CommentField
        ref={fieldRef}
        value={value}
        onChange={onChange}
        onEnter={(enter) => submit(enter === 'mod' ? layout.modEnter : layout.primary)}
        held={busy}
        inline={embedded}
        placeholder={placeholder}
        aria-label={head ?? placeholder}
        autoFocus={autoFocus}
        data-page-draft={autoFocus || undefined}
      />
      {drafting && (
        <Disclosure open={forAgent && !confirmDiscard}>
          <FieldFocus value={() => fieldRef.current?.focus({ preventScroll: true })}>
            <div className="pt-0.5">{drafting.destination}</div>
          </FieldFocus>
        </Disclosure>
      )}
      {failed && (
        <p role="alert" className="text-xs" style={{ color: 'var(--color-icon-danger)' }}>
          {t('pageViewer:comments.saveFailed')}
        </p>
      )}
      {/* Where the label and both buttons do not fit on one line (a reply
          in the list, a narrow panel), the buttons go together to the next
          line, right-aligned, rather than cutting the label short. */}
      <SwapCell as="div" sizeToIncoming swapKey={confirmDiscard ? 'discard' : 'routes'} layerClassName="flex min-h-7 flex-wrap items-center gap-1">
        {drafting?.confirmDiscard ? (
          <>
            {/* Held whole: past the line's room, the buttons wrap under it instead. */}
            <span role="alert" className="max-w-full shrink-0 grow truncate text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              {t('pageViewer:comments.discardConfirm')}
            </span>
            <span className="ml-auto flex shrink-0 items-center gap-1">
              <Button
                type="button"
                size="sm"
                variant="ghost"
                className={cn(buttonSize, 'px-2 text-xs')}
                onClick={() => {
                  drafting.onKeepWriting();
                  fieldRef.current?.focus();
                }}
              >
                {t('pageViewer:comments.keepWriting')}
              </Button>
              <Button type="button" size="sm" variant="destructive" className={cn(buttonSize, 'px-2.5 text-xs')} onClick={drafting.onDiscard}>
                {t('pageViewer:comments.discard')}
              </Button>
            </span>
          </>
        ) : (
          <>
            <CheckboxChoice
              checked={forAgent}
              onCheckedChange={setForAgent}
              className="mr-auto shrink-0 items-center gap-[7px] rounded-sm text-xs whitespace-nowrap select-none"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {t('pageViewer:comments.forAgent')}
            </CheckboxChoice>
            <span
              className="ml-auto flex shrink-0 items-center gap-1"
              onPointerEnter={() => setPointedQueue(canQueue)}
              onPointerLeave={() => setPointedQueue(null)}
            >
              {quiet && (
                // Fades out in place unticked, so the filled button stays put.
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className={cn(
                    buttonSize,
                    'shrink-0 px-[7px] text-xs font-normal text-(--color-text-secondary) hover:text-(--color-text-primary)',
                    !quiet.shown && 'pointer-events-none opacity-0 disabled:opacity-0',
                  )}
                  inert={!quiet.shown}
                  aria-hidden={!quiet.shown || undefined}
                  disabled={held(quiet.route)}
                  aria-keyshortcuts={quiet.route === layout.modEnter ? 'Meta+Enter Control+Enter' : undefined}
                  title={t(quiet.route === 'send' ? 'pageViewer:comments.sendNowTitle' : 'pageViewer:comments.addToQueueTitle', {
                    keys: clientOs() === 'mac' ? '⌘↵' : 'Ctrl+↵',
                  })}
                  onClick={() => submit(quiet.route)}
                >
                  {t(INTENT_LABEL[quiet.route])}
                </Button>
              )}
              <Button
                type="button"
                size="sm"
                className={cn(buttonSize, 'shrink-0 gap-1 px-2 text-xs')}
                disabled={held(layout.primary)}
                aria-keyshortcuts="Enter"
                onClick={() => submit(layout.primary)}
              >
                <InPlaceSwap
                  swapped={layout.primary !== tickedPrimary}
                  from={t(INTENT_LABEL[tickedPrimary])}
                  to={t(INTENT_LABEL[untickedPrimary])}
                  className="justify-items-center"
                />
                <span aria-hidden className="text-[11px] opacity-60 pointer-coarse:hidden">↵</span>
              </Button>
            </span>
          </>
        )}
      </SwapCell>
    </FieldFrame>
  );
}

/**
 * Labels shown without the leading segments they all share ("Revenue · "),
 * which would only repeat across the chips.
 */
function shortLabels(labels: string[]): string[] {
  const split = labels.map((l) => l.split(' · '));
  let common = 0;
  while (split.every((parts) => parts.length > common + 1 && parts[common] === split[0][common])) common += 1;
  return split.map((parts) => parts.slice(common).join(' · '));
}

/**
 * The parts a comment is on, one chip each, whose × takes that part out of
 * the selection when it can be changed (`onDrop`). The last part has none:
 * a comment is on something.
 */
function PartChips({ labels, onDrop }: { labels: string[]; onDrop?: (index: number) => void }) {
  const { t } = useTranslation();
  const shown = shortLabels(labels);
  return (
    <ul className="page-comment-parts" aria-label={t('pageViewer:comments.partsLabel')}>
      {labels.map((label, i) => (
        // A part is known by its place in the list, which is how the page drops it.
        <li key={i} className="page-comment-part" title={label}>
          <span className="min-w-0 truncate">{shown[i]}</span>
          {onDrop && labels.length > 1 && (
            <button
              type="button"
              className="page-comment-part-drop"
              onClick={() => onDrop(i)}
              aria-label={t('pageViewer:comments.dropPart', { label })}
            >
              <X className="h-3 w-3" aria-hidden />
            </button>
          )}
        </li>
      ))}
    </ul>
  );
}
