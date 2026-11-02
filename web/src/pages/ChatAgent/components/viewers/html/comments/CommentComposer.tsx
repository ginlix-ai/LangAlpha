import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { CheckboxChoice } from '@/components/ui/choice';
import { InPlaceSwap, SwapCell } from '@/components/ui/crossfade';
import { Disclosure } from '@/components/ui/Disclosure';
import { usePreferences } from '@/hooks/usePreferences';
import { readCommentEnter } from '@/lib/artifactPreferences';
import { clientOs } from '@/lib/clientOs';
import { isImeComposing } from '@/lib/ime';
import { cn } from '@/lib/utils';
import { INTENT_LABEL, footerLayout, routeForKey } from './composerFooter';
import type { CommentIntent } from '../../../artifactComments/destinations';

/** How tall the field grows with what is written before it scrolls, in px. */
const FIELD_MAX = 160;

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
  const fieldRef = useRef<HTMLTextAreaElement>(null);
  const [busy, setBusy] = useState<CommentIntent | null>(null);
  const [failed, setFailed] = useState(false);
  const [ownTick, setOwnTick] = useState(true);
  const drafting = mode.kind === 'reply' ? null : mode;
  const over = mode.kind === 'over' ? mode : null;
  // A field under a card, or in the list's draft card, rather than the box itself.
  const embedded = mode.kind !== 'over';
  const autoFocus = drafting !== null;
  const confirmDiscard = drafting?.confirmDiscard ?? false;
  const forAgent = drafting ? drafting.forAgent : ownTick;
  const setForAgent = drafting ? drafting.onForAgentChange : setOwnTick;
  const head = over?.head;
  const empty = !value.trim();
  const enter = readCommentEnter(usePreferences().preferences);
  const layout = footerLayout(forAgent, canQueue, enter);
  // The filled button's two names, both held so it keeps one width as the box is ticked.
  const tickedPrimary = footerLayout(true, canQueue, enter).primary;
  const untickedPrimary = footerLayout(false, canQueue, enter).primary;

  // Words carried in from another box (a new spot picked, or the list a
  // version change sent the draft to) are written on from their end; a field
  // focused on mount puts the caret at the start.
  useEffect(() => {
    const field = fieldRef.current;
    if (!autoFocus || !field || document.activeElement !== field) return;
    field.setSelectionRange(field.value.length, field.value.length);
  }, [autoFocus]);

  // The field grows with the words, up to a few lines, so the box never
  // hides what is being written behind a scrollbar it does not need yet.
  useLayoutEffect(() => {
    const field = fieldRef.current;
    if (!field) return;
    field.style.height = 'auto';
    field.style.height = `${Math.min(field.scrollHeight, FIELD_MAX)}px`;
  }, [value]);

  // Held, not hidden, while the destination is read: it comes in a moment.
  const held = (route: CommentIntent) => empty || busy !== null || (route !== 'note' && !canSend);
  const submit = async (route: CommentIntent) => {
    if (held(route)) return;
    setBusy(route);
    setFailed(false);
    try {
      await onSubmit(route);
    } catch {
      setFailed(true);
    } finally {
      setBusy(null);
    }
  };

  const onKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (isImeComposing(e)) return;
    const route = routeForKey(e, layout);
    if (!route) return;
    e.preventDefault();
    void submit(route);
  };

  const quiet = layout.quiet;
  const buttonSize = embedded ? 'h-6 rounded-md' : 'h-7 rounded-lg';

  return (
    <div
      className={cn(
        'flex min-w-0 flex-col',
        embedded
          ? 'gap-0.5 rounded-lg border pt-[7px] pr-2 pb-[5px] pl-2.5'
          : 'gap-1.5 px-3 pt-2.5 pb-2',
      )}
      style={embedded ? { backgroundColor: 'var(--color-bg-input)', borderColor: 'var(--color-border-input)' } : undefined}
    >
      {head && (
        <div className="truncate text-xs" style={{ color: 'var(--color-text-tertiary)' }} title={head}>
          {head}
        </div>
      )}
      {over?.quote && <p className={cn('page-comment-quote text-xs', over.fullQuote ? 'page-comment-quote-full' : 'line-clamp-2')}>{over.quote}</p>}
      <textarea
        ref={fieldRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        aria-label={head ?? placeholder}
        rows={embedded ? 1 : 2}
        maxLength={8000}
        autoFocus={autoFocus}
        data-page-draft={autoFocus || undefined}
        className={cn(
          // The box is the field's edge: the field draws no border, halo or
          // ring of its own. A ring would also light on a box opened by a
          // drag in the page, which the app never hears as a pointer.
          'owns-its-edge block w-full resize-none border-0 bg-transparent p-0 py-0.5 leading-[1.45] outline-hidden',
          'text-(--color-text-primary) placeholder:text-(--color-text-quaternary)',
          embedded ? 'min-h-[22px] text-[13px]' : 'min-h-[42px] text-sm',
        )}
      />
      {drafting && (
        <Disclosure open={forAgent && !confirmDiscard}>
          <div className="pt-0.5">{drafting.destination}</div>
        </Disclosure>
      )}
      {failed && (
        <p role="alert" className="text-xs" style={{ color: 'var(--color-icon-danger)' }}>
          {t('pageViewer.comments.saveFailed')}
        </p>
      )}
      {/* Where the label and both buttons do not fit on one line (a reply
          in the list, a narrow panel), the buttons go together to the next
          line, right-aligned, rather than cutting the label short. */}
      <SwapCell as="div" sizeToIncoming swapKey={confirmDiscard ? 'discard' : 'routes'} layerClassName="flex min-h-7 flex-wrap items-center gap-1">
        {drafting?.confirmDiscard ? (
          <>
            <span role="alert" className="min-w-0 flex-1 truncate text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              {t('pageViewer.comments.discardConfirm')}
            </span>
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
              {t('pageViewer.comments.keepWriting')}
            </Button>
            <Button type="button" size="sm" variant="destructive" className={cn(buttonSize, 'px-2.5 text-xs')} onClick={drafting.onDiscard}>
              {t('pageViewer.comments.discard')}
            </Button>
          </>
        ) : (
          <>
            <CheckboxChoice
              checked={forAgent}
              onCheckedChange={setForAgent}
              className="mr-auto shrink-0 items-center gap-[7px] rounded-sm text-xs whitespace-nowrap select-none"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {t('pageViewer.comments.forAgent')}
            </CheckboxChoice>
            <span className="ml-auto flex shrink-0 items-center gap-1">
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
                  title={t(quiet.route === 'send' ? 'pageViewer.comments.sendNowTitle' : 'pageViewer.comments.addToQueueTitle', {
                    keys: clientOs() === 'mac' ? '⌘↵' : 'Ctrl+↵',
                  })}
                  onClick={() => void submit(quiet.route)}
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
                onClick={() => void submit(layout.primary)}
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
    </div>
  );
}
