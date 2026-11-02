import React, { useEffect, useRef, useState } from 'react';
import { Check, CheckCheck, ListFilter as FilterIcon, X } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { SwapCell } from '@/components/ui/crossfade';
import { PresenceFold } from '@/components/ui/Disclosure';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { AnimatePresence, motion } from '@/lib/framer';
import { DURATION, TWEEN } from '@/lib/motion';
import { scrollMotion } from '@/lib/reducedMotion';
import type { ArtifactComment } from '@/types/api';
import { LIST_FILTERS, filterCounts, inFilter, showsDraft, type ListFilter } from '../../../artifactComments/commentModel';
import { PinBadge } from '../../../artifactComments/PinBadge';
import './CommentList.css';

/** No resize for this long: the card has landed. */
const SETTLED_MS = DURATION.quick * 1000;

// The panel comes out of the corner its button sits over, and goes back in.
const PANEL_HIDDEN = { opacity: 0, scale: 0.96, y: -6 };
const PANEL_SHOWN = { opacity: 1, scale: 1, y: 0, transition: TWEEN.quick };
const PANEL_GONE = { opacity: 0, scale: 0.98, y: -2, transition: TWEEN.exit };

const EMPTY_LABEL: Record<ListFilter, string> = {
  open: 'pageViewer.comments.emptyOpen',
  unread: 'pageViewer.comments.emptyUnread',
  resolved: 'pageViewer.comments.emptyResolved',
  all: 'pageViewer.comments.emptyAll',
};

const FILTER_LABEL: Record<ListFilter, string> = {
  open: 'pageViewer.comments.filterOpen',
  unread: 'pageViewer.comments.filterUnread',
  resolved: 'pageViewer.comments.filterResolved',
  all: 'pageViewer.comments.filterAll',
};

interface CommentListProps {
  comments: readonly ArtifactComment[];
  filter: ListFilter;
  onFilter: (filter: ListFilter) => void;
  /** The comment whose card is open in the list. */
  expandedId: string | null;
  /**
   * Comments kept in this view whatever the filter now says of them: a card
   * opened here, or one shown unread and read since. A card folding away under
   * the reader takes what they were reading, or about to click, with it.
   */
  keptIds: ReadonlySet<string>;
  /** A new comment waiting here with its words (`DraftCard`), at the top of the open ones. */
  draft: React.ReactNode;
  /** The card, open or shut; `onToggle` opens and shuts it. */
  renderCard: (comment: ArtifactComment, open: boolean, onToggle: () => void) => React.ReactNode;
  onToggle: (comment: ArtifactComment) => void;
  onMarkAllRead: () => void;
  onClose: () => void;
}

/** Brings `row` into `scroller`'s view by the least scroll, and scrolls nothing else. */
function reveal(scroller: HTMLElement, row: HTMLElement): void {
  const box = scroller.getBoundingClientRect();
  const r = row.getBoundingClientRect();
  // A card taller than the view shows its top.
  const by = r.top < box.top || r.height > box.height
    ? r.top - box.top
    : r.bottom > box.bottom ? r.bottom - box.bottom : 0;
  if (by) scroller.scrollTo({ top: scroller.scrollTop + by, behavior: scrollMotion() });
}

/**
 * Every comment on the page, the ones with a pin on screen and the ones
 * without: placed on another version, or on something this one no longer
 * shows. A card opens here and brings its pin, if any, into view. A filter
 * menu picks open, unread, resolved or all, each with its count, and marks
 * everything read; a filter other than Open is named beside the title.
 */
export function CommentList({
  comments, filter, onFilter, expandedId, keptIds, draft, renderCard, onToggle, onMarkAllRead, onClose,
}: CommentListProps): React.ReactElement {
  const { t } = useTranslation();
  const shown = comments.filter((c) => inFilter(c, filter) || keptIds.has(c.comment_id));
  const draftCard = showsDraft(filter) ? draft : null;
  const empty = shown.length === 0 && !draftCard;
  const counts = filterCounts(comments);

  // A card opened from its pin may be far down the list. It is brought into
  // view once it has landed: while it folds open it is still growing, and the
  // nearest edge read on its first frame would leave its foot below.
  const scrollerRef = useRef<HTMLDivElement>(null);
  const [expandedRow, setExpandedRow] = useState<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!expandedRow) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const observer = new ResizeObserver(() => {
      clearTimeout(timer);
      timer = setTimeout(() => {
        observer.disconnect();
        if (scrollerRef.current) reveal(scrollerRef.current, expandedRow);
      }, SETTLED_MS);
    });
    observer.observe(expandedRow);
    return () => {
      observer.disconnect();
      clearTimeout(timer);
    };
  }, [expandedRow]);

  const body = empty ? (
    <div className="grid gap-0.5 px-1.5 py-3.5 text-[12.5px]" style={{ color: 'var(--color-text-tertiary)' }}>
      <p>{t(EMPTY_LABEL[filter])}</p>
      {showsDraft(filter) && <p>{t('pageViewer.comments.emptyHint')}</p>}
    </div>
  ) : (
    // A comment that comes or goes while the list shows (written, resolved,
    // reopened) folds in or out. Each fold carries the room under it, so the
    // gap closes with the card rather than after it.
    <div role="list">
      <AnimatePresence initial={false}>
        {draftCard && (
          <PresenceFold key="draft" appear className="pb-1.5">
            {draftCard}
          </PresenceFold>
        )}
        {shown.map((c) => {
          const expanded = expandedId === c.comment_id;
          return (
            <PresenceFold key={c.comment_id} appear className="pb-1.5">
              <div role="listitem" ref={expanded ? setExpandedRow : undefined}>
                {renderCard(c, expanded, () => onToggle(c))}
              </div>
            </PresenceFold>
          );
        })}
      </AnimatePresence>
    </div>
  );

  return (
    <motion.aside
      className="page-comments-panel shadow-lg"
      aria-label={t('pageViewer.comments.listTitle')}
      initial={PANEL_HIDDEN}
      animate={PANEL_SHOWN}
      exit={PANEL_GONE}
    >
      <div className="flex items-center gap-0.5 pt-[9px] pr-2 pb-1.5 pl-3.5">
        <h3 className="mr-auto flex min-w-0 items-baseline gap-1.5 text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
          <span className="title-font truncate">{t('pageViewer.comments.listTitle')}</span>
          <SwapCell swapKey={filter === 'open' ? null : filter} className="shrink-0">
            <span className="text-xs font-normal" style={{ color: 'var(--color-text-tertiary)' }}>
              {t(FILTER_LABEL[filter])}
            </span>
          </SwapCell>
        </h3>
        <DropdownMenu modal={false}>
          <DropdownMenuTrigger asChild>
            <button
              type="button"
              className="page-comment-icon-btn shrink-0"
              aria-label={t('pageViewer.comments.filterLabel')}
              title={t('pageViewer.comments.filterLabel')}
            >
              <FilterIcon className="h-[15px] w-[15px]" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="min-w-44">
            {/* A menu of choices alone keeps its check column, so the labels
                stay put as the choice moves; the counts sit flush right. */}
            <DropdownMenuRadioGroup value={filter} onValueChange={(v) => onFilter(v as ListFilter)}>
              {LIST_FILTERS.map((f) => (
                <DropdownMenuRadioItem key={f} value={f}>
                  <Check className="h-3.5 w-3.5 shrink-0" style={{ opacity: f === filter ? 1 : 0 }} aria-hidden />
                  <span className="min-w-0 flex-1">{t(FILTER_LABEL[f])}</span>
                  <span className="font-mono text-[11px] tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
                    {counts[f]}
                  </span>
                </DropdownMenuRadioItem>
              ))}
            </DropdownMenuRadioGroup>
            <DropdownMenuSeparator />
            {/* Its icon takes the checks' column, so every label starts at one x. */}
            <DropdownMenuItem disabled={counts.unread === 0} onSelect={onMarkAllRead}>
              <CheckCheck className="h-3.5 w-3.5 shrink-0" aria-hidden />
              {t('pageViewer.comments.markAllRead')}
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
        <button
          type="button"
          className="page-comment-icon-btn shrink-0"
          onClick={onClose}
          aria-label={t('common.close')}
          title={t('common.close')}
        >
          <X className="h-[15px] w-[15px]" />
        </button>
      </div>
      <div ref={scrollerRef} className="min-h-0 flex-1 overflow-y-auto px-2 pt-0.5 pb-0.5">
        {/* The other filter's comments crossfade in, taking their own height
            at once so the scroller never holds the taller list's. */}
        <SwapCell as="div" sizeToIncoming swapKey={empty ? `${filter}:empty` : filter}>
          {body}
        </SwapCell>
      </div>
    </motion.aside>
  );
}

interface DraftCardProps {
  /** What the comment is on, as its box called it. */
  label: string;
  quote: string | null;
  /** The version it is placed on, said as a card says it, when that is not the one on screen. */
  tag: string | null;
  /** Close, or Esc: leaving asks first when there are words. */
  onLeave: () => void;
  /** The composer, holding the words written so far. */
  children: React.ReactNode;
}

/**
 * A new comment whose box was carried onto a version that shows its spot out
 * of view, or not at all. It waits here, open, rather than vanishing with the
 * words in it: the reader can send it, or scroll its spot back and pick it
 * again, and the words go with them.
 */
export function DraftCard({ label, quote, tag, onLeave, children }: DraftCardProps): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div
      role="listitem"
      className="page-comment-card"
      data-open=""
      onKeyDown={(e) => {
        if (e.key !== 'Escape' || e.nativeEvent.isComposing) return;
        e.stopPropagation();
        onLeave();
      }}
    >
      <div className="flex min-h-[26px] min-w-0 items-center gap-[7px]">
        <PinBadge number={null} state="draft" />
        <span className="flex min-w-0 flex-1 items-baseline gap-1.5">
          <span className="truncate text-xs" style={{ color: 'var(--color-text-secondary)' }} title={label}>
            {label}
          </span>
          {tag && (
            <span className="shrink-0 text-[11.5px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {tag}
            </span>
          )}
        </span>
        <button
          type="button"
          className="page-comment-icon-btn shrink-0"
          onClick={onLeave}
          aria-label={t('common.close')}
          title={t('common.close')}
        >
          <X className="h-[15px] w-[15px]" />
        </button>
      </div>
      <div className="flex flex-col gap-2 pt-1.5">
        {quote && <p className="page-comment-quote line-clamp-2 text-xs">{quote}</p>}
        {children}
      </div>
    </div>
  );
}
