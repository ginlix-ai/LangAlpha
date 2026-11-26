import React from 'react';
import { useTranslation } from 'react-i18next';
import { firstWords, oneLine, pinState, type ListFilter } from '../../../artifactComments/commentModel';
import { PinBadge } from '../../../artifactComments/PinBadge';
import type { OtherTimeframeComment } from '../../../artifactComments/useOtherTimeframes';
import './CommentList.css';

/** That the list has none, said of the timeframe on screen when other timeframes hold some. */
const EMPTY_HERE_LABEL: Record<ListFilter, string> = {
  open: 'pageViewer:comments.emptyOpenOn',
  unread: 'pageViewer:comments.emptyUnreadOn',
  resolved: 'pageViewer:comments.emptyResolvedOn',
  all: 'pageViewer:comments.emptyAllOn',
};

/** How much of a comment a row for another timeframe shows. */
const ROW_CHARS = 120;

/** The comments a chart keeps on its ticker's other timeframes, which the list names beside its own. */
export interface ElsewhereComments {
  /** The timeframe on screen, as the chart's toolbar says it. */
  here: string;
  comments: readonly OtherTimeframeComment[];
  /** Switches the chart to the comment's timeframe and opens its card there. */
  onOpen: (comment: OtherTimeframeComment) => void;
}

/**
 * The other timeframes' comments the list's filter shows (`shown`), after
 * this one's: under a heading of their own, or, where this one shows none
 * (`noneHere`), under the line that says so.
 */
export function OtherTimeframes({ elsewhere, shown, filter, noneHere }: {
  elsewhere: ElsewhereComments;
  shown: readonly OtherTimeframeComment[];
  filter: ListFilter;
  noneHere: boolean;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div role="group" aria-label={t('pageViewer:comments.otherTimeframes')}>
      <p className="px-1.5 pt-1.5 pb-2 text-[12.5px]" style={{ color: 'var(--color-text-tertiary)' }}>
        {noneHere
          ? t('pageViewer:comments.emptyHereElsewhere', {
            none: t(EMPTY_HERE_LABEL[filter], { timeframe: elsewhere.here }),
            elsewhere: t('pageViewer:comments.otherTimeframesCount', { count: shown.length }),
          })
          : t('pageViewer:comments.otherTimeframes')}
      </p>
      <div role="list">
        {shown.map((o) => (
          <div key={o.comment.comment_id} role="listitem" className="pb-1.5">
            <OtherTimeframeRow other={o} onOpen={() => elsewhere.onOpen(o)} />
          </div>
        ))}
      </div>
    </div>
  );
}

/**
 * A comment on another timeframe of the chart: its pin, its first words and
 * its timeframe. Pressing it takes the chart there and opens its card, since
 * its pin is drawn only on its own timeframe.
 */
function OtherTimeframeRow({ other, onOpen }: { other: OtherTimeframeComment; onOpen: () => void }): React.ReactElement {
  const { t } = useTranslation();
  const { comment, timeframe } = other;
  const unread = comment.unread > 0;
  return (
    <div className="page-comment-card" data-resolved={comment.status === 'resolved' || undefined}>
      <button
        type="button"
        className="page-comment-card-toggle"
        onClick={(event) => {
          // The row fades out as its chart takes over, and a pointer's
          // focus left on it would sit inside a hidden layer. A key press
          // keeps its focus, so a keyboard user is not dropped to the page.
          if (event.detail > 0) event.currentTarget.blur();
          onOpen();
        }}
        title={t('pageViewer:comments.showOnTimeframe', { timeframe })}
      >
        <PinBadge number={comment.number} state={pinState(comment)} unread={unread} />
        <span className="sr-only">{t('pageViewer:comments.pinLabel', { number: comment.number })}</span>
        {unread && <span className="sr-only">{t('pageViewer:comments.unreadLabel')}</span>}
        <span className="min-w-0 flex-1 truncate text-[13px]" style={{ color: 'var(--color-text-primary)' }}>
          {oneLine(firstWords(comment), ROW_CHARS)}
        </span>
        <span className="page-comment-timeframe shrink-0">{timeframe}</span>
      </button>
    </div>
  );
}
