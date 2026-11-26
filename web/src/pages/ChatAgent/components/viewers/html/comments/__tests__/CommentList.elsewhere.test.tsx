/**
 * A chart's list names the comments on its ticker's other timeframes after
 * its own, each tagged with its timeframe: where this timeframe shows none,
 * the line that says so says how many are elsewhere. A press on one takes the
 * chart there; the list's filter holds for them too.
 */
import { fireEvent, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { ArtifactComment } from '@/types/api';
import type { OtherTimeframeComment } from '../../../../artifactComments/useOtherTimeframes';
import { CommentList } from '../CommentList';
import type { ElsewhereComments } from '../OtherTimeframes';

function comment(id: string, number: number, words: string, over: Partial<ArtifactComment> = {}): ArtifactComment {
  return {
    comment_id: id,
    artifact_id: 'a-1',
    number,
    kind: 'chart',
    path: 'NVDA:1day',
    file_sha256: null,
    title: null,
    anchor: { kind: 'chart', marks: [{ type: 'point', time: '2026-10-01', price: 100 }] },
    status: 'open',
    delivery: 'note',
    thread_id: null,
    thread_title: null,
    thread_archived: false,
    created_at: '2026-10-30T00:00:00Z',
    updated_at: '2026-10-30T00:00:00Z',
    resolved_at: null,
    messages: [{ author: 'user', body: words } as ArtifactComment['messages'][number]],
    read_through: 0,
    unread: 0,
    ...over,
  };
}

const hourly: OtherTimeframeComment = { comment: comment('h1', 1, 'Breakout on volume'), path: 'NVDA:1hour', timeframe: '1H' };
const minute: OtherTimeframeComment = { comment: comment('m1', 1, 'Gap fill'), path: 'NVDA:5min', timeframe: '5m' };
const resolved: OtherTimeframeComment = {
  comment: comment('m2', 2, 'Old note', { status: 'resolved' }), path: 'NVDA:5min', timeframe: '5m',
};

function list(own: ArtifactComment[], onOpen = vi.fn()) {
  const elsewhere: ElsewhereComments = { here: '1D', comments: [minute, resolved, hourly], onOpen };
  renderWithProviders(
    <CommentList
      comments={own}
      filter="open"
      onFilter={() => {}}
      expandedId={null}
      keptIds={new Set()}
      draft={null}
      renderCard={(c) => <span>{`card ${c.comment_id}`}</span>}
      onToggle={() => {}}
      onMarkAllRead={() => {}}
      onClose={() => {}}
      elsewhere={elsewhere}
    />,
  );
  return onOpen;
}

describe('comments on other timeframes', () => {
  it('say how many are elsewhere where this timeframe shows none, each tagged with its own', () => {
    list([]);
    expect(screen.getByText('No open comments on 1D. 2 on other timeframes:')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Breakout on volume/ })).toHaveTextContent('1H');
    expect(screen.getByRole('button', { name: /Gap fill/ })).toHaveTextContent('5m');
    // The filter holds for them too: a resolved one is not open.
    expect(screen.queryByText('Old note')).toBeNull();
  });

  it('follow this timeframe\'s own under a heading of their own', () => {
    list([comment('d1', 1, 'Daily trend')]);
    expect(screen.getByText('card d1')).toBeInTheDocument();
    expect(screen.getByText('On other timeframes')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Gap fill/ })).toBeInTheDocument();
  });

  it('count in the filter menu beside this timeframe\'s own', async () => {
    list([comment('d1', 1, 'Daily trend')]);
    fireEvent.pointerDown(screen.getByRole('button', { name: 'Filter comments' }), { button: 0, ctrlKey: false });
    // Open: the daily one, Gap fill and Breakout; All adds the resolved one.
    expect(await screen.findByRole('menuitemradio', { name: /Open/ })).toHaveTextContent('3');
    expect(screen.getByRole('menuitemradio', { name: /All/ })).toHaveTextContent('4');
  });

  it('take the chart to the one pressed', () => {
    const onOpen = list([]);
    fireEvent.click(screen.getByRole('button', { name: /Breakout on volume/ }));
    expect(onOpen).toHaveBeenCalledWith(hourly);
  });
});
