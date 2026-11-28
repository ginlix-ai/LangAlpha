import { screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { renderWithProviders } from '@/test/utils';
import type { DriveMemoMark } from '@/types/drive';
import { MemoStatus } from '../components/MemoBadges';

const final4: DriveMemoMark = { marked: true, status: 'final', tickers: [], final_seq: 4, final_at: '2026-11-03T00:00:00Z' };

describe('a final memo revised since, as a list row says it', () => {
  it('says it in a word, with the sentence for a pointer and a screen reader, and no dot', () => {
    const { container } = renderWithProviders(<MemoStatus mark={final4} head={6} />);
    const revised = container.querySelector('.drive-memo-revised')!;
    expect(container.textContent).toBe('Final v4· revisedrevised since');
    expect(revised.getAttribute('title')).toBe('Revised since Final v4');
    expect(screen.getByText('revised since')).toHaveClass('sr-only');
    // The dot beside a name in a list is a comment's unread answer.
    expect(container.querySelector('.drive-memo-dot')).toBeNull();
  });

  it('keeps the dot and the words in the memo\'s own header', () => {
    const { container } = renderWithProviders(<MemoStatus mark={final4} head={6} full />);
    expect(container.querySelector('.drive-memo-dot')).not.toBeNull();
    expect(screen.getByText('revised since')).not.toHaveClass('sr-only');
  });

  it('says nothing more while the final is the head', () => {
    const { container } = renderWithProviders(<MemoStatus mark={final4} head={4} />);
    expect(container.textContent).toBe('Final v4');
  });
});
