import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes } from 'react-router';

const getNewsArticle = vi.fn();
vi.mock('../../Dashboard/utils/api', () => ({ getNewsArticle: (...a: unknown[]) => getNewsArticle(...a) }));

import NewsDetailPage from '../NewsDetailPage';

function renderPage(sentiments: unknown, tickers: string[]) {
  getNewsArticle.mockResolvedValue({ title: 'Roundup Story', tickers, sentiments });
  return render(
    <MemoryRouter initialEntries={['/news/a-1']}>
      <Routes>
        <Route path="/news/:id" element={<NewsDetailPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('NewsDetailPage sentiment list', () => {
  it('attributes scores to the vendor AI and lists unscored tickers as Unscored', async () => {
    renderPage(
      [
        { ticker: 'AAPL', sentiment: 'negative', reasoning: 'Scored with a note.' },
        { ticker: 'MSFT', sentiment: null },
      ],
      ['AAPL', 'MSFT', 'NVDA'],
    );

    expect(await screen.findByText('AI Sentiment (news vendor estimate)')).toBeInTheDocument();
    expect(screen.getAllByText('Unscored')).toHaveLength(2);
    expect(screen.getByText('negative')).toBeInTheDocument();
  });

  it('keeps the plain heading when nothing is scored', async () => {
    renderPage([{ ticker: 'AAPL', sentiment: null }], ['AAPL']);

    expect(await screen.findByText('Sentiment Analysis')).toBeInTheDocument();
    expect(screen.queryByText(/news vendor estimate/)).not.toBeInTheDocument();
    expect(screen.getByText('Unscored')).toBeInTheDocument();
  });
});
