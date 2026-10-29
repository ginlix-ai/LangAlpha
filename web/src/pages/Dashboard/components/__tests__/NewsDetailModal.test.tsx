import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

// Real catalogs (test/setup.ts loads them): the window's words are localized,
// and these assertions read the English ones.
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));
vi.mock('@/components/ui/use-toast', () => ({ useToast: () => ({ toast: vi.fn() }) }));

// Mirrors the TickerTick detail shape: no image_url, no author, no sentiments —
// the sparse article that previously rendered without a headline.
const IMAGELESS_ARTICLE = {
  id: 'article-1',
  title: 'Sample Headline Without An Image',
  author: null,
  description: 'A short summary paragraph that appears under Executive Summary.',
  published_at: '2026-06-02T02:01:05+00:00',
  article_url: 'https://example.com/sample-article',
  image_url: null,
  source: { name: 'example.com', logo_url: null, homepage_url: null, favicon_url: null },
  tickers: [],
  keywords: [],
  sentiments: null,
};

// One ticker with a scored sentiment and its note.
const SENTIMENT_ARTICLE = {
  ...IMAGELESS_ARTICLE,
  id: 'article-2',
  sentiments: [{ ticker: 'AAPL', sentiment: 'positive', reasoning: 'Margins held through the quarter.' }],
};

const getNewsArticle = vi.fn();
vi.mock('../../utils/api', () => ({ getNewsArticle: (...a: unknown[]) => getNewsArticle(...a) }));

import NewsDetailModal from '../NewsDetailModal';

describe('NewsDetailModal — imageless (TickerTick) article', () => {
  beforeEach(() => {
    getNewsArticle.mockClear();
  });

  it('renders from the row with no by-id fetch when the body is inlined', async () => {
    render(
      <NewsDetailModal
        newsId="inlined-1"
        onClose={vi.fn()}
        fallback={{
          title: 'Inlined Body Story',
          source: 'example.com',
          publishedAt: '2026-06-02T00:00:00+00:00',
          tickers: ['MSFT'],
          articleUrl: 'https://example.com/inlined',
          description: 'The full summary that shipped in the list payload.',
          keywords: ['cloud'],
        }}
      />,
    );

    expect(
      await screen.findByRole('heading', { name: 'Inlined Body Story' }),
    ).toBeInTheDocument();
    expect(screen.getByText(/shipped in the list payload/i)).toBeInTheDocument();
    // The whole point of Option A: no second round-trip when the row is complete.
    expect(getNewsArticle).not.toHaveBeenCalled();
  });

  it('renders the headline even when the article has no hero image', async () => {
    getNewsArticle.mockResolvedValue(IMAGELESS_ARTICLE);

    render(<NewsDetailModal newsId="article-1" onClose={vi.fn()} />);

    // The title (regression: was only rendered inside the skipped hero block).
    const heading = await screen.findByRole('heading', {
      name: 'Sample Headline Without An Image',
    });
    expect(heading).toBeInTheDocument();
    // Description still shows under Executive Summary.
    expect(screen.getByText(/short summary paragraph/i)).toBeInTheDocument();
  });

  it('falls back to the clicked row when the by-id fetch 404s (TickerTick rotation)', async () => {
    getNewsArticle.mockRejectedValue(new Error('Request failed with status code 404'));

    render(
      <NewsDetailModal
        newsId="rotated-1"
        onClose={vi.fn()}
        fallback={{
          title: 'Rotated Ticker Story',
          source: 'example.com',
          publishedAt: '2026-06-02T00:00:00+00:00',
          tickers: ['AAPL'],
          articleUrl: 'https://example.com/rotated',
        }}
      />,
    );

    // Renders the row's known data instead of the "details not available" state.
    expect(
      await screen.findByRole('heading', { name: 'Rotated Ticker Story' }),
    ).toBeInTheDocument();
    expect(screen.getByText('AAPL')).toBeInTheDocument();
    expect(screen.getByText('Source')).toBeInTheDocument();
    expect(screen.queryByText(/Article details not available/i)).not.toBeInTheDocument();
  });

  it('shows the empty state only when there is no fallback', async () => {
    getNewsArticle.mockRejectedValue(new Error('404'));

    render(<NewsDetailModal newsId="x" onClose={vi.fn()} />);

    expect(await screen.findByText(/Article details not available/i)).toBeInTheDocument();
  });

  it('prints each ticker\'s note in full, and Escape closes the article', async () => {
    const onClose = vi.fn();
    getNewsArticle.mockResolvedValue(SENTIMENT_ARTICLE);

    render(<NewsDetailModal newsId="article-2" onClose={onClose} />);
    await screen.findByText('AAPL');
    // The impact table holds the whole note, so there is no card to open for
    // the rest of it: the note is on the page once.
    expect(screen.getAllByText(/Margins held/)).toHaveLength(1);

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('does not close on the Escape that abandons an IME composition', async () => {
    const onClose = vi.fn();
    getNewsArticle.mockResolvedValue(SENTIMENT_ARTICLE);

    render(<NewsDetailModal newsId="article-2" onClose={onClose} />);
    await screen.findByText('AAPL');

    fireEvent.keyDown(window, { key: 'Escape', isComposing: true });
    expect(onClose).not.toHaveBeenCalled();

    fireEvent.keyDown(window, { key: 'Escape' });
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
