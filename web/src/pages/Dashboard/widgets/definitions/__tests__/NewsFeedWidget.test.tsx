import { describe, it, expect, vi, afterEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';

// i18n passthrough — assert on raw keys / publisher strings, not translations.
vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

// Peripheral widget plumbing the test doesn't exercise.
vi.mock('@/pages/Dashboard/widgets/framework/contextSnapshot', () => ({
  useWidgetContextExport: () => {},
}));
vi.mock('@/pages/Dashboard/components/RowAttachButton', () => ({
  RowAttachButton: () => null,
}));

const openNews = vi.fn();
let ctx: Record<string, unknown>;
vi.mock('@/pages/Dashboard/widgets/framework/DashboardDataContext', () => ({
  useDashboardContext: () => ctx,
}));

import '../../index'; // populate widget registry
import { getWidget } from '../../framework/WidgetRegistry';

function makeItem(id: string, title: string, source: string) {
  return { id, title, source, time: '1h', tickers: [], favicon: null, image: null, isHot: false, articleUrl: 'https://x' };
}

function baseCtx(overrides: Record<string, unknown> = {}) {
  return {
    dashboard: {
      curatedItems: [makeItem('c1', 'Curated headline one', 'Bloomberg'), makeItem('c2', 'Curated headline two', 'Reuters')],
      curatedLoading: false,
      newsItems: [makeItem('m1', 'Market headline', 'CNBC')],
      newsLoading: false,
    },
    portfolioNews: { items: [], loading: false },
    watchlistNews: { items: [], loading: false },
    modals: { openNews },
    ...overrides,
  };
}

function renderWidget(config: { source?: string }) {
  const def = getWidget('news.feed');
  if (!def) throw new Error('news.feed not registered');
  const Component = def.component;
  return render(
    <Component instance={{ id: 'nf-1', type: 'news.feed', config }} updateConfig={vi.fn()} />,
  );
}

describe('NewsFeedWidget', () => {
  afterEach(() => {
    vi.clearAllMocks();
    ctx = baseCtx();
  });

  it('Top tab renders TickerTick curated items', () => {
    ctx = baseCtx();
    renderWidget({ source: 'top' });
    expect(screen.getByText('Curated headline one')).toBeInTheDocument();
    expect(screen.getByText('Curated headline two')).toBeInTheDocument();
  });

  it('by-source dropdown filters the active feed to a publisher', () => {
    ctx = baseCtx();
    renderWidget({ source: 'top' });

    const select = screen.getByRole('combobox') as HTMLSelectElement;
    // Facet derives from loaded items: All + Bloomberg + Reuters.
    const optionValues = within(select).getAllByRole('option').map((o) => (o as HTMLOptionElement).value);
    expect(optionValues).toEqual(['all', 'Bloomberg', 'Reuters']);

    fireEvent.change(select, { target: { value: 'Reuters' } });
    expect(screen.queryByText('Curated headline one')).not.toBeInTheDocument();
    expect(screen.getByText('Curated headline two')).toBeInTheDocument();
  });

  it('time filter buckets on the ISO publishedAt, not the "10m ago" display string', () => {
    const now = Date.now();
    const recent = {
      id: 'r1', title: 'Recent story', source: 'Bloomberg', time: '10m',
      publishedAt: new Date(now - 10 * 60 * 1000).toISOString(),
      tickers: [], favicon: null, image: null, isHot: false, articleUrl: 'https://x',
    };
    const old = {
      id: 'o1', title: 'Old story', source: 'Bloomberg', time: '3d',
      publishedAt: new Date(now - 3 * 86400 * 1000).toISOString(),
      tickers: [], favicon: null, image: null, isHot: false, articleUrl: 'https://x',
    };
    ctx = baseCtx({
      dashboard: { curatedItems: [recent, old], curatedLoading: false, newsItems: [], newsLoading: false },
    });
    renderWidget({ source: 'top' });

    // Both visible under ALL.
    expect(screen.getByText('Recent story')).toBeInTheDocument();
    expect(screen.getByText('Old story')).toBeInTheDocument();

    // Switch to 1H. The "10m" display string would not have matched the old
    // parser (it expected "10 min"); filtering on publishedAt keeps the recent
    // story and drops the 3-day-old one.
    fireEvent.click(screen.getByText('dashboard.widgets.newsFeed.range_1h'));
    expect(screen.getByText('Recent story')).toBeInTheDocument();
    expect(screen.queryByText('Old story')).not.toBeInTheDocument();
  });

  it('exposes "top" in the Zod source enum and the default config round-trips', () => {
    const def = getWidget('news.feed')!;
    expect(def.configSchema!.safeParse({ source: 'top', limit: 50 }).success).toBe(true);
    expect(def.configSchema!.safeParse(def.defaultConfig).success).toBe(true);
  });
});
