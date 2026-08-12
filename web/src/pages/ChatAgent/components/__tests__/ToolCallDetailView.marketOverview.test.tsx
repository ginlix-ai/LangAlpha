/**
 * market_overview artifact rendering in the tool-call detail panel.
 *
 * The consolidated `get_market_overview` tool nests the legacy
 * market_indices / sector_performance artifacts under `indices` / `sectors`.
 * The detail panel must compose the same two chart components the legacy
 * cases use — and fall back to markdown content for the error-path artifact
 * (`{type, region}` with no nested data), never a dead-end empty state.
 *
 * Strategy mirrors the ActivityBlock tests: mock the chart components and
 * Markdown, render with synthesized neutral data.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}));

vi.mock('@/hooks/useIsMobile', () => ({
  useIsMobile: () => false,
}));

vi.mock('../charts/MarketDataCharts', () => ({
  StockPriceChart: () => <div data-testid="stock-price-chart" />,
  CompanyOverviewCard: () => <div data-testid="company-overview-card" />,
  MarketIndicesChart: ({ data }: { data: Record<string, unknown> }) => (
    <div data-testid="market-indices-chart">{JSON.stringify(data)}</div>
  ),
  SectorPerformanceChart: ({ data }: { data: Record<string, unknown> }) => (
    <div data-testid="sector-performance-chart">{JSON.stringify(data)}</div>
  ),
  StockScreenerTable: () => <div data-testid="stock-screener-table" />,
}));

vi.mock('../charts/SecFilingViewer', () => ({
  default: () => <div data-testid="sec-filing-viewer" />,
}));

vi.mock('../charts/AutomationDetailPanel', () => ({
  default: () => <div data-testid="automation-detail-panel" />,
}));

vi.mock('../charts/InlineArtifactCards', () => ({
  FaviconImg: () => null,
  googleFaviconUrl: () => '',
}));

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
  CodeBlock: ({ code }: { code: string }) => <pre data-testid="code-block">{code}</pre>,
}));

import ToolCallDetailView from '../ToolCallDetailView';

const indicesArtifact = {
  type: 'market_indices',
  indices: {
    IDXA: { ohlcv: [{ date: '2026-06-30', close: 1000.0 }], stats: { period_change_pct: 1.2 } },
  },
};

const sectorsArtifact = {
  type: 'sector_performance',
  sectors: [{ sector: 'Technology', changePercentage: 0.5 }],
};

function makeProcess(artifact: Record<string, unknown>, content = 'raw overview text') {
  return {
    toolName: 'get_market_overview',
    toolCall: { id: 'call-1', name: 'get_market_overview', args: { region: 'us' } },
    toolCallResult: { content, artifact },
    isComplete: true,
  };
}

describe('ToolCallDetailView — market_overview artifact', () => {
  it('renders both nested charts when indices and sectors are present', () => {
    render(
      <ToolCallDetailView
        toolCallProcess={makeProcess({
          type: 'market_overview',
          region: 'us',
          indices: indicesArtifact,
          sectors: sectorsArtifact,
        })}
      />,
    );

    expect(screen.getByTestId('market-indices-chart')).toHaveTextContent('IDXA');
    expect(screen.getByTestId('sector-performance-chart')).toHaveTextContent('Technology');
    expect(screen.queryByTestId('markdown-content')).not.toBeInTheDocument();
  });

  it('renders only the indices chart for a region without sector data', () => {
    render(
      <ToolCallDetailView
        toolCallProcess={makeProcess({
          type: 'market_overview',
          region: 'cn',
          indices: indicesArtifact,
        })}
      />,
    );

    expect(screen.getByTestId('market-indices-chart')).toBeInTheDocument();
    expect(screen.queryByTestId('sector-performance-chart')).not.toBeInTheDocument();
  });

  it('falls back to markdown content for the error-path artifact (no nested data)', () => {
    render(
      <ToolCallDetailView
        toolCallProcess={makeProcess(
          { type: 'market_overview', region: 'xx' },
          "Unknown region 'xx'.",
        )}
      />,
    );

    expect(screen.queryByTestId('market-indices-chart')).not.toBeInTheDocument();
    expect(screen.queryByTestId('sector-performance-chart')).not.toBeInTheDocument();
    expect(screen.getByTestId('markdown-content')).toHaveTextContent("Unknown region 'xx'.");
  });
});
