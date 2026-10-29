import React, { createContext, useContext } from 'react';
import { useIsMobile } from '@/hooks/useIsMobile';

/** The transcript column is narrower than the cards' full layout: a phone, or
 *  a chat panel beside a chart. */
export const CardNarrowContext = createContext(false);

/** Lay a card out for a narrow column: the screen's width alone misses a
 *  desktop chat panel a few hundred px wide. */
export function useCardNarrow(): boolean {
  const isMobile = useIsMobile();
  return useContext(CardNarrowContext) || isMobile;
}

/**
 * Set inside the detail panel. The company charts are shared with Market
 * View's own company page, which keeps its look; here they draw in the
 * panel's frames and type, so a card and the panel it opens read as one.
 */
export const TermPanelContext = createContext(false);

export function useTermPanel(): boolean {
  return useContext(TermPanelContext);
}

/** How a recharts chart draws its furniture, by where it is drawn. */
export interface ChartSkin {
  tick: { fill: string; fontSize: number };
  grid: string;
  axis: string;
  /** Bar corners: square in the panel, as the cards' bars are. */
  bar: [number, number, number, number];
  /** The first series, the one the brand color carries elsewhere. */
  primary: string;
  /** Slices of a breakdown, largest first. */
  slices: readonly string[];
  legend: (value: string) => React.ReactNode;
}


const SKINS: Record<'term' | 'plain', ChartSkin> = {
  term: {
    tick: { fill: 'var(--color-term-text-tertiary)', fontSize: 11 },
    grid: 'var(--color-term-rule)',
    axis: 'var(--color-term-edge)',
    bar: [0, 0, 0, 0],
    primary: 'var(--color-term-text-secondary)',
    slices: ['var(--color-term-text-secondary)', 'var(--color-profit)', '#f59e0b', 'var(--color-loss)', '#3b82f6', '#ec4899', '#8b5cf6', '#14b8a6'],
    legend: (value) => <span className="term-legend">{value}</span>,
  },
  plain: {
    tick: { fill: 'var(--color-text-secondary)', fontSize: 11 },
    grid: 'var(--color-border-default)',
    axis: 'var(--color-border-default)',
    bar: [4, 4, 0, 0],
    primary: 'var(--color-accent-primary)',
    slices: ['var(--color-accent-primary)', 'var(--color-profit)', '#f59e0b', 'var(--color-loss)', '#3b82f6', '#ec4899', '#8b5cf6', '#14b8a6'],
    legend: (value) => <span style={{ color: 'var(--color-text-secondary)' }}>{value}</span>,
  },
};

export function useChartSkin(): ChartSkin {
  return SKINS[useTermPanel() ? 'term' : 'plain'];
}
