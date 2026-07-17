/**
 * Shared surface for the inline artifact cards (InlineArtifactCards.tsx and its
 * split-out per-card files). Holds only what two or more cards use: theme
 * tokens, sizing constants, base card styles, the small numeric formatters, the
 * market-status label/colour maps, and the market-overview unwrap contract.
 */
import type React from 'react';
import type { TFunction } from 'i18next';

// ─── Theme tokens ───────────────────────────────────────────────────

export const GREEN = 'var(--color-profit)';
export const RED = 'var(--color-loss)';
export const TEXT_COLOR = 'var(--color-text-tertiary)';
export const CARD_BG = 'var(--color-bg-tool-card)';
export const CARD_BORDER = 'var(--color-border-muted)';

// ─── Sizing tokens ──────────────────────────────────────────────────

/** Shared mobile / desktop sizing tokens for inline cards */
export const SIZES_MOBILE = {
  gap: 6, listGap: 2, gridGap: '1px 12px',
  headerFs: 12, rowFs: 11, labelFs: 10, badgeFs: 9,
  rowPad: '2px 0', sectionMb: 4, filingMb: 6,
  moreMt: 2, changeMinW: 48,
} as const;
export const SIZES_DESKTOP = {
  gap: 8, listGap: 4, gridGap: '2px 20px',
  headerFs: 13, rowFs: 12, labelFs: 11, badgeFs: 10,
  rowPad: '3px 0', sectionMb: 6, filingMb: 8,
  moreMt: 4, changeMinW: 55,
} as const;

// ─── Base card styles ───────────────────────────────────────────────

export const cardStyle: React.CSSProperties = {
  background: CARD_BG,
  border: `1px solid ${CARD_BORDER}`,
  borderRadius: 8,
  padding: '12px 14px',
  cursor: 'pointer',
  transition: 'border-color 0.15s',
  outline: 'none',
  WebkitTapHighlightColor: 'transparent',
  userSelect: 'none',
};

export const mobileCardStyle: React.CSSProperties = {
  ...cardStyle,
  padding: '8px 10px',
  borderRadius: 6,
};

// ─── Numeric formatters ─────────────────────────────────────────────
//
// Kept on `toFixed` (not lib/format) deliberately: `formatPct` must never
// group thousands and `formatCompactNumber` uses fixed English B/M/K suffixes
// with a forced single decimal — an Intl compact formatter would diverge on
// both (grouping, trailing-zero suppression, locale-specific suffixes).

export function formatCompactNumber(num: number | null | undefined): string {
  if (num == null) return 'N/A';
  if (Math.abs(num) >= 1e9) return `${(num / 1e9).toFixed(1)}B`;
  if (Math.abs(num) >= 1e6) return `${(num / 1e6).toFixed(1)}M`;
  if (Math.abs(num) >= 1e3) return `${(num / 1e3).toFixed(1)}K`;
  return typeof num === 'number' ? num.toFixed(2) : String(num);
}

export function formatPct(val: number | null | undefined): string {
  if (val == null) return 'N/A';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(2)}%`;
}

// ─── Market-status labels ───────────────────────────────────────────

export const MARKET_STATUS_LABELS: Record<string, string> = {
  early_trading: 'Pre-Market',
  open: 'Regular',
  late_trading: 'After-Hours',
  closed: 'Closed',
};
export const MARKET_STATUS_COLORS: Record<string, string> = {
  early_trading: '#f59e0b',
  open: GREEN,
  late_trading: '#3b82f6',
  closed: TEXT_COLOR,
};

/** Localized status badge; MARKET_STATUS_LABELS is the fallback for statuses
 * without a `toolArtifact.marketStatus.*` key (and unknown ones stay raw). */
export function marketStatusLabel(t: TFunction, status: string | undefined): string {
  if (!status) return '';
  return t(`toolArtifact.marketStatus.${status}`, MARKET_STATUS_LABELS[status] || status);
}

/**
 * Abbreviated extended-hours session label. Only `early_trading` /
 * `late_trading` carry an extended move, so those are the only statuses this
 * resolves — anything else falls to the after-hours copy. `long` drives the
 * hero and company-overview line ("Pre-Mkt" / "After-Hrs"); `short` drives the
 * compact multi-symbol rows ("Pre" / "AH"). MARKET_STATUS_LABELS still holds
 * the full ("Pre-Market" / "After-Hours") badge form.
 */
export function extendedHoursLabel(
  t: TFunction,
  status: string | undefined,
  variant: 'long' | 'short',
): string {
  const session = status === 'early_trading' ? 'preMarket' : 'afterHours';
  return t(`toolArtifact.extendedHours.${session}.${variant}`);
}

// ─── Shared props ───────────────────────────────────────────────────

export interface InlineCardProps {
  artifact: Record<string, unknown> | null | undefined;
  onClick?: () => void;
}

// ─── market_overview unwrap ─────────────────────────────────────────

export interface MarketOverviewUnwrap {
  indicesArtifact: Record<string, unknown> | undefined;
  sectorsArtifact: Record<string, unknown> | undefined;
  hasIndices: boolean;
  hasSectors: boolean;
}

/**
 * Unwrap the composite `market_overview` artifact into its nested legacy
 * market_indices / sector_performance artifacts (carried verbatim under
 * `indices` / `sectors`) plus whether each actually holds renderable data.
 * Single source for both the inline card and the tool-call detail panel, which
 * otherwise re-derive this contract independently.
 */
export function unwrapMarketOverview(
  artifact: Record<string, unknown> | null | undefined,
): MarketOverviewUnwrap {
  const overview = (artifact || {}) as Record<string, unknown>;
  const indicesArtifact = overview.indices as Record<string, unknown> | undefined;
  const sectorsArtifact = overview.sectors as Record<string, unknown> | undefined;

  const indicesData = indicesArtifact?.indices as Record<string, unknown> | undefined;
  const hasIndices = !!indicesData && Object.keys(indicesData).length > 0;
  const sectorsData = sectorsArtifact?.sectors as unknown[] | undefined;
  const hasSectors = Array.isArray(sectorsData) && sectorsData.length > 0;

  return { indicesArtifact, sectorsArtifact, hasIndices, hasSectors };
}
