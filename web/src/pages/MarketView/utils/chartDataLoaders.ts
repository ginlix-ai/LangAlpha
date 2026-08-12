/**
 * The shared chart data-loading primitives now live in lib/bars (so lib/ never
 * imports a page). This module re-exports them for page-internal callers;
 * cross-page consumers should import from '@/lib/bars' directly.
 */
export * from '@/lib/bars/chartDataLoaders';
