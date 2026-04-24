/**
 * Shared seed defaults the framework and presets reach for. Lives outside
 * `definitions/` so framework-tier code (presets.ts, future widget pickers)
 * can import without inverting the dependency direction (framework→widget).
 */
export const DEFAULT_BLUE_CHIPS = ['NVDA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'];
