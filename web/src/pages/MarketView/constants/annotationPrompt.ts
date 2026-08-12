/**
 * Skill-injection context shared by MarketView's two chat surfaces (the desktop
 * `MarketChatPanel` and the mobile FAB path in `MarketView`). Tells the
 * chart-annotation skill which ticker + timeframe "the chart" is, so the agent
 * edits the instance the user is actually viewing (chart_id = SYMBOL:timeframe)
 * instead of guessing. `sym` is the uppercased ticker; `tf` is a normalized
 * timeframe.
 */
export function marketViewAnnotationContext(sym: string, tf: string): string {
  return `The user is viewing the ${sym} chart on the ${tf} timeframe in MarketView. When they ask to annotate or draw on "the chart", pass symbol="${sym}" and timeframe="${tf}" unless they name another ticker or interval.`;
}
