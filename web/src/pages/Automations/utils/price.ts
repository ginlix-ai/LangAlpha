import type { PriceTriggerConfig } from '@/types/automation';

export function isPriceTriggerConfig(v: unknown): v is PriceTriggerConfig {
  return (
    v != null &&
    typeof v === 'object' &&
    'symbol' in v &&
    typeof (v as Record<string, unknown>).symbol === 'string' &&
    'conditions' in v &&
    Array.isArray((v as Record<string, unknown>).conditions)
  );
}

export function formatPriceTrigger(triggerConfig: PriceTriggerConfig | null | undefined): string {
  if (!triggerConfig || !isPriceTriggerConfig(triggerConfig)) return 'Price alert';
  const symbol = triggerConfig.symbol || '???';
  const condition = triggerConfig.conditions?.[0];
  if (!condition) return `${symbol} price alert`;
  const value = condition.value;
  switch (condition.type) {
    case 'price_above':
      return `${symbol} > $${Number(value).toFixed(2)}`;
    case 'price_below':
      return `${symbol} < $${Number(value).toFixed(2)}`;
    case 'pct_change_above':
      return `${symbol} \u2191${Number(value).toFixed(2)}% from ${condition.reference === 'day_open' ? 'open' : 'close'}`;
    case 'pct_change_below':
      return `${symbol} \u2193${Number(value).toFixed(2)}% from ${condition.reference === 'day_open' ? 'open' : 'close'}`;
    default:
      return `${symbol} price alert`;
  }
}

export function formatRetriggerMode(triggerConfig: PriceTriggerConfig | null | undefined): string {
  const retrigger = triggerConfig?.retrigger;
  if (!retrigger) return 'One-shot';
  switch (retrigger.mode) {
    case 'recurring': {
      if (retrigger.cooldown_seconds) {
        const hours = Math.round(retrigger.cooldown_seconds / 3600);
        return hours > 0 ? `Recurring (${hours}h)` : 'Recurring';
      }
      return 'Recurring (daily)';
    }
    case 'one_shot':
    default:
      return 'One-shot';
  }
}
