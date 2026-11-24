/**
 * The words for an effort level, apart from the rest of the tuning vocabulary
 * (`modelTuning.ts`) because these read only the main catalog: a surface that
 * names a level, such as a hand-off's approval card on a shared chat, imports
 * them without loading the Settings copy.
 */
export const EFFORT_LABELS: Record<string, string> = {
  none: 'chat.modelSelector.effortNone',
  minimal: 'chat.modelSelector.effortMinimal',
  low: 'chat.modelSelector.effortLow',
  medium: 'chat.modelSelector.effortMedium',
  high: 'chat.modelSelector.effortHigh',
  xhigh: 'chat.modelSelector.effortXhigh',
  max: 'chat.modelSelector.effortMax',
};

/** Weakest first — the order a ladder is drawn in, whatever subset a model honors. */
export const EFFORT_ORDER: readonly string[] = [
  'none',
  'minimal',
  'low',
  'medium',
  'high',
  'xhigh',
  'max',
];

/** The trigger pill and its measure twin must render the same string, so both
 *  resolve the label through here rather than building the key by hand. */
export function effortLabelFor(t: (key: string) => string, level: string | null): string | null {
  return level && EFFORT_LABELS[level] ? t(EFFORT_LABELS[level]) : null;
}
