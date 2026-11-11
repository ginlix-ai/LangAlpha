import type { i18n as I18n } from 'i18next';
import type { KitStrings, Theme } from './protocol';

/** The tokens an app is handed, so a kit component looks like the page around it. */
const THEME_VARS = [
  '--color-bg-page', '--color-bg-card', '--color-bg-surface', '--color-bg-elevated', '--color-bg-hover',
  '--color-border-muted', '--color-border-default',
  '--color-text-primary', '--color-text-secondary', '--color-text-tertiary', '--color-text-muted',
  '--color-accent-primary', '--color-up', '--color-down', '--color-up-soft', '--color-down-soft',
  '--color-success', '--color-warning', '--color-loss',
  '--font-ui', '--font-content', '--font-mono',
];

// Some tokens are bare HSL channels (`128 49% 49%`) that the app's own CSS
// wraps in `hsl()`; an app uses each token as a whole color, so it gets one.
const HSL_CHANNELS = /^[\d.]+(deg)?\s+[\d.]+%\s+[\d.]+%(\s*\/\s*[\d.]+%?)?$/;

/** The page's look as it stands, for the frame to match. */
export function readTheme(mode: 'light' | 'dark'): Theme {
  const computed = getComputedStyle(document.documentElement);
  const vars: Record<string, string> = {};
  for (const name of THEME_VARS) {
    const value = computed.getPropertyValue(name).trim();
    if (value) vars[name] = HSL_CHANNELS.test(value) ? `hsl(${value})` : value;
  }
  return { mode, vars };
}

function flatten(node: unknown, prefix: string, out: KitStrings): KitStrings {
  if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node)) {
      if (typeof value === 'string') out[prefix + key] = value;
      else flatten(value, `${prefix}${key}.`, out);
    }
  }
  return out;
}

/** The kit's own words in a language: the `kit` block of the catalog as dotted keys. */
export function kitStrings(i18n: I18n, locale: string): KitStrings {
  return flatten(i18n.getResource(locale, 'translation', 'kit'), '', {});
}
