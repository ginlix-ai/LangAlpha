/** The layer renderer's colors, from the theme's tokens (literals before the stylesheet stamps them). */

import { createThemeResolver } from '@/lib/themeTokens';

export interface LayerTheme {
  chipBg: string;
  chipBorder: string;
  ink: string;
  muted: string;
  /** The chart's own background: the halo that keeps a tag legible over candles. */
  halo: string;
  shadow: string;
  /** Text on a solid colored pill, as the price axis writes on its labels. */
  onFill: string;
  neutral: string;
  profit: string;
  loss: string;
  warning: string;
}

type ThemeName = 'light' | 'dark';

const LAYER_SOURCES: Record<ThemeName, LayerTheme> = {
  dark: {
    chipBg: '--color-bg-elevated',
    chipBorder: '--color-border-elevated',
    ink: '--color-text-primary',
    muted: '--color-text-secondary',
    halo: '--color-bg-card',
    shadow: 'rgba(0,0,0,0.4)',
    onFill: 'rgba(255,255,255,0.97)',
    neutral: '--color-info',
    profit: '--color-profit',
    loss: '--color-loss',
    warning: '--color-warning',
  },
  light: {
    chipBg: '--color-bg-elevated',
    chipBorder: '--color-border-elevated',
    ink: '--color-text-primary',
    muted: '--color-text-secondary',
    halo: '--color-bg-card',
    shadow: 'rgba(31,31,30,0.14)',
    onFill: 'rgba(255,255,255,0.97)',
    neutral: '--color-info',
    profit: '--color-profit',
    loss: '--color-loss',
    warning: '--color-warning',
  },
};

/** Literal mirror of LAYER_SOURCES: the jsdom / pre-stamp path. */
export const LAYER_FALLBACK: Record<ThemeName, LayerTheme> = {
  dark: {
    chipBg: '#2A2B2E',
    chipBorder: '#34363A',
    ink: '#E6E6E4',
    muted: '#9B9FA6',
    halo: '#232426',
    shadow: 'rgba(0,0,0,0.4)',
    onFill: 'rgba(255,255,255,0.97)',
    neutral: '#6E9EF7',
    profit: '#3FB950',
    loss: '#F85149',
    warning: '#D8B04C',
  },
  light: {
    chipBg: '#F7F7F6',
    chipBorder: '#E3E3E1',
    ink: '#1F1F1E',
    muted: '#73726E',
    halo: '#FFFFFF',
    shadow: 'rgba(31,31,30,0.14)',
    onFill: 'rgba(255,255,255,0.97)',
    neutral: '#4477D4',
    profit: '#1A7F37',
    loss: '#CF222E',
    warning: '#B45309',
  },
};

const resolveLayerTheme = createThemeResolver(LAYER_SOURCES, LAYER_FALLBACK);

export function getLayerTheme(theme: ThemeName): LayerTheme {
  return resolveLayerTheme(theme === 'light' ? 'light' : 'dark');
}
