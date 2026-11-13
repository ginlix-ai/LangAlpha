/** What the panel hands every section, kept apart from the components so they hot-reload. */
import type { DesktopSettings } from '@/lib/desktop';
import type { MarketStatusData } from '@/lib/marketUtils';

export interface SectionProps {
  active: boolean;
  settings: Pick<DesktopSettings, 'menubar' | 'panel'>;
  marketStatus: MarketStatusData | null;
}

export const marketPath = (symbol: string) => `/market?symbol=${encodeURIComponent(symbol)}`;
