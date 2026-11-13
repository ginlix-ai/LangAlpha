import { useTranslation } from 'react-i18next';
import { Sparkline } from '@/components/charts/Sparkline';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { openInMain } from '@/lib/desktop';
import { DASH, grouped2 } from '@/lib/format';
import { indexName } from '@/lib/indices';
import { marketPhase, type MarketPhase } from '@/lib/marketUtils';
import { utcMsToETTime } from '@/lib/utils';
import { changeDirection } from '@/pages/ChatAgent/components/charts/termChange';
import { minutesLeftInPhase } from './glanceSummary';
import { ExtendedLine, Move, Pending } from './primitives';
import { marketPath, type SectionProps } from './panelContext';
import { useTilesGlance, type GlanceTile } from './useGlanceData';

const PHASE_KEY: Record<MarketPhase, string> = {
  open: 'menubar.market.open',
  pre: 'menubar.market.pre',
  after: 'menubar.market.after',
  closed: 'menubar.market.closed',
  unknown: 'menubar.market.unknown',
};

function useDuration() {
  const { t } = useTranslation();
  return (minutes: number) => {
    const h = Math.floor(minutes / 60);
    const m = minutes % 60;
    return h > 0 ? t('menubar.duration.hoursMinutes', { h, m }) : t('menubar.duration.minutes', { m });
  };
}

function MarketStatusLine({ phase, active }: { phase: MarketPhase; active: boolean }) {
  const { t } = useTranslation();
  const duration = useDuration();
  const now = useNow(60_000, active);
  const left = minutesLeftInPhase(phase, utcMsToETTime(now));
  const countdown =
    left == null
      ? null
      : phase === 'open'
        ? t('menubar.market.closesIn', { time: duration(left) })
        : phase === 'pre'
          ? t('menubar.market.opensIn', { time: duration(left) })
          : t('menubar.market.endsIn', { time: duration(left) });
  return (
    <div className="mb-status">
      <span className="mb-status-dot" data-phase={phase} aria-hidden="true" />
      <span className="mb-status-label">{t(PHASE_KEY[phase])}</span>
      {countdown && <span className="mb-status-left">{countdown}</span>}
    </div>
  );
}

function Tile({ tile }: { tile: GlanceTile }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const symbol = tile.ref.symbol.toUpperCase();
  const isIndex = tile.ref.kind === 'index';
  const dir = tile.quoteAvailable ? changeDirection(tile.pct) : 'flat';
  const tone = dir === 'up' ? 'var(--color-profit)' : dir === 'down' ? 'var(--color-loss)' : 'var(--color-text-tertiary)';
  const name = (isIndex && indexName(symbol, 'long', t)) || symbol;
  return (
    <button
      type="button"
      className="mb-tile"
      onClick={() => openInMain(marketPath(isIndex ? `^${symbol}` : symbol))}
      title={name}
      aria-busy={tile.pending || undefined}
    >
      <span className="mb-tile-name">{name}</span>
      {tile.pending ? (
        <Pending className="mb-tile-price" />
      ) : (
        <span className="mb-tile-price mb-num">{tile.price != null ? grouped2(tile.price, locale) : DASH}</span>
      )}
      <Move value={tile.pct} pending={tile.pending} className="mb-tile-move" />
      {tile.ext && <ExtendedLine ext={tile.ext} className="mb-tile-ext" />}
      <span className="mb-tile-spark">
        {tile.line.length > 1 && <Sparkline values={tile.line} color={tone} height={16} fill={false} strokeWidth={1.25} />}
      </span>
    </button>
  );
}

export function MarketSection({ active, settings, marketStatus }: SectionProps) {
  const { t } = useTranslation();
  const tiles = useTilesGlance(settings.panel.tiles, active, marketStatus);
  return (
    <section className="mb-section mb-market" aria-label={t('menubar.market.title')}>
      <MarketStatusLine phase={marketPhase(marketStatus)} active={active} />
      {tiles.length > 0 && (
        <div className="mb-tiles">
          {tiles.map((tile) => (
            <Tile key={`${tile.ref.kind}:${tile.ref.symbol}`} tile={tile} />
          ))}
        </div>
      )}
    </section>
  );
}
