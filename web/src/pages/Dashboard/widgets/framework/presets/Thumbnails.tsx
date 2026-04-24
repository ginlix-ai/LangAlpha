import { useMemo } from 'react';
import { getPreset, type PresetId } from '../../presets';
import type { RGLItem, WidgetInstance } from '../../types';

/* Colors lifted from the HTML mockup's SVG sprites */
const C = {
  paper: '#FDFBF6',
  paperHi: '#FFFBF0',
  bg: '#F3EEE8',
  line: '#E6DFD2',
  gain: '#7A9A67',
  gainStrong: '#0E7A4B',
  loss: '#B3362F',
  ink: '#17150F',
  inkSoft: '#47423A',
  inkMute: '#8A8377',
  accent: '#0F3D2E',
  accentSoft: '#DBE8DF',
  highlight: '#F7D76A',
  tape: '#EEE8DC',
  watchBg: '#F4EEE0',
  luxGold: '#C9A97B',
};

interface CellVisuals {
  bg: string;
  stroke?: string;
}

const TYPE_VISUALS: Record<string, CellVisuals> = {
  'markets.overview': { bg: C.tape },
  'markets.miniChartGrid': { bg: C.paper, stroke: C.line },
  'chart.symbol': { bg: C.paper, stroke: C.line },
  'insight.brief': { bg: C.paperHi, stroke: C.line },
  'news.feed': { bg: C.tape },
  'calendar.earnings': { bg: C.paper, stroke: C.line },
  'portfolio.holdings': { bg: C.paper, stroke: C.line },
  'watchlist.list': { bg: C.watchBg, stroke: C.line },
  'personal.portfolioWatchlist': { bg: C.watchBg, stroke: C.line },
  'agent.conversation': { bg: C.highlight, stroke: C.line },
  'workspace.picker': { bg: C.accentSoft },
  'threads.recent': { bg: C.paper, stroke: C.line },
  'automations.list': { bg: C.paper, stroke: C.line },
  // TradingView embeds — same palette as their native siblings so thumbnails
  // read as variations on the same canvas. Glyphs fall through to the default
  // empty state which is fine; the cell bg + size still communicate layout.
  'tv.ticker-tape': { bg: C.tape },
  'tv.stock-heatmap': { bg: C.accentSoft, stroke: C.line },
  'tv.crypto-heatmap': { bg: C.accentSoft, stroke: C.line },
  'tv.forex-heatmap': { bg: C.accentSoft, stroke: C.line },
  'tv.economic-events': { bg: C.paper, stroke: C.line },
  'tv.technicals': { bg: C.paperHi, stroke: C.line },
  'tv.movers': { bg: C.paperHi, stroke: C.line },
  'tv.symbol-spotlight': { bg: C.paper, stroke: C.line },
  'tv.company-profile': { bg: C.paper, stroke: C.line },
  'tv.company-financials': { bg: C.paper, stroke: C.line },
};

interface GlyphProps {
  type: string;
  config: unknown;
  x: number;
  y: number;
  w: number;
  h: number;
}

/**
 * Draws a tiny signature motif inside a widget cell. Glyphs are intentionally
 * schematic — enough to communicate "this is a chart / news feed / list" at a
 * glance, not to reproduce the real widget's pixel detail.
 */
function WidgetGlyph({ type, config, x, y, w, h }: GlyphProps) {
  const pad = Math.min(5, w * 0.08, h * 0.14);
  const cx = x + pad;
  const cy = y + pad;
  const cw = w - pad * 2;
  const ch = h - pad * 2;
  if (cw < 12 || ch < 8) return null;

  switch (type) {
    case 'markets.overview': {
      const count = Math.max(2, Math.min(6, Math.floor(cw / 42)));
      const barW = (cw - (count - 1) * 4) / count;
      const barH = Math.min(ch - 2, 8);
      return (
        <>
          {Array.from({ length: count }).map((_, i) => {
            const up = i < Math.ceil(count * 0.6);
            return (
              <rect
                key={i}
                x={cx + i * (barW + 4)}
                y={cy + (ch - barH) / 2}
                width={barW}
                height={barH}
                rx={1}
                fill={up ? C.gain : C.loss}
                opacity={up ? 0.55 : 0.35}
              />
            );
          })}
        </>
      );
    }
    case 'chart.symbol': {
      const chartType =
        (config as { chartType?: string } | undefined)?.chartType ?? 'candle';
      if (chartType === 'candle') {
        const n = Math.max(6, Math.min(16, Math.floor(cw / 6)));
        const gap = cw / n;
        return (
          <>
            {Array.from({ length: n }).map((_, i) => {
              const up = (i * 7 + 3) % 10 > 4;
              const body = 3 + ((i * 13) % 5);
              const wick = body + 2;
              const yCenter =
                cy + ch * 0.5 - Math.sin(i * 0.4 + 1) * ch * 0.22;
              const xMid = cx + i * gap + gap / 2;
              const color = up ? C.gainStrong : C.loss;
              return (
                <g key={i}>
                  <line
                    x1={xMid}
                    y1={yCenter - wick}
                    x2={xMid}
                    y2={yCenter + wick}
                    stroke={color}
                    strokeWidth={0.6}
                  />
                  <rect
                    x={xMid - 1.5}
                    y={yCenter - body}
                    width={3}
                    height={body * 2}
                    fill={color}
                  />
                </g>
              );
            })}
          </>
        );
      }
      const steps = 14;
      const points: string[] = [];
      for (let i = 0; i <= steps; i++) {
        const px = cx + (i / steps) * cw;
        const py =
          cy +
          ch * 0.75 -
          (i / steps) * ch * 0.45 -
          Math.sin(i * 0.7) * ch * 0.08;
        points.push(`${px.toFixed(1)},${py.toFixed(1)}`);
      }
      const line = `M ${points.join(' L ')}`;
      const fill = `${line} L ${cx + cw},${cy + ch} L ${cx},${cy + ch} Z`;
      return (
        <>
          {chartType === 'area' && (
            <path d={fill} fill={C.gainStrong} opacity={0.12} />
          )}
          <path d={line} stroke={C.gainStrong} strokeWidth={1} fill="none" />
        </>
      );
    }
    case 'insight.brief': {
      const hasBody = ch > 38;
      const hasCTA = ch > 60;
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(30, cw * 0.32)}
            height={4}
            rx={1}
            fill={C.accent}
            opacity={0.85}
          />
          <rect
            x={cx}
            y={cy + 10}
            width={cw * 0.88}
            height={5}
            fill={C.ink}
          />
          <rect
            x={cx}
            y={cy + 19}
            width={cw * 0.72}
            height={5}
            fill={C.ink}
          />
          {hasBody && (
            <>
              <rect
                x={cx}
                y={cy + 30}
                width={cw * 0.85}
                height={2.5}
                fill={C.inkSoft}
                opacity={0.5}
              />
              <rect
                x={cx}
                y={cy + 35}
                width={cw * 0.7}
                height={2.5}
                fill={C.inkSoft}
                opacity={0.5}
              />
              <rect
                x={cx}
                y={cy + 40}
                width={cw * 0.6}
                height={2.5}
                fill={C.inkSoft}
                opacity={0.5}
              />
            </>
          )}
          {hasCTA && (
            <rect
              x={cx}
              y={cy + ch - 10}
              width={Math.min(34, cw * 0.32)}
              height={9}
              rx={2}
              fill={C.ink}
            />
          )}
        </>
      );
    }
    case 'news.feed': {
      const rowH = 8;
      const headerH = 8;
      const rows = Math.max(1, Math.min(8, Math.floor((ch - headerH) / rowH)));
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(34, cw * 0.35)}
            height={3}
            fill={C.inkSoft}
          />
          {Array.from({ length: rows }).map((_, i) => (
            <g key={i}>
              <rect
                x={cx}
                y={cy + headerH + i * rowH}
                width={cw - 2}
                height={2.5}
                fill={C.ink}
                opacity={0.55}
              />
              <rect
                x={cx}
                y={cy + headerH + 4 + i * rowH}
                width={cw * (0.72 - (i % 3) * 0.08)}
                height={2}
                fill={C.ink}
                opacity={0.32}
              />
            </g>
          ))}
        </>
      );
    }
    case 'calendar.earnings': {
      const cols = Math.max(1, Math.min(4, Math.floor(cw / 22)));
      const rows = Math.max(1, Math.min(3, Math.floor((ch - 8) / 16)));
      const tileW = (cw - (cols - 1) * 2) / cols;
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(36, cw * 0.5)}
            height={3}
            fill={C.inkSoft}
          />
          {Array.from({ length: cols * rows }).map((_, i) => {
            const c = i % cols;
            const r = Math.floor(i / cols);
            const up = (c + r) % 3 !== 0;
            return (
              <g key={i}>
                <rect
                  x={cx + c * (tileW + 2)}
                  y={cy + 8 + r * 16}
                  width={tileW}
                  height={2.5}
                  fill={C.ink}
                  opacity={0.7}
                />
                <rect
                  x={cx + c * (tileW + 2)}
                  y={cy + 12 + r * 16}
                  width={tileW * 0.6}
                  height={3}
                  fill={up ? C.gainStrong : C.loss}
                  opacity={0.3}
                />
              </g>
            );
          })}
        </>
      );
    }
    case 'portfolio.holdings': {
      const steps = 12;
      const points: string[] = [];
      for (let i = 0; i <= steps; i++) {
        const px = cx + (i / steps) * cw;
        const py =
          cy +
          ch * 0.65 -
          (i / steps) * ch * 0.45 -
          Math.sin(i * 0.6) * ch * 0.04;
        points.push(`${px.toFixed(1)},${py.toFixed(1)}`);
      }
      const line = `M ${points.join(' L ')}`;
      const fill = `${line} L ${cx + cw},${cy + ch - 8} L ${cx},${cy + ch - 8} Z`;
      const allocs = [0.4, 0.25, 0.15, 0.12, 0.08];
      const allocFills = [C.accent, '#1F6B48', C.gain, C.luxGold, '#E8D7BA'];
      let offset = 0;
      return (
        <>
          <path d={fill} fill={C.accent} opacity={0.1} />
          <path d={line} stroke={C.accent} strokeWidth={1} fill="none" />
          {allocs.map((frac, i) => {
            const ax = cx + offset * cw;
            const aw = frac * cw;
            offset += frac;
            return (
              <rect
                key={i}
                x={ax}
                y={cy + ch - 4}
                width={aw}
                height={4}
                fill={allocFills[i]}
              />
            );
          })}
        </>
      );
    }
    case 'watchlist.list': {
      const rowH = 7;
      const headerH = 8;
      const n = Math.max(1, Math.min(9, Math.floor((ch - headerH) / rowH)));
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(30, cw * 0.42)}
            height={3}
            fill={C.ink}
          />
          {Array.from({ length: n }).map((_, i) => {
            const up = i % 2 === 0;
            return (
              <g key={i}>
                <rect
                  x={cx}
                  y={cy + headerH + i * rowH}
                  width={Math.min(16, cw * 0.28)}
                  height={2.5}
                  fill={C.ink}
                  opacity={0.7}
                />
                <rect
                  x={cx + cw - Math.min(24, cw * 0.38)}
                  y={cy + headerH + i * rowH}
                  width={Math.min(24, cw * 0.38)}
                  height={2.5}
                  fill={up ? C.gainStrong : C.loss}
                  opacity={0.3}
                />
              </g>
            );
          })}
        </>
      );
    }
    case 'personal.portfolioWatchlist': {
      const rowH = 7;
      const headerH = 12;
      const n = Math.max(1, Math.min(9, Math.floor((ch - headerH) / rowH)));
      const tabW = Math.min(28, cw * 0.38);
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={tabW}
            height={5}
            rx={1.5}
            fill={C.ink}
            opacity={0.8}
          />
          <rect
            x={cx + tabW + 3}
            y={cy}
            width={tabW}
            height={5}
            rx={1.5}
            fill="none"
            stroke={C.inkMute}
            opacity={0.5}
          />
          {Array.from({ length: n }).map((_, i) => {
            const up = i % 2 === 0;
            return (
              <g key={i}>
                <rect
                  x={cx}
                  y={cy + headerH + i * rowH}
                  width={Math.min(14, cw * 0.25)}
                  height={2.5}
                  fill={C.ink}
                  opacity={0.7}
                />
                <rect
                  x={cx + cw - Math.min(22, cw * 0.34)}
                  y={cy + headerH + i * rowH}
                  width={Math.min(22, cw * 0.34)}
                  height={2.5}
                  fill={up ? C.gainStrong : C.loss}
                  opacity={0.3}
                />
              </g>
            );
          })}
        </>
      );
    }
    case 'agent.conversation': {
      const hasInput = ch > 44;
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(30, cw * 0.32)}
            height={4}
            rx={1}
            fill={C.accent}
            opacity={0.85}
          />
          <rect
            x={cx}
            y={cy + 10}
            width={cw * 0.82}
            height={5}
            fill={C.ink}
          />
          <rect
            x={cx}
            y={cy + 19}
            width={cw * 0.65}
            height={4}
            fill={C.inkSoft}
            opacity={0.7}
          />
          {hasInput && (
            <>
              <rect
                x={cx}
                y={cy + ch - 12}
                width={cw - 2}
                height={10}
                rx={2}
                fill={C.paperHi}
                stroke={C.line}
              />
              <rect
                x={cx + cw - 14}
                y={cy + ch - 10}
                width={10}
                height={6}
                rx={1.5}
                fill={C.accent}
              />
            </>
          )}
        </>
      );
    }
    case 'workspace.picker': {
      const n = Math.max(2, Math.min(4, Math.floor(cw / 40)));
      const gap = 4;
      const cardW = (cw - gap * (n - 1)) / n;
      const cardH = Math.min(ch - 10, 34);
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(30, cw * 0.32)}
            height={3}
            fill={C.accent}
            opacity={0.8}
          />
          {Array.from({ length: n }).map((_, i) => (
            <g key={i}>
              <rect
                x={cx + i * (cardW + gap)}
                y={cy + 8}
                width={cardW}
                height={cardH}
                rx={2}
                fill={C.paperHi}
                stroke={C.line}
              />
              <rect
                x={cx + i * (cardW + gap) + 4}
                y={cy + 12}
                width={cardW * 0.5}
                height={2.5}
                fill={C.ink}
                opacity={0.7}
              />
              <rect
                x={cx + i * (cardW + gap) + 4}
                y={cy + 17}
                width={cardW * 0.7}
                height={2}
                fill={C.inkMute}
                opacity={0.4}
              />
            </g>
          ))}
        </>
      );
    }
    case 'threads.recent': {
      const rowH = 10;
      const headerH = 7;
      const n = Math.max(1, Math.min(8, Math.floor((ch - headerH) / rowH)));
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(32, cw * 0.36)}
            height={3}
            fill={C.inkSoft}
          />
          {Array.from({ length: n }).map((_, i) => (
            <g key={i}>
              <circle
                cx={cx + 3}
                cy={cy + headerH + 3 + i * rowH}
                r={1.5}
                fill={C.accent}
                opacity={0.6}
              />
              <rect
                x={cx + 8}
                y={cy + headerH + 1 + i * rowH}
                width={cw * 0.7 - 8}
                height={2.5}
                fill={C.ink}
                opacity={0.7}
              />
              <rect
                x={cx + 8}
                y={cy + headerH + 5 + i * rowH}
                width={cw * 0.55 - 8}
                height={2}
                fill={C.inkMute}
                opacity={0.4}
              />
            </g>
          ))}
        </>
      );
    }
    case 'automations.list': {
      const rowH = 10;
      const headerH = 7;
      const n = Math.max(1, Math.min(6, Math.floor((ch - headerH) / rowH)));
      return (
        <>
          <rect
            x={cx}
            y={cy}
            width={Math.min(36, cw * 0.45)}
            height={3}
            fill={C.inkSoft}
          />
          {Array.from({ length: n }).map((_, i) => (
            <g key={i}>
              <circle
                cx={cx + 4}
                cy={cy + headerH + 3 + i * rowH}
                r={2.5}
                fill="none"
                stroke={C.luxGold}
                strokeWidth={0.7}
              />
              <rect
                x={cx + 11}
                y={cy + headerH + 1 + i * rowH}
                width={cw * 0.55 - 8}
                height={2.5}
                fill={C.ink}
                opacity={0.7}
              />
              <rect
                x={cx + cw - Math.min(20, cw * 0.3)}
                y={cy + headerH + 1 + i * rowH}
                width={Math.min(16, cw * 0.26)}
                height={2.5}
                fill={C.inkMute}
                opacity={0.4}
              />
            </g>
          ))}
        </>
      );
    }
    default: {
      return (
        <>
          {[0, 1, 2].map((i) => (
            <rect
              key={i}
              x={cx}
              y={cy + i * 6}
              width={cw * (0.8 - i * 0.12)}
              height={3}
              fill={C.inkMute}
              opacity={0.4}
            />
          ))}
        </>
      );
    }
  }
}

interface LayoutProps {
  widgets: WidgetInstance[];
  layouts: RGLItem[];
}

function PresetLayoutThumbnail({ widgets, layouts }: LayoutProps) {
  const VIEW_W = 300;
  const VIEW_H = 190;
  const PAD = 4;
  const GRID_COLS = 12;
  const innerW = VIEW_W - PAD * 2;
  const innerH = VIEW_H - PAD * 2;
  const maxRow = Math.max(1, ...layouts.map((l) => l.y + l.h));
  const colW = innerW / GRID_COLS;
  const rowH = innerH / maxRow;
  const widgetsById = new Map(widgets.map((w) => [w.id, w]));

  return (
    <svg viewBox={`0 0 ${VIEW_W} ${VIEW_H}`} className="w-full block" aria-hidden>
      <rect x={0} y={0} width={VIEW_W} height={VIEW_H} fill={C.bg} />
      {layouts.map((l) => {
        const widget = widgetsById.get(l.i);
        if (!widget) return null;
        const visuals = TYPE_VISUALS[widget.type] ?? { bg: C.paper, stroke: C.line };
        const rx = PAD + l.x * colW + 1;
        const ry = PAD + l.y * rowH + 1;
        const rw = l.w * colW - 2;
        const rh = l.h * rowH - 2;
        return (
          <g key={l.i}>
            <rect
              x={rx}
              y={ry}
              width={rw}
              height={rh}
              rx={3}
              fill={visuals.bg}
              stroke={visuals.stroke ?? 'none'}
              strokeWidth={visuals.stroke ? 1 : 0}
            />
            <WidgetGlyph
              type={widget.type}
              config={widget.config}
              x={rx}
              y={ry}
              w={rw}
              h={rh}
            />
          </g>
        );
      })}
    </svg>
  );
}

export function PresetThumbnail({ id }: { id: PresetId }) {
  const preset = useMemo(() => getPreset(id), [id]);
  return (
    <PresetLayoutThumbnail
      widgets={preset.widgets}
      layouts={preset.layouts.lg ?? []}
    />
  );
}
