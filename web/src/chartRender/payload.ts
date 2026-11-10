/**
 * What the snapshot driver hands the render page, as `window.__LA_CHART_RENDER__`
 * set by an init script before the bundle runs. The bars and layers are not in
 * it: the chart fetches them from the app's real API routes, which the driver
 * answers.
 */
export interface ChartRenderPayload {
  symbol: string;
  timeframe: string;
  theme: 'light' | 'dark';
  locale: 'en-US' | 'zh-CN';
  /** Any id: the driver answers the chart-annotations route for it. */
  workspaceId: string;
  /** The chart region's size in CSS px (toolbar and bottom bar excluded). */
  width: number;
  height: number;
}

/** Set on the window once the page has rendered the chart, or failed to. */
export interface ChartRenderStatus {
  mounted: boolean;
  error: string | null;
}

/** How the driver asks the chart to frame itself once its bars are drawn. */
export type ChartRenderFrame =
  | { kind: 'layer'; layerId: string }
  | { kind: 'range'; from: string; to: string };

export type ChartRenderFrameStatus = 'ok' | 'pending' | 'unsupported';

/** The window on screen, in the convention a user's message carries it (`chart_views`). */
export interface ChartRenderView {
  from: string;
  to: string;
  width: number;
  height: number;
}

declare global {
  interface Window {
    __LA_CHART_RENDER__?: Partial<ChartRenderPayload>;
    __LA_CHART_RENDER_STATUS__?: ChartRenderStatus;
    __LA_CHART_RENDER_FRAME__?: (frame: ChartRenderFrame) => ChartRenderFrameStatus;
    __LA_CHART_RENDER_VIEW__?: () => ChartRenderView | null;
  }
}

export function readPayload(raw: Partial<ChartRenderPayload> | undefined): ChartRenderPayload {
  if (!raw?.symbol || !raw.timeframe) throw new Error('chart render: no payload (symbol and timeframe are required)');
  return {
    symbol: raw.symbol,
    timeframe: raw.timeframe,
    theme: raw.theme === 'dark' ? 'dark' : 'light',
    locale: raw.locale === 'zh-CN' ? 'zh-CN' : 'en-US',
    workspaceId: raw.workspaceId || 'snapshot',
    width: Math.round(raw.width || 1400),
    height: Math.round(raw.height || 800),
  };
}
