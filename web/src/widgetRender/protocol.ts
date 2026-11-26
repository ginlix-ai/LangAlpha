/**
 * What the backend driver (`src/server/services/widget_render.py`) and the
 * widget render page say to each other. The driver loads the page, then calls
 * `window.__LA_WIDGET_RENDER_RUN__(payload)` and reads the result it resolves.
 * While the widget runs, the page hands each diagnostic to the driver as it
 * arrives (`__laWidgetDiag`), so a widget that hangs the page still leaves
 * what it said before, and asks whether any request is still in flight
 * (`__laWidgetNetBusy`) before it calls the widget settled. The frame's own
 * messages are the chat's widget protocol (`viewers/html/widgetProtocol.ts`).
 */
import type { WidgetDiag, WidgetInspection } from '@/pages/ChatAgent/components/viewers/html/widgetProtocol';

export type { WidgetDiag, WidgetInspection };

export interface WidgetRenderPayload {
  html: string;
  data?: Record<string, string> | null;
  /** Absolute URL ending in `/` that relative URLs in the widget resolve against. */
  base?: string | null;
  /** The frame's width in CSS px (the widest chat column); the screenshot's too. */
  width: number;
  /**
   * A phone's column: after the main pass the frame is measured again at this
   * width, then put back. Absent or null skips the pass.
   */
  narrowWidth?: number | null;
  /** Quiet after the last resize or diagnostic before the widget counts as settled. */
  settleMs: number;
  /** Give up waiting for quiet after this long, and report what is there. */
  maxWaitMs: number;
  /** The tallest the frame may grow, whatever the widget reports. */
  maxHeight: number;
}

export interface WidgetRenderResult {
  /** True when the widget went quiet; false when the wait hit its cap. */
  settled: boolean;
  /** The frame's document finished loading (its `load` event fired). */
  loaded: boolean;
  /** The last height the widget reported, or null if it never did. */
  height: number | null;
  /** The height the frame was given (what a screenshot should cover). */
  frameHeight: number;
  /** Null when the frame did not answer the inspect request. */
  inspection: WidgetInspection | null;
  /**
   * The same answer at `narrowWidth`, or null when that pass was not run (no
   * main answer, too little time left) or the frame did not answer it.
   */
  narrow: WidgetInspection | null;
}

declare global {
  interface Window {
    __LA_WIDGET_RENDER_RUN__?: (payload: WidgetRenderPayload) => Promise<WidgetRenderResult>;
    __laWidgetDiag?: (diag: WidgetDiag) => Promise<void>;
    __laWidgetNetBusy?: () => Promise<number>;
  }
}
