import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import type { ChartSelection } from '@/pages/MarketView/stores/chartSelectionStore';

export const ACCEPTED_IMAGE_TYPES = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'];
export const ACCEPTED_PDF_TYPES = ['application/pdf'];
export const MAX_FILE_SIZE = 10 * 1024 * 1024; // 10MB
export const MAX_FILES = 5;

export interface Attachment {
  file: File;
  dataUrl: string | null;
  type: string;
}

export interface AttachmentContext {
  type: string;
  data: string;
  description: string;
}

export interface FileValidationResult {
  valid: boolean;
  error?: string;
}

export function readFileAsDataUrl(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result as string);
    reader.onerror = () => reject(new Error(`Failed to read file: ${file.name}`));
    reader.readAsDataURL(file);
  });
}

/**
 * Convert attachments to additional_context format with accurate type tags.
 * Images → "image", PDFs → "pdf", everything else → "file".
 */
export function attachmentsToContexts(attachments: Attachment[]): AttachmentContext[] {
  return attachments
    .filter((a) => a.dataUrl != null)
    .map((a) => ({
      type: a.type.startsWith('image/') ? 'image'
          : a.type === 'application/pdf' ? 'pdf'
          : 'file',
      data: a.dataUrl!,
      description: a.file.name,
    }));
}

/**
 * Per-widget snapshot serialization for `additional_context`.
 *
 * Each snapshot becomes ONE `{type:"widget", ...}` item. Image-bearing
 * snapshots additionally produce a sibling `{type:"image", ...}` item that
 * rides the existing MultimodalContext channel — the backend modality gate
 * handles vision-vs-text-only routing without further code changes.
 *
 * Pre-flight size guard: a structured-clone error from `navigate(state)` will
 * crash the dashboard → chat handoff, so we cap the structured `data` payload
 * at ~100KB per item. Oversized `data` is dropped (the rendered `text` and
 * the optional image still ride along — those are what the agent reads).
 */
const MAX_DATA_BYTES_PER_SNAPSHOT = 100 * 1024;

interface WidgetCtxItem {
  type: 'widget';
  widget_type: string;
  widget_id: string;
  label: string;
  text: string;
  data: Record<string, unknown>;
  captured_at?: string;
  description?: string;
}

interface WidgetImageItem {
  type: 'image';
  data: string;
  description: string;
}

export function widgetSnapshotsToContexts(
  snapshots: WidgetContextSnapshot[],
): Array<WidgetCtxItem | WidgetImageItem> {
  const out: Array<WidgetCtxItem | WidgetImageItem> = [];
  for (const s of snapshots) {
    let data = s.data ?? {};
    try {
      const sz = new Blob([JSON.stringify(data)]).size;
      if (sz > MAX_DATA_BYTES_PER_SNAPSHOT) {
        // Drop the structured payload; the rendered text + image still ship.
        data = { _truncated: true, _original_bytes: sz };
      }
    } catch {
      data = { _truncated: true };
    }
    out.push({
      type: 'widget',
      widget_type: s.widget_type,
      widget_id: s.widget_id,
      label: s.label,
      text: s.text,
      data,
      captured_at: s.captured_at,
      description: s.description,
    });
    if (s.image_jpeg_data_url) {
      out.push({
        type: 'image',
        data: s.image_jpeg_data_url,
        description: s.label,
      });
    }
  }
  return out;
}

interface ChartSelectionCtxItem {
  type: 'chart_selection';
  symbol: string;
  timeframe: string;
  selection_type: 'region' | 'price_level';
  time_start?: string;
  time_end?: string;
  price_low: number;
  price_high: number;
  bars: ChartSelection['bars'];
  bars_truncated: boolean;
  label?: string;
}

interface ChartSelectionImageItem {
  type: 'image';
  data: string;
  description: string;
}

/** Caption for a region's cropped screenshot — the structured item holds the exact values. */
export function describeSelectionImage(sel: ChartSelection): string {
  const range = `$${sel.priceLow}–$${sel.priceHigh}`;
  const span = sel.timeStart && sel.timeEnd ? `, ${sel.timeStart} → ${sel.timeEnd}` : '';
  return `Cropped chart image of ${sel.symbol} ${sel.timeframe} (price ${range}${span})`;
}

/**
 * Map a user's chart selection to `additional_context` items.
 *
 * Emits one structured `chart_selection` item (bounds + per-candle OHLCV the
 * agent can analyze and draw back onto the exact region). A region selection
 * with a cropped screenshot also emits a sibling `{type:"image", ...}` item
 * that rides the existing MultimodalContext channel — a vision-capable model
 * sees the region directly, others fall back to the bars via the backend
 * modality gate. The structured bars stay the primary signal.
 *
 * Pass `liveSymbol`/`liveTimeframe` to drop a stale selection (the chart was
 * switched to a different instance after the user drew it).
 */
export function chartSelectionToContext(
  sel: ChartSelection,
  live?: { symbol?: string | null; timeframe?: string | null },
): Array<ChartSelectionCtxItem | ChartSelectionImageItem> {
  if (live) {
    const liveSym = (live.symbol ?? '').toUpperCase();
    const liveTf = live.timeframe ?? '';
    if ((liveSym && liveSym !== sel.symbol) || (liveTf && liveTf !== sel.timeframe)) {
      return [];
    }
  }

  const comment = sel.comment?.trim();
  const item: ChartSelectionCtxItem = {
    type: 'chart_selection',
    symbol: sel.symbol,
    timeframe: sel.timeframe,
    selection_type: sel.selectionType,
    price_low: sel.priceLow,
    price_high: sel.priceHigh,
    bars: sel.bars,
    bars_truncated: sel.barsTruncated,
  };
  if (sel.selectionType === 'region') {
    item.time_start = sel.timeStart;
    item.time_end = sel.timeEnd;
  }
  // The user's per-selection note rides as `label`, separate from the message.
  if (comment) item.label = comment;

  const out: Array<ChartSelectionCtxItem | ChartSelectionImageItem> = [item];
  if (sel.croppedImage) {
    out.push({ type: 'image', data: sel.croppedImage, description: describeSelectionImage(sel) });
  }
  return out;
}

/**
 * Validate a file for upload.
 * When flashOnly is true, only images and PDFs are accepted (Flash mode).
 * Otherwise any file type is accepted (PTC mode).
 */
export function validateFile(file: File, flashOnly = false): FileValidationResult {
  if (flashOnly) {
    const allAccepted = [...ACCEPTED_IMAGE_TYPES, ...ACCEPTED_PDF_TYPES];
    if (!allAccepted.includes(file.type)) {
      return { valid: false, error: `Unsupported file type: ${file.type || 'unknown'}` };
    }
  }
  if (file.size > MAX_FILE_SIZE) {
    return { valid: false, error: `File too large: ${file.name} (max 10MB)` };
  }
  return { valid: true };
}
