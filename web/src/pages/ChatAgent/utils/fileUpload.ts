import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

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
