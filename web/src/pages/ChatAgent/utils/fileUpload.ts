import type { ReadyAttachment } from '@/components/ui/chat-input.types';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';

/** An image by its MIME type, the server's `image` tag, or its name, for a file the browser could not type. */
export function isImageFile({ type = '', name = '' }: { type?: string; name?: string }): boolean {
  return type.startsWith('image/') || type === 'image' || /\.(jpe?g|png|gif|webp)$/i.test(name);
}

export type AttachmentContext = {
  type: 'image' | 'pdf' | 'file';
  attachment_id: string;
  description: string;
};

/**
 * The sent bubble's record of an attachment, until history replaces it with
 * the server's. A staged file carries itself, which survives the route state
 * a send from another page travels in; a chart capture carries its image as
 * `dataUrl`, having no file behind it.
 */
export type AttachmentMeta = {
  name: string;
  type: string;
  size: number;
  file?: File;
  dataUrl?: string;
  /** History only: the turn never received this file, so there is nothing to show. */
  missing?: boolean;
  /** When the staged file became ready, kept so a send handed back to the
   *  composer re-stages it before the server's expiry. Never sent. */
  readyAt?: number;
};

/**
 * What a send carries for its staged files: the `additional_context` items
 * naming them, typed for the agent (images, PDFs, everything else), and the
 * bubble's record of them.
 */
export function attachmentSendParts(files: readonly ReadyAttachment[]): { contexts: AttachmentContext[]; meta: AttachmentMeta[] } {
  return {
    contexts: files.map((a) => ({
      type: isImageFile({ type: a.type, name: a.file.name }) ? 'image' : a.type === 'application/pdf' ? 'pdf' : 'file',
      attachment_id: a.attachmentId,
      description: a.file.name,
    })),
    meta: files.map((a) => ({
      name: a.file.name,
      type: a.type,
      size: a.file.size,
      file: a.file,
      ...(a.readyAt !== undefined ? { readyAt: a.readyAt } : {}),
    })),
  };
}

/** Whether a send's `additional_context` names files staged on the server. */
export function namesStagedFiles(contexts: readonly Record<string, unknown>[] | null | undefined): boolean {
  return !!contexts?.some((c) => typeof c.attachment_id === 'string');
}

/**
 * The staged files a send carries, as the composer holds them, read back from
 * what `attachmentSendParts` made of them: its items name them in the order
 * its records hold the files.
 */
export function stagedFilesOf(
  contexts: readonly Record<string, unknown>[] | null | undefined,
  meta: readonly AttachmentMeta[] | null | undefined,
): ReadyAttachment[] {
  const ids = (contexts ?? []).flatMap((c) => (typeof c.attachment_id === 'string' ? [c.attachment_id] : []));
  const files = (meta ?? []).filter((m): m is AttachmentMeta & { file: File } => !!m.file);
  return ids.slice(0, files.length).map((attachmentId, i) => ({
    file: files[i].file,
    type: files[i].type,
    attachmentId,
    ...(files[i].readyAt !== undefined ? { readyAt: files[i].readyAt } : {}),
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

type WidgetCtxItem = {
  type: 'widget';
  widget_type: string;
  widget_id: string;
  label: string;
  text: string;
  data: Record<string, unknown>;
  captured_at?: string;
  description?: string;
};

type WidgetImageItem = {
  type: 'image';
  data: string;
  description: string;
};

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
