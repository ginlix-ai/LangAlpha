import React, { useMemo } from 'react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import Markdown from '@/pages/ChatAgent/components/Markdown';
import type { WidgetContextSnapshot } from './contextSnapshot';

/**
 * Subset of `WidgetContextSnapshot` the preview actually needs. Live
 * snapshots from the chat-input always have the full shape; replayed
 * snapshots from history may be missing `text` (older rows pre-dating the
 * `serialize_widget_contexts_for_metadata` change) or `image_jpeg_data_url`
 * (image rides the multimodal channel and isn't persisted). Both paths
 * render through the same component so the user sees the same layout
 * before sending and after replay.
 */
export type WidgetContextPreviewShape = Pick<
  WidgetContextSnapshot,
  'widget_type' | 'widget_id' | 'label'
> & {
  description?: string;
  text?: string;
  data?: unknown;
  image_jpeg_data_url?: string;
};

/** Strip the outer ``<widget-context type='...' ...>...</widget-context>``
 *  envelope so the inner markdown body renders cleanly without the XML
 *  scaffolding the agent only sees in its prompt context. */
function stripWidgetContextEnvelope(text: string): string {
  const trimmed = text.trim();
  const opener = /^<widget-context\b[^>]*>\s*\n?/;
  const closer = /\n?\s*<\/widget-context>\s*$/;
  return trimmed.replace(opener, '').replace(closer, '').trim();
}

/** Humanize a snake_case / camelCase key into a Title Case label. */
function humanizeKey(k: string): string {
  return k
    .replace(/[_-]+/g, ' ')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2})/;

/** Recursively render a JSON-ish value as a human-friendly tree. URLs become
 *  links, ISO timestamps become localized dates, scalar arrays become inline
 *  comma-joined lists, and deeply-nested branches collapse to a code dump
 *  past `maxDepth` so the modal stays bounded. */
function renderStructuredValue(v: unknown, depth: number, maxDepth = 4): React.ReactNode {
  if (v === null || v === undefined || v === '') {
    return <span style={{ color: 'var(--color-text-tertiary)' }}>—</span>;
  }
  if (typeof v === 'string') {
    if (/^https?:\/\//.test(v)) {
      return (
        <a
          href={v}
          target="_blank"
          rel="noopener noreferrer"
          className="underline break-all"
          style={{ color: 'var(--color-accent-primary, #b88a2c)' }}
        >
          {v}
        </a>
      );
    }
    if (ISO_DATE_RE.test(v)) {
      const d = new Date(v);
      if (!Number.isNaN(d.getTime())) {
        return <span title={v}>{d.toLocaleString()}</span>;
      }
    }
    return <span className="break-words whitespace-pre-wrap">{v}</span>;
  }
  if (typeof v === 'number' || typeof v === 'boolean') return String(v);
  if (Array.isArray(v)) {
    if (v.length === 0) return <span style={{ color: 'var(--color-text-tertiary)' }}>(empty)</span>;
    const allScalar = v.every(
      (x) => typeof x === 'string' || typeof x === 'number' || typeof x === 'boolean',
    );
    if (allScalar) return <span>{v.map((x) => String(x)).join(', ')}</span>;
    if (depth >= maxDepth) return <code className="text-xs">{JSON.stringify(v)}</code>;
    return (
      <ol className="list-decimal pl-5 space-y-3">
        {v.map((x, i) => (
          <li key={i}>{renderStructuredValue(x, depth + 1, maxDepth)}</li>
        ))}
      </ol>
    );
  }
  if (typeof v === 'object') {
    if (depth >= maxDepth) return <code className="text-xs">{JSON.stringify(v)}</code>;
    const entries = Object.entries(v as Record<string, unknown>);
    if (entries.length === 0) return <span style={{ color: 'var(--color-text-tertiary)' }}>(empty)</span>;
    return (
      <dl className="space-y-1.5">
        {entries.map(([k, val]) => (
          <div key={k} className="grid grid-cols-[110px_1fr] gap-3">
            <dt
              className="text-[10px] font-semibold uppercase tracking-[0.12em] pt-0.5"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {humanizeKey(k)}
            </dt>
            <dd className="break-words text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {renderStructuredValue(val, depth + 1, maxDepth)}
            </dd>
          </div>
        ))}
      </dl>
    );
  }
  return <code className="text-xs">{String(v)}</code>;
}

/** Last-resort fallback for snapshots with `data` but no `text` (older
 *  persisted rows). Recursively walks the structured payload so nested
 *  objects and arrays render as nested fields/lists rather than a JSON
 *  dump. New snapshots emit `text` and use the Markdown path instead. */
function StructuredDataFallback({ data }: { data: unknown }) {
  return <div className="text-sm">{renderStructuredValue(data, 0)}</div>;
}

/**
 * Modal preview rendering exactly what the agent receives for one snapshot.
 * Strips the ``<widget-context>`` envelope from `text` and renders the
 * inner markdown via the chat's standard Markdown component, so the same
 * layout appears whether the user is previewing a card before sending
 * (live snapshot from chat-input) or after replay (history metadata).
 *
 * Falls back to a structured key/value tree when only `data` exists
 * (older persisted rows from before the text-in-metadata change).
 */
export function WidgetContextPreview({
  snapshot,
  onClose,
}: {
  snapshot: WidgetContextPreviewShape | null;
  onClose: () => void;
}) {
  const open = snapshot !== null;
  const markdownBody = useMemo(() => {
    if (!snapshot?.text) return '';
    return stripWidgetContextEnvelope(snapshot.text);
  }, [snapshot]);
  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o) onClose(); }}>
      <DialogContent
        className="max-w-2xl"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        <DialogHeader>
          <DialogTitle className="text-base" style={{ color: 'var(--color-text-primary)' }}>
            {snapshot?.label ?? ''}
          </DialogTitle>
          {snapshot?.description && (
            <DialogDescription className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {snapshot.description}
            </DialogDescription>
          )}
        </DialogHeader>
        <div className="max-h-[65vh] overflow-auto">
          {snapshot?.image_jpeg_data_url && (
            <img
              src={snapshot.image_jpeg_data_url}
              alt=""
              className="w-full rounded-md border mb-3"
              style={{ borderColor: 'var(--color-border-muted)' }}
            />
          )}
          {markdownBody ? (
            <Markdown content={markdownBody} variant="panel" />
          ) : snapshot?.data !== undefined ? (
            <StructuredDataFallback data={snapshot.data} />
          ) : (
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              (no preview content)
            </p>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default WidgetContextPreview;
