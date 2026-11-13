import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { FileText, ImageIcon, Ruler, SquareDashedMousePointer } from 'lucide-react';
import type { WidgetContextPreviewShape } from '@/pages/Dashboard/widgets/framework/WidgetContextPreview';
import { WidgetContextDeck } from '@/pages/Dashboard/widgets/framework/WidgetContextDeck';
import { SelectionContextPreview, type SelectionPreviewShape } from '../SelectionContextPreview';
import { selectionPriceBounds } from '../../utils/selectionBounds';
import { useLocale } from '@/hooks/useLocale';
import { useObjectUrl } from '@/hooks/useObjectUrl';
import { formatBytes } from '@/lib/format';
import ImageLightbox from '../ImageLightbox';
import { isImageFile } from '../../utils/fileUpload';

// --- AttachmentCard ---

export interface AttachmentData {
  name?: string;
  type?: string;
  size?: number;
  /** A live send's file, its thumbnail until history replaces it. */
  file?: File;
  /** A chart capture's image, which has no file behind it. */
  dataUrl?: string;
  url?: string;
  /** History only: the turn never received this file. */
  missing?: boolean;
  [key: string]: unknown;
}

interface AttachmentCardProps {
  attachment: AttachmentData;
}

/** Local alias for the preview shape — keeps inline-deck internals decoupled from the import path. */
export type WidgetChipShape = WidgetContextPreviewShape;

/**
 * Static read-only deck rendered below the user message bubble. Reuses the
 * shared `WidgetContextDeck` — no eyebrow, no remove buttons, just the
 * stacked cards with click-to-fan / click-to-preview semantics. Width is
 * pinned at 320px since the user-message column has a `max-w-[80%]`
 * constraint that would otherwise collapse absolute-positioned cards.
 */
export function InlineWidgetDeck({ snapshots }: { snapshots: WidgetChipShape[] }) {
  const [fanned, setFanned] = useState(false);
  return (
    <WidgetContextDeck
      snapshots={snapshots}
      fanned={fanned}
      onToggleFan={() => setFanned((p) => !p)}
      onCollapse={() => setFanned(false)}
      compactCardGrid
      style={{
        width: 320,
        borderBottom: 'none',
        marginBottom: 0,
        paddingBottom: 0,
      }}
    />
  );
}

/**
 * Read-only cards below the user bubble summarizing the chart selections
 * (region / price level + note) attached to this send. Styled like the
 * widget-context cards: an icon thumb plus a title and a note/bounds snippet.
 * Clicking a card opens a "how the agent sees it" preview of its context.
 */
export function InlineSelectionCards({ selections }: { selections: SelectionPreviewShape[] }) {
  const { t } = useTranslation();
  const [previewed, setPreviewed] = useState<SelectionPreviewShape | null>(null);
  return (
    <div className="flex flex-col items-end gap-1.5">
      {selections.map((s) => {
        const Icon = s.selectionType === 'region' ? SquareDashedMousePointer : Ruler;
        const title = s.selectionType === 'region'
          ? t('marketView.selection.cardRegionTitle')
          : t('marketView.selection.cardPriceTitle');
        // en-US on purpose: the bounds mirror the context text the agent receives.
        const bounds = selectionPriceBounds(s, 'en-US');
        // Prefer the user's note as the snippet; fall back to the bounds.
        const snippet = s.comment ? `“${s.comment}”` : bounds;
        return (
          <button
            key={`${s.selectionType}-${s.symbol}-${s.timeframe}-${s.priceLow}-${s.priceHigh}-${s.timeStart ?? ''}`}
            type="button"
            onClick={() => setPreviewed(s)}
            title={t('marketView.selection.cardOpenPreview')}
            className="flex items-center gap-2.5 text-left transition-colors hover:brightness-105"
            style={{
              width: 280,
              maxWidth: '100%',
              padding: '8px 12px',
              borderRadius: 10,
              background: 'var(--color-bg-elevated)',
              border: '1px solid var(--color-border-muted)',
              boxShadow: 'var(--shadow-card)',
              cursor: 'pointer',
            }}
          >
            <div
              className="flex items-center justify-center shrink-0"
              style={{
                width: 34,
                height: 34,
                borderRadius: 8,
                background: 'var(--color-accent-soft)',
                color: 'var(--color-accent-light)',
              }}
            >
              <Icon className="h-4 w-4" />
            </div>
            <div className="min-w-0 flex flex-col">
              <div
                className="truncate"
                style={{ fontSize: '0.8125rem', fontWeight: 600, color: 'var(--color-text-primary)' }}
              >
                {title}
                <span style={{ fontWeight: 400, color: 'var(--color-text-tertiary)' }}>
                  {' · '}{s.symbol} {s.timeframe}
                </span>
              </div>
              <div
                className="truncate"
                style={{ fontSize: '0.75rem', color: 'var(--color-text-secondary)' }}
              >
                {snippet}
              </div>
            </div>
          </button>
        );
      })}
      <SelectionContextPreview selection={previewed} onClose={() => setPreviewed(null)} />
    </div>
  );
}

/**
 * AttachmentCard -- 96x96 preview card matching the composer's file tiles.
 * Handles both live attachments (the file itself, or a chart capture's
 * dataUrl) and history attachments (name/type/size, and a url once stored).
 */
export function AttachmentCard({ attachment }: AttachmentCardProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const [previewOpen, setPreviewOpen] = useState(false);
  const att = attachment;
  const isImage = isImageFile(att);
  const fileUrl = useObjectUrl(isImage ? att.file : null);
  const src = fileUrl || att.dataUrl || att.url;
  const ext = att.name?.split('.').pop() || '';

  // A file the turn never received has no bytes to show or open.
  if (att.missing) {
    return (
      <div
        role="img"
        aria-label={t('chat.composer.attachment.notReceivedLabel', { name: att.name })}
        title={att.name}
        className="relative shrink-0 w-24 h-24 rounded-xl overflow-hidden opacity-60"
        style={{ border: '1px dashed var(--color-border-muted)', background: 'var(--color-bg-input)' }}
      >
        <div className="w-full h-full p-3 flex flex-col justify-between">
          <div className="p-1.5 rounded self-start" style={{ background: 'var(--color-border-muted)' }}>
            {isImage
              ? <ImageIcon className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />
              : <FileText className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />}
          </div>
          <div className="space-y-0.5">
            <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-tertiary)' }}>{att.name}</p>
            <p className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>{t('chat.composer.attachment.notReceived')}</p>
          </div>
        </div>
      </div>
    );
  }

  // A live file has its URL from the first commit on; until then the card stands empty.
  if (isImage && (src || att.file)) {
    return (
      <>
        <button
          type="button"
          onClick={(e) => { e.stopPropagation(); setPreviewOpen(true); }}
          aria-label={t('chat.composer.attachment.preview', { name: att.name })}
          className="relative group shrink-0 w-24 h-24 rounded-xl overflow-hidden cursor-zoom-in"
          style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}
        >
          {src && <img src={src} alt={att.name} className="w-full h-full object-cover" />}
          <div className="absolute inset-0 bg-black/20 transition-colors group-hover:bg-black/0" />
        </button>
        {src && <ImageLightbox src={src} alt={att.name} open={previewOpen} onClose={() => setPreviewOpen(false)} />}
      </>
    );
  }

  if (isImage) {
    // History image -- no thumbnail available, show placeholder
    return (
      <div className="relative shrink-0 w-24 h-24 rounded-xl overflow-hidden" style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}>
        <div className="w-full h-full p-3 flex flex-col items-center justify-center gap-2">
          <ImageIcon className="w-6 h-6" style={{ color: 'var(--color-icon-muted)' }} />
          <p className="text-[0.625rem] truncate w-full text-center" style={{ color: 'var(--color-text-tertiary)' }}>{att.name}</p>
        </div>
      </div>
    );
  }

  // PDF / generic file card
  return (
    <div className="relative shrink-0 w-24 h-24 rounded-xl overflow-hidden" style={{ border: '1px solid var(--color-border-muted)', background: 'var(--color-bg-input)' }}>
      <div className="w-full h-full p-3 flex flex-col justify-between">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded" style={{ background: 'var(--color-border-muted)' }}>
            <FileText className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />
          </div>
          <span className="text-[0.625rem] font-medium uppercase tracking-wider truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {ext}
          </span>
        </div>
        <div className="space-y-0.5">
          <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-muted)' }} title={att.name}>{att.name}</p>
          {(att.size ?? 0) > 0 && (
            <p className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>{formatBytes(att.size ?? 0, locale)}</p>
          )}
        </div>
      </div>
    </div>
  );
}
