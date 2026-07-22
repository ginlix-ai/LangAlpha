import { FileText, Loader2, X } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import { WidgetContextDeck } from '@/pages/Dashboard/widgets/framework/WidgetContextDeck';
import type { FileAttachment } from './chat-input.types';

const formatFileSize = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/* --- FILE PREVIEW CARD --- */
export const FilePreviewCard = ({ file, onRemove }: { file: FileAttachment; onRemove: (id: string) => void }) => {
  const isMobilePreview = useIsMobile();
  const isImage = file.type.startsWith('image/') && file.preview;

  return (
    <div className="relative group flex-shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-[var(--color-border-muted)] bg-[var(--color-bg-elevated)] animate-fade-in transition-all hover:border-[var(--color-border-default)]">
      {isImage ? (
        <div className="w-full h-full relative">
          <img src={file.preview!} alt={file.file.name} className="w-full h-full object-cover" />
          <div className="absolute inset-0 bg-black/20 group-hover:bg-black/0 transition-colors" />
        </div>
      ) : (
        <div className="w-full h-full p-3 flex flex-col justify-between">
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded" style={{ background: 'var(--color-border-muted)' }}>
              <FileText className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <span className="text-[10px] font-medium uppercase tracking-wider truncate" style={{ color: 'var(--color-text-tertiary)' }}>
              {file.file.name.split('.').pop()}
            </span>
          </div>
          <div className="space-y-0.5">
            <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-muted)' }} title={file.file.name}>
              {file.file.name}
            </p>
            <p className="text-[10px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {formatFileSize(file.file.size)}
            </p>
          </div>
        </div>
      )}

      {/* Remove Button Overlay */}
      <button
        onClick={() => onRemove(file.id)}
        className={`absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white transition-opacity ${isMobilePreview ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
      >
        <X className="w-3 h-3" />
      </button>

      {/* Upload Status */}
      {file.uploadStatus === 'uploading' && (
        <div className="absolute inset-0 bg-black/40 flex items-center justify-center">
          <Loader2 className="w-5 h-5 text-white animate-spin" />
        </div>
      )}
    </div>
  );
};

/**
 * Composer pill toggle (Plan / Watch). Transparent when off; fills with
 * `--color-border-muted` on hover and while active. `activeClass` tags the
 * active state for CSS hooks; `title` is the hover tooltip.
 */
export function PillToggle({
  active,
  onToggle,
  icon: Icon,
  label,
  title,
  activeClass,
}: {
  active: boolean;
  onToggle: () => void;
  icon: LucideIcon;
  label: string;
  title: string;
  activeClass: string;
}) {
  return (
    <button
      className={`inline-flex items-center rounded-full border-none cursor-pointer${active ? ` ${activeClass}` : ''}`}
      style={{
        gap: '6px',
        padding: '6px 10px',
        fontSize: '13px',
        fontWeight: 500,
        background: active ? 'var(--color-border-muted)' : 'transparent',
        color: 'var(--color-text-muted, #8b8fa3)',
        border: '1px solid transparent',
        transition: 'background 0.2s, color 0.2s, border-color 0.2s',
      }}
      onClick={(e) => { e.stopPropagation(); onToggle(); }}
      onMouseEnter={(e) => {
        if (!active) e.currentTarget.style.background = 'var(--color-border-muted)';
      }}
      onMouseLeave={(e) => {
        if (!active) e.currentTarget.style.background = 'transparent';
      }}
      type="button"
      title={title}
      aria-pressed={active}
    >
      <Icon className="h-4 w-4" style={active ? { color: 'var(--color-accent-light)' } : {}} />
      <span>{label}</span>
    </button>
  );
}

/* --- WIDGET CONTEXT DECK ---
 *
 * The chat-input live deck is a thin wrapper around the shared
 * `WidgetContextDeck` component (in `pages/Dashboard/widgets/framework/`).
 * The shared component owns card geometry, fanning, outside-click collapse,
 * and the preview modal; we supply the live-deck-only chrome (eyebrow row
 * with clear button, per-card remove `×`, fan-hint chevron) via render
 * slots so the visual + behavioral contract stays identical to the
 * chat-view inline deck.
 */
export function ChatInputWidgetDeck({
  snapshots,
  fanned,
  onToggle,
  onCollapse,
  onRemove,
  onClear,
  boundaryRef,
}: {
  snapshots: WidgetContextSnapshot[];
  fanned: boolean;
  onToggle: () => void;
  onCollapse: () => void;
  onRemove: (widgetId: string) => void;
  onClear: () => void;
  /** The chat-input outer container. Clicks within this boundary (textarea,
   *  send button, attach controls) keep the deck fanned; only clicks fully
   *  outside the chat input collapse it. */
  boundaryRef: React.RefObject<HTMLElement | null>;
}) {
  const { t } = useTranslation();
  const cardCount = snapshots.length;
  return (
    <WidgetContextDeck
      snapshots={snapshots}
      fanned={fanned}
      onToggleFan={onToggle}
      onCollapse={onCollapse}
      boundaryRef={boundaryRef}
      className="widget-drag-cancel"
      testId="widget-context-deck"
      eyebrow={
        <div className="widget-deck-eyebrow">
          <span className="widget-deck-eyebrow-left">
            <span className="widget-deck-dot" />
            {t('chat.widgetContext.inContext', { count: cardCount, defaultValue: '{{count}} in context' })}
            {cardCount > 1 && !fanned && (
              <span className="widget-deck-hint">{t('chat.widgetContext.fanHint', { defaultValue: 'click to fan' })}</span>
            )}
          </span>
          <span className="widget-deck-eyebrow-right">
            {fanned && cardCount > 1 && (
              <button
                type="button"
                className="widget-deck-show-less"
                onClick={(e) => {
                  e.stopPropagation();
                  onCollapse();
                }}
              >
                {t('chat.widgetContext.showLess', { defaultValue: 'Show less' })}
              </button>
            )}
            <button
              type="button"
              className="widget-deck-clear"
              onClick={(e) => {
                e.stopPropagation();
                onClear();
              }}
            >
              {t('chat.widgetContext.clear', { defaultValue: 'Clear' })}
            </button>
          </span>
        </div>
      }
      renderCardSlotEnd={(s) => (
        <button
          type="button"
          className="widget-deck-card-remove"
          onClick={(e) => {
            e.stopPropagation();
            onRemove(s.widget_id);
          }}
          aria-label={t('chat.widgetContext.removeAria', { defaultValue: 'Remove from context' })}
        >
          <X className="h-3 w-3" />
        </button>
      )}
    />
  );
}
