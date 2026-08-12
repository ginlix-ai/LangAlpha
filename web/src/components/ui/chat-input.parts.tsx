import type { ButtonHTMLAttributes, CSSProperties, ReactNode, Ref } from 'react';
import { FileText, X } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useIsMobile } from '@/hooks/useIsMobile';
import { cn } from '@/lib/utils';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import { WidgetContextDeck } from '@/pages/Dashboard/widgets/framework/WidgetContextDeck';
import { getSlashCommandIcon } from './chat-input.helpers';
import type { FileAttachment, SlashCommand } from './chat-input.types';

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
            <span className="text-[0.625rem] font-medium uppercase tracking-wider truncate" style={{ color: 'var(--color-text-tertiary)' }}>
              {file.file.name.split('.').pop()}
            </span>
          </div>
          <div className="space-y-0.5">
            <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-muted)' }} title={file.file.name}>
              {file.file.name}
            </p>
            <p className="text-[0.625rem]" style={{ color: 'var(--color-text-tertiary)' }}>
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
          <Loader size={20} label="Uploading" className="text-white" />
        </div>
      )}
    </div>
  );
};

/**
 * The single composer pill: every labeled control in the action bar (mode,
 * workspace, plan, watch) is one of these, so pill geometry exists once and
 * the hidden measure row can render the very same component.
 *
 * Always transparent with a transient hover fill; active state is signaled by
 * the amber accent alone (icon + label), never a fill or border ring.
 */
export function PillToggle({
  active,
  onToggle,
  icon: Icon,
  label,
  title,
  trailing,
  disabledReason,
  aria = 'pressed',
  gap = 6,
  className = 'flex-none',
  labelClassName,
  buttonRef,
  measureOnly = false,
}: {
  /** Amber accent state (a toggle that is on, or a menu pill that is open). */
  active?: boolean;
  onToggle?: () => void;
  icon: LucideIcon;
  label: string;
  title?: string;
  /** Rendered after the label — e.g. the chevron on a menu-opening pill. */
  trailing?: ReactNode;
  /** Inert pill: dimmed, not-allowed cursor, reason shown as the tooltip. */
  disabledReason?: string | null;
  /** Which state the pill announces: a toggle, a menu trigger, or neither. */
  aria?: 'pressed' | 'expanded' | 'none';
  gap?: number;
  className?: string;
  labelClassName?: string;
  buttonRef?: Ref<HTMLButtonElement>;
  /** Geometry-only render for the hidden measure row — same element and
   *  styles, no handlers, tooltip, or aria state, and out of the tab order. */
  measureOnly?: boolean;
}) {
  const blocked = !!disabledReason;
  // Active = accent only (amber icon + label), no fill or border ring — the
  // pill silhouette stays identical in both states; hover feedback is the
  // same transient fill either way.
  const style: CSSProperties = {
    gap: `${gap}px`,
    padding: '6px 10px',
    fontSize: '0.8125rem',
    fontWeight: 500,
    background: 'transparent',
    color: blocked
      ? 'var(--color-text-quaternary, #9ca3af)'
      : active ? 'var(--color-accent-light)' : 'var(--color-text-muted, #8b8fa3)',
    border: '1px solid transparent',
    opacity: blocked ? 0.6 : 1,
    cursor: blocked ? 'not-allowed' : 'pointer',
    transition: 'background 0.2s, color 0.2s, border-color 0.2s',
  };
  const body = (
    <>
      <Icon className="h-4 w-4 flex-none" />
      <span className={labelClassName}>{label}</span>
      {trailing}
    </>
  );

  if (measureOnly) {
    return (
      <button
        type="button"
        tabIndex={-1}
        className={cn('inline-flex items-center rounded-full border-none whitespace-nowrap', className)}
        style={style}
      >
        {body}
      </button>
    );
  }

  const ariaProps: ButtonHTMLAttributes<HTMLButtonElement> =
    aria === 'pressed' ? { 'aria-pressed': active }
      : aria === 'expanded' ? { 'aria-haspopup': 'menu', 'aria-expanded': active }
        : {};

  return (
    <button
      ref={buttonRef}
      className={cn('inline-flex items-center rounded-full border-none whitespace-nowrap', className)}
      style={style}
      onClick={(e) => { e.stopPropagation(); if (!blocked) onToggle?.(); }}
      onMouseEnter={(e) => { if (!blocked) e.currentTarget.style.background = 'var(--color-border-muted)'; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
      type="button"
      title={disabledReason || title}
      aria-disabled={blocked || undefined}
      {...ariaProps}
    >
      {body}
    </button>
  );
}

/* --- @MENTION / /SLASH AUTOCOMPLETE LISTS ---
 * Presentation only — the matching, selection and keyboard machinery lives in
 * chat-input.useMentions / chat-input.useSlashCommands. */

export function MentionAutocompleteList({
  listRef,
  files,
  activeIndex,
  hasWorkspaceFiles,
  onSelect,
  onHover,
}: {
  listRef: Ref<HTMLDivElement>;
  files: string[];
  activeIndex: number;
  hasWorkspaceFiles: boolean;
  onSelect: (path: string) => void;
  onHover: (idx: number) => void;
}) {
  return (
    <div className="mention-autocomplete" ref={listRef}>
      {files.length === 0 ? (
        <div className="mention-autocomplete-empty">
          {hasWorkspaceFiles ? 'No matching files' : 'No files available'}
        </div>
      ) : (
        files.map((filePath, idx) => {
          const name = filePath.split('/').pop();
          const dir = filePath.includes('/') ? filePath.slice(0, filePath.lastIndexOf('/')) : '';
          return (
            <div
              key={filePath}
              className={`mention-autocomplete-item ${idx === activeIndex ? 'active' : ''}`}
              onMouseDown={(e) => { e.preventDefault(); onSelect(filePath); }}
              onMouseEnter={() => onHover(idx)}
            >
              <FileText className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="file-name">{name}</span>
              {dir && <span className="file-path">{dir}/</span>}
            </div>
          );
        })
      )}
    </div>
  );
}

export function SlashCommandList({
  listRef,
  commands,
  activeIndex,
  onSelect,
  onHover,
}: {
  listRef: Ref<HTMLDivElement>;
  commands: SlashCommand[];
  activeIndex: number;
  onSelect: (cmd: SlashCommand) => void;
  onHover: (idx: number) => void;
}) {
  const { t } = useTranslation();
  return (
    <div className="mention-autocomplete" ref={listRef}>
      {commands.length === 0 ? (
        <div className="mention-autocomplete-empty">{t('chat.slashCommand.noMatching')}</div>
      ) : (
        commands.map((cmd, idx) => (
          <div
            key={cmd.name}
            className={`mention-autocomplete-item ${idx === activeIndex ? 'active' : ''}`}
            onMouseDown={(e) => { e.preventDefault(); onSelect(cmd); }}
            onMouseEnter={() => onHover(idx)}
          >
            {getSlashCommandIcon(cmd, 'h-4 w-4 flex-shrink-0 slash-cmd-icon')}
            <span className="slash-cmd-name">/{cmd.name}</span>
            <span className="slash-cmd-desc">{cmd.description}</span>
          </div>
        ))
      )}
    </div>
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
