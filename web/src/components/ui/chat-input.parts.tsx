import type { ButtonHTMLAttributes, CSSProperties, ReactNode, Ref } from 'react';
import { FileText, X } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { useIsMobile } from '@/hooks/useIsMobile';
import { motion, useIsPresent } from '@/lib/framer';
import { cn } from '@/lib/utils';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import { WidgetContextDeck } from '@/pages/Dashboard/widgets/framework/WidgetContextDeck';
import { CHIP_PRESENCE, MENU_PRESENCE, getSlashCommandIcon } from './chat-input.helpers';
import { SUBAGENT_CORE, SUBAGENT_SEATS } from './subagents-rings.geometry';
import type { FileAttachment, SlashCommand } from './chat-input.types';

const formatFileSize = (bytes: number): string => {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

/* --- FILE PREVIEW CARD ---
 * Its root is a motion element: rendered as a child of an AnimatePresence, it
 * plays CHIP_PRESENCE on the way in and on the way out. */
export const FilePreviewCard = ({ file, onRemove }: { file: FileAttachment; onRemove: (id: string) => void }) => {
  const { t } = useTranslation();
  const isMobilePreview = useIsMobile();
  const isImage = file.type.startsWith('image/') && file.preview;

  return (
    <motion.div
      {...CHIP_PRESENCE}
      className="relative group shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-(--color-border-muted) bg-(--color-bg-elevated) transition-colors hover:border-(--color-border-default)"
    >
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
        type="button"
        onClick={() => onRemove(file.id)}
        aria-label={t('chat.composer.remove')}
        className={`absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white transition-opacity ${isMobilePreview ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
      >
        <X className="w-3 h-3" />
      </button>

      {/* Upload Status */}
      {file.uploadStatus === 'uploading' && (
        <div className="absolute inset-0 bg-black/40 flex items-center justify-center">
          <Loader size={20} label={t('chat.composer.uploading')} className="text-white" />
        </div>
      )}
    </motion.div>
  );
};

/**
 * The single composer pill: every labeled control in the action bar (scope,
 * mode, workspace) is one of these, so pill geometry exists once and the
 * hidden measure row can render the very same component.
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
  /** Which state the pill announces: a toggle, a picker trigger, or neither. */
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
        className={cn('inline-flex items-center rounded-lg border-none whitespace-nowrap transition-colors', className)}
        style={style}
      >
        {body}
      </button>
    );
  }

  const opensPicker = aria === 'expanded';
  const ariaProps: ButtonHTMLAttributes<HTMLButtonElement> =
    aria === 'pressed' ? { 'aria-pressed': active }
      : opensPicker ? { 'aria-haspopup': 'dialog', 'aria-expanded': active }
        : {};

  return (
    <button
      ref={buttonRef}
      className={cn('inline-flex items-center rounded-lg border-none whitespace-nowrap transition-colors', className)}
      style={style}
      onClick={(e) => { e.stopPropagation(); if (!blocked) onToggle?.(); }}
      // A press leaves focus where it was, as a menu trigger's does: the picker
      // hands focus back on close to whatever held it at open, which should
      // be the draft, and a second press reaches the pill as a toggle rather
      // than as a blur that has already closed the picker.
      onMouseDown={opensPicker ? (e) => e.preventDefault() : undefined}
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

/**
 * The agent with three seats around it. On, an agent leaves the core for each
 * seat and fills it; off, they return and the seats stay as empty rings, so the
 * state reads from the shape and not from color alone. Seats take a lighter
 * stroke than the core, which keeps them secondary and still leaves an empty one
 * a visible hole at 16px. The figure is centered on its box rather than on the
 * core, to sit level with the lucide icons beside it (24 box, 2 stroke).
 */
const CORE = SUBAGENT_CORE;
const SEATS = SUBAGENT_SEATS;

export function SubagentsGlyph({ on, className, style }: { on: boolean; className?: string; style?: CSSProperties }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={cn('subagents-glyph', className)}
      style={style}
      data-on={on}
      aria-hidden="true"
    >
      <circle cx={CORE.x} cy={CORE.y} r="3.5" />
      {SEATS.map(([x, y]) => <circle key={`seat-${x}`} cx={x} cy={y} r="2.25" strokeWidth={1.5} />)}
      {/* A filled disc the size of a seat's outer edge, so a landed agent
          covers its ring exactly. It starts on the core, small and clear. */}
      {SEATS.map(([x, y], i) => (
        <circle
          key={x}
          className="subagents-glyph-node"
          cx={x}
          cy={y}
          r="3"
          stroke="none"
          fill="currentColor"
          style={{
            '--dx': `${CORE.x - x}px`,
            '--dy': `${CORE.y - y}px`,
            '--seat': i,
          } as CSSProperties}
        />
      ))}
    </svg>
  );
}

/**
 * A composer toggle with no label: the glyph carries the state and a tooltip
 * names the control. Sized and colored like a PillToggle, so it sits in the
 * same row and the hidden measure row renders the very same element.
 */
export function IconToggle({
  active,
  onToggle,
  label,
  tooltip,
  children,
  measureOnly = false,
}: {
  active: boolean;
  onToggle: () => void;
  /** The control's name, for assistive tech. */
  label: string;
  /** What hovering says, including the current state. */
  tooltip: string;
  children: ReactNode;
  measureOnly?: boolean;
}) {
  const className = 'inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg border-none bg-transparent transition-colors';
  const style: CSSProperties = {
    color: active ? 'var(--color-accent-light)' : 'var(--color-text-muted)',
  };
  if (measureOnly) {
    return <button type="button" tabIndex={-1} className={className} style={style}>{children}</button>;
  }
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className={cn(className, 'hover:bg-(--color-border-muted) cursor-pointer')}
            style={style}
            onClick={(e) => { e.stopPropagation(); onToggle(); }}
            aria-label={label}
            aria-pressed={active}
          >
            {children}
          </button>
        </TooltipTrigger>
        <TooltipContent side="top">{tooltip}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/* --- @MENTION / /SLASH AUTOCOMPLETE LISTS ---
 * Presentation only — the matching, selection and keyboard machinery lives in
 * chat-input.useMentions / chat-input.useSlashCommands. Each root is the
 * motion element an AnimatePresence around it opens and closes; the menu is
 * absolutely placed, so the motion stays on the element itself, where a
 * wrapper would take a gap in the composer's column and, while it moved, be
 * the box the menu is placed against. A closing menu is inert: its rows still
 * hold the draft as it was, and a second press on one would splice into the
 * draft the first press already changed. */

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
  const { t } = useTranslation();
  const present = useIsPresent();
  return (
    <motion.div className="mention-autocomplete" ref={listRef} inert={!present} {...MENU_PRESENCE}>
      {files.length === 0 ? (
        <div className="mention-autocomplete-empty">
          {hasWorkspaceFiles ? t('chat.mention.noMatching') : t('chat.mention.noFiles')}
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
              <FileText className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="file-name">{name}</span>
              {dir && <span className="file-path">{dir}/</span>}
            </div>
          );
        })
      )}
    </motion.div>
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
  const present = useIsPresent();
  return (
    <motion.div className="mention-autocomplete" ref={listRef} inert={!present} {...MENU_PRESENCE}>
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
            {getSlashCommandIcon(cmd, 'h-4 w-4 shrink-0 slash-cmd-icon')}
            <span className="slash-cmd-name">/{cmd.name}</span>
            <span className="slash-cmd-desc">{cmd.description}</span>
          </div>
        ))
      )}
    </motion.div>
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
