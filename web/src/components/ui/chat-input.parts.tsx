import { useState } from 'react';
import type { ButtonHTMLAttributes, CSSProperties, ReactNode, Ref } from 'react';
import { FileText, X } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { Loader } from '@/components/ui/loader';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { useObjectUrl } from '@/hooks/useObjectUrl';
import { formatBytes } from '@/lib/format';
import { motion, useIsPresent } from '@/lib/framer';
import { cn } from '@/lib/utils';
import ImageLightbox from '@/pages/ChatAgent/components/ImageLightbox';
import { isImageFile } from '@/pages/ChatAgent/utils/fileUpload';
import type { UploadFailure } from '@/pages/ChatAgent/utils/partUpload';
import type { WidgetContextSnapshot } from '@/pages/Dashboard/widgets/framework/contextSnapshot';
import { WidgetContextDeck } from '@/pages/Dashboard/widgets/framework/WidgetContextDeck';
import { CHIP_PRESENCE, MENU_PRESENCE, getSlashCommandIcon } from './chat-input.helpers';
import { SUBAGENT_CORE, SUBAGENT_SEATS } from './subagents-rings.geometry';
import type { FileAttachment, SlashCommand } from './chat-input.types';

/** A ring of `percent` around nothing, over an uploading image's thumbnail. */
function ProgressRing({ percent }: { percent: number }) {
  const r = 11;
  const c = 2 * Math.PI * r;
  return (
    <svg width={28} height={28} viewBox="0 0 28 28" aria-hidden className="-rotate-90">
      <circle cx={14} cy={14} r={r} fill="none" strokeWidth={2.5} className="stroke-white/30" />
      <circle
        cx={14}
        cy={14}
        r={r}
        fill="none"
        strokeWidth={2.5}
        strokeLinecap="round"
        strokeDasharray={c}
        strokeDashoffset={c * (1 - percent / 100)}
        className="stroke-white transition-[stroke-dashoffset] duration-quick ease-out"
      />
    </svg>
  );
}

/* --- FILE TILES ---
 * A file uploads from the moment it is added, and its tile shows how far it
 * has gone: a ring over an image, a thin bar under any other file's name, and
 * once failed, why, with a way to send it again. Each tile's root is a motion
 * element: rendered as a child of an AnimatePresence, it plays CHIP_PRESENCE
 * on the way in and on the way out. */

const FAILURE_KEYS = {
  send: 'chat.composer.attachment.sendFailed',
  tooLarge: 'chat.composer.attachment.tooLarge',
  refused: 'chat.composer.attachment.failed',
  unknown: 'chat.composer.attachment.failed',
} as const satisfies Record<UploadFailure['kind'], string>;

/** A failure in the current language; the server's own words when it gave them. */
function failureText(t: TFunction, failure: UploadFailure): string {
  return (failure.kind === 'refused' && failure.message) || t(FAILURE_KEYS[failure.kind]);
}

type TileProps = { file: FileAttachment; onRemove: (id: string) => void; onRetry: (id: string) => void };

/** The upload's state, over an image's thumbnail or inline under another file's name. */
function UploadState({ file, variant, onRetry }: Omit<TileProps, 'onRemove'> & { variant: 'overlay' | 'inline' }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const size = formatBytes(file.file.size, locale);
  const overlay = variant === 'overlay';
  const caption = (text: string, saving = false) => (
    <p
      className={cn(
        'flex items-center gap-1 text-[0.625rem] tabular-nums',
        overlay ? 'text-white/90' : 'text-(--color-text-tertiary)',
      )}
      title={file.file.name}
    >
      <span className="truncate">{text}</span>
      {saving && !overlay && <Loader size={10} label={t('chat.composer.attachment.saving')} style={{ color: 'inherit' }} />}
    </p>
  );
  const strip = (children: ReactNode) => (
    <div className="absolute inset-x-0 bottom-0 bg-linear-to-t from-black/70 to-transparent px-2 pb-1.5 pt-4">{children}</div>
  );
  const { upload } = file;

  switch (upload.status) {
    case 'uploading': {
      const { percent } = upload;
      const meta = percent === null ? size : `${size} · ${percent}%`;
      if (overlay) {
        return (
          <>
            <div className="absolute inset-0 flex items-center justify-center text-white">
              {percent === null
                ? <Loader size={16} label={t('chat.composer.attachment.saving')} className="text-white" />
                : <ProgressRing percent={percent} />}
            </div>
            {strip(caption(meta))}
          </>
        );
      }
      return (
        <>
          {caption(meta, percent === null)}
          <div className="h-0.5 w-full overflow-hidden rounded-full bg-(--color-border-muted)" aria-hidden>
            <div
              className="h-full rounded-full bg-(--color-text-tertiary) transition-[width] duration-quick ease-out"
              style={{ width: `${percent ?? 100}%` }}
            />
          </div>
        </>
      );
    }
    case 'ready':
      return overlay ? strip(caption(size)) : caption(size);
    case 'failed': {
      const reason = failureText(t, upload.failure);
      const failure = (
        <div className="relative flex min-w-0 flex-col items-start gap-0.5">
          <TooltipProvider delayDuration={300}>
            <Tooltip>
              <TooltipTrigger asChild>
                <p
                  tabIndex={0}
                  className={cn('w-full truncate text-[0.625rem] leading-tight', overlay ? 'text-white' : 'text-(--color-loss)')}
                >
                  {reason}
                </p>
              </TooltipTrigger>
              <TooltipContent side="top" className="max-w-64">{reason}</TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <button
            type="button"
            onClick={() => onRetry(file.id)}
            className={cn(
              'tap-target rounded text-[0.625rem] font-medium underline-offset-2 hover:underline',
              overlay ? 'text-white' : 'text-(--color-text-primary)',
            )}
          >
            {t('chat.composer.attachment.retry')}
          </button>
        </div>
      );
      return overlay ? strip(failure) : failure;
    }
  }
}

/** The tile around either kind of file: its name for assistive tech, the failed border, and remove. */
function TileShell({ file, onRemove, children }: Omit<TileProps, 'onRetry'> & { children: ReactNode }) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const { upload } = file;
  const name = file.file.name;
  const label = upload.status === 'uploading'
    ? t('chat.composer.attachment.uploadingLabel', { name, percent: upload.percent ?? 100 })
    : upload.status === 'failed'
      ? t('chat.composer.attachment.failedLabel', { name, error: failureText(t, upload.failure) })
      : name;
  const failed = upload.status === 'failed';
  return (
    <motion.div
      {...CHIP_PRESENCE}
      role="group"
      aria-label={label}
      data-status={upload.status}
      className={cn(
        'relative group shrink-0 w-24 h-24 rounded-xl overflow-hidden border bg-(--color-bg-elevated) transition-colors',
        failed ? 'border-(--color-border-loss)' : 'border-(--color-border-muted) hover:border-(--color-border-default)',
      )}
    >
      {children}
      <button
        type="button"
        onClick={() => onRemove(file.id)}
        aria-label={t('chat.composer.remove')}
        // The tile clips what reaches past its corner, so the finger's box
        // moves in by that much and keeps its whole 44px inside the tile.
        className={cn(
          'tap-target [--tap-target-x:-8px] [--tap-target-y:8px] absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white transition-opacity',
          isMobile || failed ? 'opacity-60' : 'opacity-0 group-hover:opacity-100 focus-visible:opacity-100',
        )}
      >
        <X className="w-3 h-3" />
      </button>
    </motion.div>
  );
}

/**
 * An image's tile. The thumbnail opens the lightbox; the strip along the
 * bottom (size, or why it failed and Retry) sits beside that target rather
 * than over it, so Retry never zooms.
 */
function ImageTile({ file, onRemove, onRetry }: TileProps) {
  const { t } = useTranslation();
  const src = useObjectUrl(file.file);
  const [previewOpen, setPreviewOpen] = useState(false);
  const busy = file.upload.status !== 'ready';
  return (
    <TileShell file={file} onRemove={onRemove}>
      {src && <img src={src} alt={file.file.name} className="w-full h-full object-cover" />}
      <div
        aria-hidden
        className={cn('absolute inset-0 transition-colors', busy ? 'bg-black/45' : 'bg-black/20 group-hover:bg-black/0')}
      />
      <UploadState file={file} variant="overlay" onRetry={onRetry} />
      <button
        type="button"
        onClick={() => setPreviewOpen(true)}
        aria-label={t('chat.composer.attachment.preview', { name: file.file.name })}
        className="absolute inset-x-0 top-0 bottom-10 cursor-zoom-in"
      />
      {src && <ImageLightbox src={src} alt={file.file.name} open={previewOpen} onClose={() => setPreviewOpen(false)} />}
    </TileShell>
  );
}

/** Any other file's tile: its extension, its name, and the upload's state under it. */
function DocTile({ file, onRemove, onRetry }: TileProps) {
  return (
    <TileShell file={file} onRemove={onRemove}>
      <div className="w-full h-full p-3 flex flex-col justify-between">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded" style={{ background: 'var(--color-border-muted)' }}>
            <FileText className="w-4 h-4" style={{ color: 'var(--color-text-tertiary)' }} />
          </div>
          <span className="text-[0.625rem] font-medium uppercase tracking-wider truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {file.file.name.split('.').pop()}
          </span>
        </div>
        <div className="space-y-0.5 min-w-0">
          <p className="text-xs font-medium truncate" style={{ color: 'var(--color-text-muted)' }} title={file.file.name}>
            {file.file.name}
          </p>
          <UploadState file={file} variant="inline" onRetry={onRetry} />
        </div>
      </div>
    </TileShell>
  );
}

export function FileTile(props: TileProps) {
  return isImageFile({ type: props.file.type, name: props.file.file.name }) ? <ImageTile {...props} /> : <DocTile {...props} />;
}

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
    <motion.div data-slot="menu" className="mention-autocomplete" ref={listRef} inert={!present} {...MENU_PRESENCE}>
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
              data-slot="menu-item"
              className={cn('mention-autocomplete-item', idx === activeIndex && 'bg-accent/15')}
              onMouseDown={(e) => { e.preventDefault(); onSelect(filePath); }}
              onMouseEnter={() => onHover(idx)}
            >
              <FileText className="h-4 w-4 shrink-0 text-(--color-icon-muted)" />
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
    <motion.div data-slot="menu" className="mention-autocomplete" ref={listRef} inert={!present} {...MENU_PRESENCE}>
      {commands.length === 0 ? (
        <div className="mention-autocomplete-empty">{t('chat.slashCommand.noMatching')}</div>
      ) : (
        commands.map((cmd, idx) => (
          <div
            key={cmd.name}
            data-slot="menu-item"
            className={cn('mention-autocomplete-item', idx === activeIndex && 'bg-accent/15')}
            onMouseDown={(e) => { e.preventDefault(); onSelect(cmd); }}
            onMouseEnter={() => onHover(idx)}
          >
            {getSlashCommandIcon(cmd, 'h-4 w-4 shrink-0 text-(--color-icon-muted)')}
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
                data-bracket="word"
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
