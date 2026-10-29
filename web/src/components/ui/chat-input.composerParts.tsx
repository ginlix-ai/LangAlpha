import { Fragment, useRef, useState, type CSSProperties } from 'react';
import {
  Archive, ArrowUp, ChartCandlestick, ClipboardList, FileText, Mic, MicOff, MoreHorizontal, Square, TextSelect, X,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuSeparator, DropdownMenuTrigger,
} from './dropdown-menu';
import { Loader } from './loader';
import { AnimatePresence, motion } from '@/lib/framer';
import { SwapCell } from '@/components/ui/crossfade';
import { cn } from '@/lib/utils';
import { CHIP_PRESENCE, getSlashCommandIcon, mentionPillKeys } from './chat-input.helpers';
import { FilePreviewCard } from './chat-input.parts';
import type { ToolbarItem } from './chat-input.useToolbarFold';
import type { FileAttachment, MentionedFile, SlashCommand } from './chat-input.types';

/* The composer box's own pieces: the draft's pills and attachments, the
 * overlays that cover it, and the buttons of its action bar. Each takes the
 * values and callbacks it draws from, so the composer's state stays in
 * ChatInput and these only present it. */

/** The listening waveform's bars, each a step behind the last (`--bar`). */
const WAVEFORM_BARS = [0, 1, 2, 3, 4, 5, 6, 7];

/** How long after a press sends that the same button, now Stop, ignores the
 *  next: about an OS double-click interval, so the second click of a
 *  double-clicked send, or a second Enter on the still-focused button, doesn't
 *  cancel the turn it just started. */
const STOP_ARM_MS = 500;

/**
 * Slash command pills + mention pills. The row stays mounted so a pill can
 * leave as it arrived; empty, it takes no room.
 */
export function ComposerPills({
  slashCommands,
  onRemoveCommand,
  mentionedFiles,
  onRemoveMention,
}: {
  slashCommands: SlashCommand[];
  onRemoveCommand: (name: string) => void;
  mentionedFiles: MentionedFile[];
  onRemoveMention: (path: string, label?: string, snippet?: string) => void;
}) {
  const { t } = useTranslation();
  const mentionKeys = mentionPillKeys(mentionedFiles);
  return (
    <div className="mention-pills">
      <AnimatePresence initial={false}>
        {slashCommands.map((cmd) => (
          <motion.div
            key={`slash-${cmd.name}`}
            {...CHIP_PRESENCE}
            className="mention-pill mention-pill-slash"
            title={cmd.description}
          >
            {getSlashCommandIcon(cmd, "h-3 w-3 shrink-0 mention-pill-icon")}
            <span>/{cmd.name}</span>
            <button
              className="mention-pill-remove"
              onClick={(e) => { e.stopPropagation(); onRemoveCommand(cmd.name); }}
              title={t('chat.composer.remove')}
            >
              <X className="h-2.5 w-2.5" />
            </button>
          </motion.div>
        ))}
        {mentionedFiles.map((f, idx) => {
          const isSnippet = !!f.snippet;
          const isPaste = f.source === 'paste';
          const isChart = f.source === 'chart';
          const name = isSnippet ? f.label : f.path.split('/').pop();
          // Snippet tooltips preview, not mirror: a condensed paste can
          // be tens of KB and title= renders it all.
          const tooltip = isSnippet
            ? (f.snippet!.length > 500 ? f.snippet!.slice(0, 500) + '…' : f.snippet)
            : f.path;
          return (
            <motion.div
              key={`mention::${mentionKeys[idx]}`}
              {...CHIP_PRESENCE}
              className={`mention-pill ${isSnippet ? 'mention-pill-snippet' : ''}`}
              title={tooltip}
            >
              {isPaste
                ? <ClipboardList className="h-3 w-3 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                : isChart
                  ? <ChartCandlestick className="h-3 w-3 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                : isSnippet
                  ? <TextSelect className="h-3 w-3 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                  : <FileText className="h-3 w-3 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              }
              <span>{name}</span>
              <button
                className="mention-pill-remove"
                onClick={(e) => { e.stopPropagation(); onRemoveMention(f.path, f.label, f.snippet); }}
                title={t('chat.composer.remove')}
              >
                <X className="h-2.5 w-2.5" />
              </button>
            </motion.div>
          );
        })}
      </AnimatePresence>
    </div>
  );
}

/**
 * Chart image + file preview cards, in a row that stays mounted for the same
 * reason as the pills'.
 */
export function ComposerAttachments({
  chartImage,
  onRemoveChartImage,
  files,
  onRemoveFile,
  isMobile,
}: {
  chartImage: string | null;
  onRemoveChartImage?: (() => void) | null;
  files: FileAttachment[];
  onRemoveFile: (id: string) => void;
  isMobile: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex gap-3 overflow-x-auto pb-2 px-1 empty:hidden">
      <AnimatePresence initial={false}>
        {chartImage && (
          <motion.div
            key="chart-image"
            {...CHIP_PRESENCE}
            className="relative group shrink-0 w-24 h-24 rounded-xl overflow-hidden border border-(--color-border-muted) bg-(--color-bg-elevated) transition-colors hover:border-(--color-border-default)"
          >
            <img src={chartImage} alt={t('chat.composer.chartAlt')} className="w-full h-full object-cover" />
            <button
              type="button"
              onClick={(e) => { e.stopPropagation(); onRemoveChartImage?.(); }}
              aria-label={t('chat.composer.remove')}
              className={`absolute top-1 right-1 p-1 bg-black/50 hover:bg-black/70 rounded-full text-white transition-opacity ${isMobile ? 'opacity-60' : 'opacity-0 group-hover:opacity-100'}`}
            >
              <X className="w-3 h-3" />
            </button>
          </motion.div>
        )}
        {files.map((file) => (
          <FilePreviewCard
            key={file.id}
            file={file}
            onRemove={onRemoveFile}
          />
        ))}
      </AnimatePresence>
    </div>
  );
}

/** The recording wash and the listening hint, over the composer box. */
export function VoiceOverlay() {
  const { t } = useTranslation();
  return (
    <>
      <div className="recording-overlay" />
      <div className="voice-status-hint">
        <div className="waveform">
          {WAVEFORM_BARS.map((bar) => (
            <div key={bar} className="waveform-bar" style={{ '--bar': bar } as CSSProperties} />
          ))}
        </div>
        <span className="animate-pulse">{t('chat.voice.listening', { defaultValue: 'Listening... (Type to cancel)' })}</span>
      </div>
    </>
  );
}

/**
 * Voice input's button. A turn hides it without unmounting it, so the model
 * pill beside it holds its place.
 */
export function VoiceButton({
  listening,
  hidden,
  disabled,
  onToggle,
}: {
  listening: boolean;
  hidden: boolean;
  disabled: boolean;
  onToggle: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div
      className={cn(
        'flex items-center transition-[opacity,visibility]',
        hidden && 'invisible opacity-0 pointer-events-none duration-exit',
      )}
      inert={hidden}
      aria-hidden={hidden || undefined}
    >
      <button
        onClick={(e) => { e.stopPropagation(); onToggle(); }}
        disabled={disabled}
        tabIndex={hidden ? -1 : undefined}
        className={cn(
          'press mic-button inline-flex flex-none items-center justify-center h-8 w-8 rounded-xl',
          listening ? 'recording' : 'text-(--color-icon-muted) hover:text-(--color-text-muted) hover:bg-foreground/5',
        )}
        type="button"
        title={listening ? t('chat.voice.stop') : t('chat.voice.start')}
        aria-label={listening ? t('chat.voice.stop') : t('chat.voice.start')}
      >
        {listening ? (
          <MicOff className="w-4 h-4" />
        ) : (
          <Mic className="w-4 h-4" />
        )}
      </button>
    </div>
  );
}

/**
 * Send / Stop: one button, whose glyph crossfades in place and whose fill
 * follows. Stop also shows during a compaction with no streaming turn (manual
 * /compact|/offload) so the user can interrupt it; the Enter-key send path
 * stays open for queuing.
 */
export function SendStopButton({
  running,
  canSend,
  onSend,
  onStop,
}: {
  /** A turn, or a compaction running without one, is in progress. */
  running: boolean;
  canSend: boolean;
  onSend: () => void;
  /** Without it a running turn still shows Send: there is nothing to stop with. */
  onStop?: (() => void) | null;
}) {
  const { t } = useTranslation();
  const [isStopping, setIsStopping] = useState(false);
  const sentAtRef = useRef(-Infinity);

  // Reset isStopping once both a running turn and a compaction have finished,
  // in the render that sees them finish.
  if (isStopping && !running) setIsStopping(false);

  const stopMode = running && !!onStop;
  const sendFace = stopMode ? (isStopping ? 'stopping' : 'stop') : 'send';
  const stopLabel = isStopping ? t('chat.stopping') : t('chat.stop');

  const handleStop = () => {
    if (isStopping) return;
    setIsStopping(true);
    onStop?.();
  };

  return (
    <button
      onClick={(e) => {
        e.stopPropagation();
        if (!stopMode) {
          sentAtRef.current = performance.now();
          onSend();
        } else if (performance.now() - sentAtRef.current >= STOP_ARM_MS) {
          handleStop();
        }
      }}
      disabled={stopMode ? isStopping : !canSend}
      data-mode={stopMode ? 'stop' : 'send'}
      className={cn(
        'send-button press grid place-items-center h-8 w-8 flex-none rounded-xl',
        stopMode ? 'disabled:cursor-not-allowed' : 'disabled:cursor-default',
      )}
      title={stopMode ? stopLabel : undefined}
      aria-label={stopMode ? stopLabel : t('chat.composer.send')}
      type="button"
    >
      <SwapCell swapKey={sendFace} scale layerClassName="flex items-center justify-center">
        {sendFace === 'send' ? (
          <ArrowUp className="w-4 h-4" />
        ) : sendFace === 'stop' ? (
          <Square className="h-4 w-4" fill="currentColor" />
        ) : (
          <Loader size={16} label={t('chat.stopping')} className="text-current" />
        )}
      </SwapCell>
    </button>
  );
}

/**
 * Drag overlay. Mounted throughout so it fades out as it fades in, and on the
 * composer's own surface: the accent never fills or washes a surface
 * (DESIGN.md, Accent discipline).
 */
export function DropOverlay({ active }: { active: boolean }) {
  const { t } = useTranslation();
  return (
    <div
      className={cn(
        'absolute inset-0 z-50 flex flex-col items-center justify-center rounded-[22px] border-2 border-dashed border-(--color-border-strong) bg-(--color-bg-card) pointer-events-none transition-[opacity,visibility]',
        active ? 'opacity-100' : 'invisible opacity-0 duration-exit',
      )}
    >
      <Archive className="w-10 h-10 mb-2 text-(--color-icon-muted)" />
      <p className="font-medium text-(--color-text-secondary)">{t('chat.composer.dropFiles')}</p>
    </div>
  );
}

/**
 * The ⋯ overflow: only the toggles that didn't fit inline, in priority order,
 * a separator wherever the group changes. The amber dot marks a folded toggle
 * that is on.
 */
export function ToolbarOverflowMenu({
  items,
  side,
  container,
}: {
  items: ToolbarItem[];
  side: 'top' | 'bottom';
  /** Where the menu portals to; unset portals to the body. */
  container?: HTMLElement | null;
}) {
  const { t } = useTranslation();
  return (
    <DropdownMenu modal={false}>
      <DropdownMenuTrigger asChild>
        <button
          className="relative inline-flex flex-none items-center justify-center h-8 w-8 rounded-lg transition-colors text-(--color-icon-muted) hover:text-(--color-text-muted) hover:bg-foreground/5 data-[state=open]:bg-foreground/5 data-[state=open]:text-(--color-text-muted)"
          onClick={(e) => e.stopPropagation()}
          type="button"
          title={t('chat.moreTools', { defaultValue: 'More tools' })}
          aria-label={t('chat.moreTools', { defaultValue: 'More tools' })}
        >
          <MoreHorizontal className="w-4 h-4" />
          {items.some((item) => item.active) && (
            <span
              className="absolute top-1 right-1 h-1.5 w-1.5 rounded-full"
              style={{ background: 'var(--color-accent-primary)' }}
            />
          )}
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        side={side}
        className="w-52"
        container={container}
      >
        {items.map((item, idx) => (
          <Fragment key={item.id}>
            {idx > 0 && items[idx - 1].group !== item.group && <DropdownMenuSeparator />}
            {item.menu()}
          </Fragment>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
