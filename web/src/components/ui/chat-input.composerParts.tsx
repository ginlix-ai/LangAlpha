import { Fragment, useRef, useState, type CSSProperties, type Ref, type WheelEvent } from 'react';
import {
  Archive, ArrowUp, ChartCandlestick, ClipboardList, Mic, MicOff, MoreHorizontal, Quote, Square, TextSelect, X,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  DropdownMenu, DropdownMenuContent, DropdownMenuSeparator, DropdownMenuTrigger,
} from './dropdown-menu';
import { Loader } from './loader';
import { AnimatePresence, motion } from '@/lib/framer';
import { SwapCell } from '@/components/ui/crossfade';
import { cn } from '@/lib/utils';
import { CHIP_PRESENCE, mentionPillKeys } from './chat-input.helpers';
import { escapeToken } from './chat-input.quoteTokens';
import { QuoteCard } from './chat-input.quoteCard';
import { FilePreviewCard } from './chat-input.parts';
import type { ToolbarItem } from './chat-input.useToolbarFold';
import type { ChipContext, FileAttachment, QuoteMention } from './chat-input.types';

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


/** The context a chip stands for, by what made it. */
function chipIcon(f: ChipContext) {
  const Icon = f.source === 'paste' ? ClipboardList
    : f.source === 'chart' ? ChartCandlestick
    : TextSelect;
  return <Icon className="h-3.5 w-3.5 shrink-0" />;
}

/**
 * What goes out with the draft without being a word of it: long pastes, chart
 * views, sheet ranges. They sit in the line the draft starts on, because they
 * read as part of the message, so they are direct children of the row around the
 * textarea.
 */
export function ComposerChips({
  chips,
  onRemove,
}: {
  chips: ChipContext[];
  onRemove: (chip: ChipContext) => void;
}) {
  const { t } = useTranslation();
  const keys = mentionPillKeys(chips);
  return (
    <AnimatePresence initial={false}>
      {chips.map((f, idx) => (
        <QuoteCard
          key={`chip::${keys[idx]}`}
          side="top"
          quote={f.snippet}
          source={f.path ? f.label : undefined}
        >
          <motion.span {...CHIP_PRESENCE} className="composer-quote-chip">
            {chipIcon(f)}
            <span>{f.label}</span>
            <button
              type="button"
              className="composer-quote-remove"
              onClick={(e) => { e.stopPropagation(); onRemove(f); }}
              title={t('chat.composer.remove')}
              aria-label={t('chat.composer.remove')}
            >
              <X className="h-2.5 w-2.5" />
            </button>
          </motion.span>
        </QuoteCard>
      ))}
    </AnimatePresence>
  );
}

/**
 * The copy of the draft a token's tint is drawn on (`.draft-token-layer`). With
 * a quote in the draft it draws the words too, since only this copy can show one
 * as its chip: an icon, in the accent. That token is also the one part of the
 * draft a pointer can tell apart, so it takes the hover; a press leaves the caret
 * at its end, as a token is whole.
 */
export function DraftTokenLayer({
  text,
  tokens,
  quotes,
  layerRef,
  className,
  onQuotePress,
  onWheel,
}: {
  text: string;
  tokens: string[];
  quotes: QuoteMention[];
  layerRef: Ref<HTMLDivElement>;
  className?: string;
  /** Gets where the pressed token ends in `text`. */
  onQuotePress: (end: number) => void;
  onWheel?: (e: WheelEvent<HTMLDivElement>) => void;
}) {
  // A `/skill` or `@file` is one only as a word of its own. A quote's marks
  // already bound it, so text typed hard against one leaves it a token.
  const matchers = [
    ...(tokens.length ? [`(?<=^|\\s)(?:${tokens.map(escapeToken).join('|')})(?=\\s|$)`] : []),
    ...quotes.map((f) => escapeToken(f.token)),
  ];
  const pieces = matchers.length ? text.split(new RegExp(`(${matchers.join('|')})`)) : [text];
  return (
    <div ref={layerRef} aria-hidden data-shows-text={quotes.length > 0 || undefined} className={cn('draft-token-layer', className)} onWheel={onWheel}>
      {pieces.map((piece, i) => {
        if (!(i % 2)) return piece;
        const quote = quotes.find((f) => f.token === piece);
        if (!quote) return <span key={i} className="draft-token">{piece}</span>;
        return (
          <QuoteCard
            // Pasted, or given back beside itself, a token can be in the draft twice.
            key={pieces.indexOf(piece) === i ? piece : `${piece}:${i}`}
            side="top"
            quote={quote.snippet}
            reply={quote.reply}
            source={quote.from.kind === 'file' ? quote.from.label : undefined}
          >
            <span
              className="draft-token-quote"
              onMouseDown={(e) => { e.preventDefault(); onQuotePress(pieces.slice(0, i + 1).join('').length); }}
            >
              <Quote />
              {piece}
            </span>
          </QuoteCard>
        );
      })}
      {/* A last empty line takes no height in a block as it does in a textarea. */}
      {text.endsWith('\n') && '\u200b'}
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
