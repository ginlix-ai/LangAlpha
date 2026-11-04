import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { AnimatePresence, motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { cn } from '@/lib/utils';
import type { ArtifactComment } from '@/types/api';
import { useCommentLook } from './commentLayerContext';
import { firstWords, oneLine } from '../../../artifactComments/commentModel';
import { PinFace } from '../../../artifactComments/PinBadge';
import { PIN_SIZE, type PlacedPin } from './pinPlaces';
import '../../../artifactComments/PinBadge.css';

// A pin arriving on a document fades in; one the owner just made pops out of
// the draft's spot; one that goes shrinks a little as it fades.
const PIN_APPEAR = { opacity: 0 };
const PIN_ARRIVE = { opacity: 0, scale: 0.6 };
const PIN_SHOWN = { opacity: 1, scale: 1, transition: TWEEN.quick };
const PIN_GONE = { opacity: 0, scale: 0.8, transition: TWEEN.exit };
// The draft's "+" hands over to the new pin in place, so it only fades.
const DRAFT_PIN_GONE = { opacity: 0, transition: TWEEN.exit };
const PIN_ORIGIN = { originX: 0, originY: 1 } as const;

interface CommentPinsProps {
  pins: readonly PlacedPin[];
  /** The comment whose card is open, over its pin or in the list. */
  shownId: string | null;
  /** The number of the draft whose "+" is drawn, null for none. */
  draftSerial: number | null;
  /** Comments a reply just landed on while the page was open. */
  pulse: ReadonlySet<string>;
  /** The comment the owner just made here, which pops out of the draft's spot. */
  madeId: string | null;
  size: { width: number; height: number };
  /** Moves when the pointer goes onto the page, which a pin never hears leave it. */
  pointerIn: number;
  /** Out while the document under them is being replaced. */
  hidden: boolean;
  scrolling: boolean;
  /** A pin's preview opened: ask the page to say when the pointer is back over it. */
  onPreview: () => void;
  onPin: (comment: ArtifactComment) => void;
}

/** The pins over the page, the draft's "+" among them, arriving and leaving as one presence. */
export function CommentPins({
  pins, shownId, draftSerial, pulse, madeId, size, pointerIn, hidden, scrolling, onPreview, onPin,
}: CommentPinsProps): React.ReactElement {
  return (
    <div className="page-comment-pins" data-hidden={hidden || undefined} data-scrolling={scrolling || undefined}>
      <TooltipProvider delayDuration={300} disableHoverableContent>
        <AnimatePresence initial={false}>
          {pins.map(({ comment, x, y }) => (comment ? (
            <CommentPin
              key={comment.comment_id}
              comment={comment}
              x={x}
              y={y}
              open={shownId === comment.comment_id}
              pulse={pulse.has(comment.comment_id)}
              made={madeId === comment.comment_id}
              offView={y < 0 || y - PIN_SIZE > size.height || x + PIN_SIZE < 0 || x > size.width}
              pointerIn={pointerIn}
              onPreview={onPreview}
              onClick={() => onPin(comment)}
            />
          ) : draftSerial !== null && <DraftPin key={`draft:${draftSerial}`} x={x} y={y} />))}
        </AnimatePresence>
      </TooltipProvider>
    </div>
  );
}

/**
 * The draft's "+", in its box's place. It keeps the box's key, so it stays
 * put as the box goes into the list.
 */
function DraftPin({ x, y }: { x: number; y: number }): React.ReactElement {
  return (
    <motion.span
      className="page-comment-pin-slot"
      data-open
      style={{ left: x, top: y - PIN_SIZE, ...PIN_ORIGIN }}
      initial={PIN_ARRIVE}
      animate={PIN_SHOWN}
      exit={DRAFT_PIN_GONE}
      aria-hidden
    >
      <span className="page-comment-pin is-open" data-pin-state="draft">
        <span className="page-comment-pin-disc">
          <PinFace number={null} size={11} />
        </span>
      </span>
    </motion.span>
  );
}

interface CommentPinProps {
  comment: ArtifactComment;
  x: number;
  y: number;
  open: boolean;
  /** A reply just landed while the page was open. */
  pulse: boolean;
  /** The owner just made it here, out of the draft's spot. */
  made: boolean;
  /** Scrolled out of the visible part of the page: no tab stop, as focus there lands on nothing to see. */
  offView: boolean;
  pointerIn: number;
  onPreview: () => void;
  onClick: () => void;
}

/**
 * A comment's pin: a speech bubble whose tail is the spot, so the words it
 * is about stay readable. The bubble arrives and leaves through the layer's
 * presence; the button inside presses and changes state on its own beat.
 * Resting on it shows the comment's first words. The number gives way to the
 * live glyph while the agent is on it, and an agent message the owner has not
 * opened wears the dot until its card opens.
 */
function CommentPin({
  comment, x, y, open, pulse, made, offView, pointerIn, onPreview, onClick,
}: CommentPinProps): React.ReactElement {
  const { t } = useTranslation();
  const { state, working } = useCommentLook(comment);
  const unread = comment.unread > 0;
  const preview = oneLine(firstWords(comment), 80);
  const [previewing, setPreviewing] = useState(false);
  const [pointerWas, setPointerWas] = useState(pointerIn);
  if (pointerIn !== pointerWas) {
    setPointerWas(pointerIn);
    setPreviewing(false);
  }
  return (
    <motion.span
      className="page-comment-pin-slot"
      data-open={open || undefined}
      style={{ left: x, top: y - PIN_SIZE, ...PIN_ORIGIN }}
      initial={made ? PIN_ARRIVE : PIN_APPEAR}
      animate={PIN_SHOWN}
      exit={PIN_GONE}
    >
      {/* Anew each time the pointer goes onto the page: the trigger never
          heard it leave, and would wait for that before opening again. */}
      <Tooltip
        key={pointerWas}
        open={previewing && !open && !!preview}
        onOpenChange={(next) => {
          setPreviewing(next && !open);
          if (next && !open) onPreview();
        }}
      >
        <TooltipTrigger asChild>
          <button
            type="button"
            className={cn('page-comment-pin press', open && 'is-open')}
            data-comment-pin=""
            data-pin-state={state}
            data-working={working || undefined}
            data-unread={unread || undefined}
            data-pulse={pulse || undefined}
            aria-label={t(unread ? 'pageViewer.comments.pinLabelUnread' : 'pageViewer.comments.pinLabel', { number: comment.number })}
            aria-expanded={open}
            tabIndex={offView ? -1 : undefined}
            onClick={onClick}
          >
            <span className="page-comment-pin-disc">
              <PinFace number={comment.number} working={working} label={t('pageViewer.comments.working')} size={11} />
            </span>
          </button>
        </TooltipTrigger>
        <TooltipContent side="top" align="start" className="max-w-64 truncate">
          {t('pageViewer.comments.pinPreview', { number: comment.number, words: preview })}
        </TooltipContent>
      </Tooltip>
    </motion.span>
  );
}
