import React, { useLayoutEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { MessageSquare } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import type { DockStatus, PillOffset } from './useDockFloat';

/** The pill's height, which the composer folding into it aims at the middle of. */
export const PILL_HEIGHT = 32;
/** Past this many pixels a press on the pill is a drag, not a click. */
const DRAG_SLOP = 4;
/** The room kept between the pill and the file's edges. */
const EDGE = 8;
const KEY_STEP = 16;

/**
 * `next`, moved back inside `area`. The pill's rect less the offset it is
 * drawn at is where it sits at home. Null when either has no box, as in a
 * chat kept mounted out of sight: there is nothing to keep it inside then.
 */
function clampTo(area: HTMLElement, pill: HTMLElement, drawnAt: PillOffset, next: PillOffset): PillOffset | null {
  const bounds = area.getBoundingClientRect();
  const rect = pill.getBoundingClientRect();
  if (!bounds.width || !bounds.height || !rect.width || !rect.height) return null;
  const left = rect.left - drawnAt.x;
  const top = rect.top - drawnAt.y;
  const clamp = (v: number, lo: number, hi: number) => Math.max(lo, Math.min(v, hi));
  return {
    x: Math.round(clamp(next.x, bounds.left + EDGE - left, bounds.right - EDGE - rect.width - left)),
    y: Math.round(clamp(next.y, bounds.top + EDGE - top, bounds.bottom - EDGE - rect.height - top)),
  };
}

const paint = (pill: HTMLElement, offset: PillOffset) => {
  pill.style.translate = `${offset.x}px ${offset.y}px`;
};

const ARROWS: Record<string, PillOffset> = {
  ArrowLeft: { x: -1, y: 0 },
  ArrowRight: { x: 1, y: 0 },
  ArrowUp: { x: 0, y: -1 },
  ArrowDown: { x: 0, y: 1 },
};

/**
 * The folded composer: a pill that opens it again on a click, and moves
 * anywhere inside `area` on a drag or the arrow keys. A drag draws it straight
 * from the pointer (by `translate`, leaving `transform` to the pop it enters
 * with) and settles in the dock's state on release. From the press on, the
 * layer under it takes the pointer (dockFloat.css): a frame in the file
 * catches a move even from an element holding the capture.
 */
export function DockPill({ area, offset, takeFocus, onMove, onOpen, status, queued }: {
  /** The box it stays inside: the layer over the whole file. */
  area: HTMLElement | null;
  offset: PillOffset;
  /** Focused as it appears, taking over from the control that folded the composer. */
  takeFocus: boolean;
  onMove: (offset: PillOffset) => void;
  onOpen: () => void;
  status: DockStatus;
  queued: number;
}): React.ReactElement {
  const { t } = useTranslation();
  const ref = useRef<HTMLButtonElement>(null);
  // Where it is drawn, which runs ahead of `offset` while it is dragged.
  const drawn = useRef(offset);
  const press = useRef<{ id: number; x: number; y: number; from: PillOffset; moved: boolean } | null>(null);
  // The click a drag ends in is not a request to open.
  const dragEnded = useRef(false);

  const draw = (next: PillOffset) => {
    const pill = ref.current;
    const box = area;
    const clamped = pill && box ? clampTo(box, pill, drawn.current, next) : null;
    if (!pill || !clamped) return;
    drawn.current = clamped;
    paint(pill, clamped);
  };

  // Drawn where the dock's state says, which a drag only changes on release.
  useLayoutEffect(() => {
    if (!ref.current) return;
    drawn.current = offset;
    paint(ref.current, offset);
  }, [offset]);

  // Kept inside the file as its box resizes, and as the pill does with its
  // label and count, from the first measure on.
  useLayoutEffect(() => {
    const box = area;
    if (!box || !ref.current) return;
    const observer = new ResizeObserver(() => {
      const pill = ref.current;
      const fitted = pill ? clampTo(box, pill, drawn.current, drawn.current) : null;
      if (!pill || !fitted || (fitted.x === drawn.current.x && fitted.y === drawn.current.y)) return;
      drawn.current = fitted;
      paint(pill, fitted);
      onMove(fitted);
    });
    observer.observe(box);
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, [area, onMove]);

  const onPointerDown = (e: React.PointerEvent<HTMLButtonElement>) => {
    if (e.button !== 0) return;
    dragEnded.current = false;
    press.current = { id: e.pointerId, x: e.clientX, y: e.clientY, from: drawn.current, moved: false };
    e.currentTarget.setPointerCapture(e.pointerId);
    e.currentTarget.dataset.pressed = '';
  };
  const onPointerMove = (e: React.PointerEvent<HTMLButtonElement>) => {
    const p = press.current;
    if (!p || p.id !== e.pointerId) return;
    const dx = e.clientX - p.x;
    const dy = e.clientY - p.y;
    if (!p.moved && Math.hypot(dx, dy) < DRAG_SLOP) return;
    if (!p.moved) {
      p.moved = true;
      e.currentTarget.dataset.dragging = '';
    }
    draw({ x: p.from.x + dx, y: p.from.y + dy });
  };
  // On release, and on a capture lost without one, which would otherwise
  // leave the layer taking the pointer from the whole file.
  const endPress = (e: React.PointerEvent<HTMLButtonElement>) => {
    const p = press.current;
    if (!p || p.id !== e.pointerId) return;
    press.current = null;
    delete e.currentTarget.dataset.pressed;
    if (!p.moved) return;
    delete e.currentTarget.dataset.dragging;
    dragEnded.current = e.type === 'pointerup';
    onMove(drawn.current);
  };
  const onClick = () => {
    if (dragEnded.current) dragEnded.current = false;
    else onOpen();
  };
  const onKeyDown = (e: React.KeyboardEvent<HTMLButtonElement>) => {
    const dir = ARROWS[e.key];
    if (!dir) return;
    e.preventDefault();
    const step = e.shiftKey ? KEY_STEP * 4 : KEY_STEP;
    draw({ x: drawn.current.x + dir.x * step, y: drawn.current.y + dir.y * step });
    onMove(drawn.current);
  };

  return (
    <button
      ref={ref}
      type="button"
      className="composer-float-pill tap-target"
      style={{ height: PILL_HEIGHT }}
      title={t('chat.dockPillHint')}
      autoFocus={takeFocus}
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={endPress}
      onPointerCancel={endPress}
      onLostPointerCapture={endPress}
      onClick={onClick}
      onKeyDown={onKeyDown}
    >
      {status === 'running'
        ? <Loader size={12} className="text-(--color-accent-primary)" />
        : <MessageSquare className="h-3.5 w-3.5" aria-hidden="true" />}
      <span>{t(PILL_LABELS[status])}</span>
      {queued > 0 && <span className="composer-float-pill-count">{queued}</span>}
    </button>
  );
}

const PILL_LABELS = {
  idle: 'chat.dockRestore',
  running: 'chat.dockWorking',
  waiting: 'chat.dockWaiting',
} as const satisfies Record<DockStatus, string>;
