import type React from 'react';
import { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { KEY_STEP, LIST_WIDTH_DEFAULT, LIST_WIDTH_MAX, LIST_WIDTH_MIN } from '../listWidth';

/** On `<body>` while a drag runs: its cursor everywhere, and frames kept from taking the moves. */
const RESIZING = 'drive-resizing';

/**
 * The list's width as drawn, read off the edge's place (Drive.css sets it 4px
 * left of the column's edge). It can be less than the width kept, which gives
 * way on a narrow window, and a move starts from what the person sees.
 */
function drawnWidth(edge: HTMLElement): number {
  return edge.offsetLeft + 4;
}

/**
 * The edge between the list and the open file: dragged, or moved with the
 * arrow keys, it sets the list's width; a double click puts it back. A drag
 * holds the pointer, so its moves reach the edge wherever the pointer goes.
 */
export function ListDivider({ width, onChange }: { width: number; onChange: (width: number, commit?: boolean) => void }) {
  const { t } = useTranslation();
  const drag = useRef<{ x: number; width: number } | null>(null);

  // A file closed mid-drag takes the edge with it; the page's cursor goes too.
  useEffect(() => () => document.body.classList.remove(RESIZING), []);

  const widthAt = (e: React.PointerEvent) => (drag.current ? drag.current.width + (e.clientX - drag.current.x) : width);
  const end = (e: React.PointerEvent<HTMLDivElement>) => {
    if (!drag.current) return;
    onChange(widthAt(e), true);
    drag.current = null;
    document.body.classList.remove(RESIZING);
  };

  return (
    <div
      className="drive-divider"
      role="separator"
      aria-orientation="vertical"
      aria-label={t('drive:resizeList')}
      aria-valuenow={width}
      aria-valuemin={LIST_WIDTH_MIN}
      aria-valuemax={LIST_WIDTH_MAX}
      title={t('drive:resizeList')}
      tabIndex={0}
      onPointerDown={(e) => {
        if (e.button !== 0) return;
        e.preventDefault();
        e.currentTarget.setPointerCapture(e.pointerId);
        drag.current = { x: e.clientX, width: drawnWidth(e.currentTarget) };
        document.body.classList.add(RESIZING);
      }}
      onPointerMove={(e) => {
        if (drag.current) onChange(widthAt(e));
      }}
      onPointerUp={end}
      onPointerCancel={end}
      onLostPointerCapture={end}
      onDoubleClick={() => onChange(LIST_WIDTH_DEFAULT, true)}
      onKeyDown={(e) => {
        if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
          e.preventDefault();
          onChange(drawnWidth(e.currentTarget) + (e.key === 'ArrowRight' ? KEY_STEP : -KEY_STEP), true);
        } else if (e.key === 'Home' || e.key === 'Enter') {
          e.preventDefault();
          onChange(LIST_WIDTH_DEFAULT, true);
        }
      }}
    />
  );
}
