import { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLatestRef } from '@/hooks/useLatestRef';
import type { CellBox } from '@/pages/ChatAgent/utils/a1';
import { ViewerComments, type ViewerCommentsTarget } from '../ViewerComments';
import { cellSpots, type SheetView } from './cellSpots';

const noTab = () => null;
const noReveal = () => {};

/**
 * The owner's comments on a sheet's cells, in a workbook or a CSV, over the
 * grid's scroll viewport. Drawn beside the viewport, in a parent exactly its
 * size, which the layer covers.
 */
export function SheetComments({ grid, view, tabOf = noTab, reveal = noReveal, target }: {
  /** The grid's scroll viewport, holding the table. */
  grid: HTMLElement | null;
  /** The sheet drawn, null while there is none. */
  view: SheetView | null;
  /** The tab another sheet is shown by; a CSV has none. */
  tabOf?: (sheet: string) => Element | null;
  /** Draws that sheet with the range in view. */
  reveal?: (sheet: string, box: CellBox) => void;
  target: ViewerCommentsTarget;
}) {
  const { t } = useTranslation();
  // Where the ranges' tokens are kept (`cellSpots`): outside the grid, so making one is not a change to it.
  const [holder, setHolder] = useState<HTMLDivElement | null>(null);
  // Read at each call rather than rebuilt with them: a new strategy would start the comment page over.
  const latest = useLatestRef({ view, tabOf, reveal });
  const strategy = useMemo(() => (grid && holder ? cellSpots(grid, holder, {
    view: () => latest.current.view,
    tabOf: (sheet) => latest.current.tabOf(sheet),
    reveal: (sheet, box) => latest.current.reveal(sheet, box),
  }) : null), [grid, holder, latest]);
  // In comment mode (the page marks the grid with its cursor) a press is a
  // pick, and must not take the focus: the grid can hold it, and the box of
  // the comment the pick adds to closes when the focus leaves it.
  useEffect(() => {
    if (!grid) return;
    const keepFocus = (e: MouseEvent) => {
      if (grid.dataset.commentCursor) e.preventDefault();
    };
    grid.addEventListener('mousedown', keepFocus, true);
    return () => grid.removeEventListener('mousedown', keepFocus, true);
  }, [grid]);
  return (
    <>
      <div ref={setHolder} hidden />
      <ViewerComments
        scroller={grid}
        content={grid}
        strategy={strategy}
        target={target}
        modeHint={t('pageViewer.comments.modeHintCells')}
      />
    </>
  );
}
