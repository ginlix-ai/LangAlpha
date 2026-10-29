import { memo, type Ref } from 'react';
import type { TFunction } from 'i18next';
import { columnName, type CellBox } from '@/pages/ChatAgent/utils/a1';
import { withEquals, type GridCell } from './parse';

export interface GridRowProps {
  /** 1-based row number, as the sheet addresses it. */
  r: number;
  cells: GridCell[];
  showFormulas: boolean;
  /**
   * The selection's columns where it crosses this row, 0/0 where it does not.
   * Numbers rather than a box so a row the selection never touched sees the
   * same props from one render to the next and bails out.
   */
  selLeft: number;
  selRight: number;
  /** Boxes of the active formula's precedents that reach this row. */
  precedents: readonly CellBox[];
  /** Column of the active cell when it sits on this row. */
  focusCol: number | null;
  /** The id the grid's `aria-activedescendant` points at. */
  focusId: string;
  focusRef: Ref<HTMLTableCellElement>;
  t: TFunction;
}

/**
 * One row of the spreadsheet grid. Kept free of hooks and of closures over
 * the viewer's state so `memo` can skip it: clicks are read off `data-r` /
 * `data-c` by one handler on the table.
 */
export function GridRowView({ r, cells, showFormulas, selLeft, selRight, precedents, focusCol, focusId, focusRef, t }: GridRowProps) {
  return (
    <tr role="row">
      <th
        scope="row"
        role="rowheader"
        tabIndex={-1}
        data-r={r}
        className={`excel-row-num${selLeft > 0 ? ' is-active' : ''}`}
        title={t('excelViewer.selectRow', { n: r })}
      >
        {r}
      </th>
      {cells.map((cell, ci) => {
        const c = ci + 1;
        if (cell.master) return null;
        const isFocus = focusCol === c;
        const selected = selLeft > 0 && c >= selLeft && c <= selRight;
        const classes: string[] = [];
        if (cell.isText) classes.push('is-text');
        if (cell.kind === 'formula') classes.push('is-formula');
        if (!cell.calculated) classes.push('is-uncalculated');
        if (cell.error) classes.push('is-error');
        if (precedents.length > 0 && precedents.some((b) => c >= b.left && c <= b.right)) classes.push('is-precedent');
        if (selected) classes.push('is-selected');
        if (isFocus) classes.push('is-focus');
        return (
          <td
            key={c}
            role="gridcell"
            id={isFocus ? focusId : undefined}
            ref={isFocus ? focusRef : undefined}
            className={classes.join(' ') || undefined}
            aria-selected={selected || undefined}
            data-r={r}
            data-c={c}
            data-ref={`${columnName(c)}${r}`}
            style={cell.style}
            colSpan={cell.colSpan}
            rowSpan={cell.rowSpan}
            title={!cell.calculated ? t('excelViewer.noCachedValue') : cell.hyperlink}
          >
            {showFormulas && cell.formula ? withEquals(cell.formula) : cell.text}
          </td>
        );
      })}
    </tr>
  );
}

export const GridRow = memo(GridRowView);
