import React, { useCallback, useEffect, useEffectEvent, useId, useMemo, useRef, useState } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { FunctionSquare } from 'lucide-react';
import { integer } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import {
  boxOf,
  columnName,
  formatLocator,
  formatRange,
  isWholeColumns,
  isWholeRows,
  MAX_COL,
  MAX_ROW,
  parseLocator,
  type CellBox,
  type CellRef,
} from '@/pages/ChatAgent/utils/a1';
import type { ContextPayload } from '../filePanel/types';
import { parseWorkbook, withEquals, type GridCell, type SheetData } from './excel/parse';
import { GridRow } from './excel/GridRow';
import { precedentsOf } from './excel/precedents';
import { buildRangeSnippet, buildWholeSnippet, withinSheet } from './excel/snippet';
import './ExcelViewer.css';

/** Rows a Page Up / Page Down step covers. */
const PAGE_ROWS = 20;

/** Arrow and page keys, as (rows, cols) deltas from the active cell. */
const STEP: Record<string, [number, number]> = {
  ArrowUp: [-1, 0],
  ArrowDown: [1, 0],
  ArrowLeft: [0, -1],
  ArrowRight: [0, 1],
  PageUp: [-PAGE_ROWS, 0],
  PageDown: [PAGE_ROWS, 0],
};


/** Shared by every row no precedent reaches, so those rows keep one identity. */
const NO_BOXES: readonly CellBox[] = [];

interface Selection {
  anchor: CellRef;
  focus: CellRef;
  /**
   * The box a locator writes. A header click selects every cell of its
   * columns or rows however far the sheet grows, so that box runs to the
   * workbook's limit (`MAX_ROW` / `MAX_COL`), as `B:B` means.
   */
  box: CellBox;
}

/** The cells the grid paints: a whole axis stops at the sheet's edge. */
function clampBox(box: CellBox, sheet: SheetData): CellBox {
  return {
    top: box.top,
    left: box.left,
    bottom: Math.min(box.bottom, Math.max(1, sheet.rows.length)),
    right: Math.min(box.right, Math.max(1, sheet.colCount)),
  };
}

function isWhole(box: CellBox): boolean {
  return isWholeColumns(box) || isWholeRows(box);
}

/** A link's box read back into a selection: `B:D` selects columns, `4:9` rows. */
function selectionFor(box: CellBox): Selection {
  if (isWholeColumns(box)) return { anchor: { row: 1, col: box.right }, focus: { row: 1, col: box.left }, box };
  if (isWholeRows(box)) return { anchor: { row: box.bottom, col: 1 }, focus: { row: box.top, col: 1 }, box };
  // Anchor at the far corner so the outline lands on the cell the link named.
  return { anchor: { row: box.bottom, col: box.right }, focus: { row: box.top, col: box.left }, box };
}

const ORIGIN: Selection = { anchor: { row: 1, col: 1 }, focus: { row: 1, col: 1 }, box: boxOf({ row: 1, col: 1 }, { row: 1, col: 1 }) };

export interface ExcelViewerProps {
  data: ArrayBuffer;
  /** Workspace path of the open file, stamped onto the context payload. */
  filePath?: string;
  /**
   * `Model!B7`, `Model!B4:D9` or `B7`: switches to that sheet, selects the
   * range and scrolls it into view. This is what a `#Model!B7` link opens.
   */
  focusCell?: string | null;
  /** Bumped to replay the same `focusCell`, the way PdfViewer replays a page. */
  focusSeq?: number;
  onAddContext?: (payload: ContextPayload) => void;
  /** The page around the viewer offers this file as a download, so a note can send the reader there. */
  canDownload?: boolean;
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

function cellAt(sheet: SheetData, ref: CellRef): GridCell | undefined {
  return sheet.rows[ref.row - 1]?.[ref.col - 1];
}

/** Selecting a cell a merge covers selects the merge, the way Excel does. */
function resolveMaster(sheet: SheetData, ref: CellRef): CellRef {
  const cell = cellAt(sheet, ref);
  return cell?.master ? { row: cell.master.row, col: cell.master.col } : ref;
}

/**
 * A locator's box under the same merge rule a click follows. Only a single
 * cell moves: a range or a whole axis already holds the master it covers, and
 * growing it to swallow the rest of a merge would cite cells nobody named.
 */
function resolveBox(sheet: SheetData, box: CellBox): CellBox {
  if (box.top !== box.bottom || box.left !== box.right) return box;
  const master = resolveMaster(sheet, { row: box.top, col: box.left });
  return boxOf(master, master);
}

export default function ExcelViewer({ data, filePath, focusCell, focusSeq, onAddContext, canDownload = false }: ExcelViewerProps) {
  // Cell text is formatted once in parseWorkbook, in the locale of that
  // moment, and re-parsed only when `data` changes; a language switch reaches
  // the chrome around the grid, not the cells, so it cannot reset the sheet
  // and selection the reader is on.
  const { t } = useTranslation();
  const locale = useLocale();
  const parseLocale = useEffectEvent(() => locale);
  const [sheets, setSheets] = useState<SheetData[] | null>(null);
  const [parseError, setParseError] = useState<Error | null>(null);
  const [activeSheet, setActiveSheet] = useState(0);
  const [sel, setSel] = useState<Selection | null>(null);
  const [showFormulas, setShowFormulas] = useState(false);
  // The chip is an offer, not a status line: the locator of a selection the
  // user made, and nothing once they have added it.
  const [offer, setOffer] = useState<string | null>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const focusCellRef = useRef<HTMLTableCellElement>(null);
  // Only the active cell carries an id, so one per viewer is enough for
  // `aria-activedescendant`; useId keeps two open workbooks apart.
  const focusId = `${useId()}focus`;

  useEffect(() => {
    let cancelled = false;
    // A rewritten file arrives as new bytes on the same instance, so the grid
    // and any offer built on the old bytes go before the parse, not after.
    setSheets(null);
    setSel(null);
    setOffer(null);
    setParseError(null);
    parseWorkbook(data, parseLocale())
      .then((result) => {
        if (cancelled) return;
        setSheets(result);
        setActiveSheet(0);
        setSel(ORIGIN);
        setOffer(null);
      })
      .catch((err: Error) => {
        console.error('[ExcelViewer] Failed to parse workbook:', err);
        if (!cancelled) setParseError(err);
      });
    return () => { cancelled = true; };
  }, [data]);

  // Null before the parse lands, and for a workbook with no sheets at all.
  const sheet = sheets && sheets.length > 0 ? (sheets[activeSheet] ?? sheets[0]) : null;

  useEffect(() => {
    if (!focusCell || !sheets) return;
    const target = parseLocator(focusCell);
    if (!target) return;
    const index = target.sheet
      ? sheets.findIndex((s) => s.name.toLowerCase() === target.sheet!.toLowerCase())
      : activeSheet;
    // A sheet the workbook does not have is a dead link: selecting its cells
    // on whatever sheet is showing would offer a citation nothing wrote.
    if (target.sheet && index < 0) {
      setSel(null);
      setOffer(null);
      return;
    }
    const linked = sheets[index] ?? sheets[0];
    const box = resolveBox(linked, target.box);
    setActiveSheet(index);
    setSel(selectionFor(box));
    setOffer(formatLocator(linked.name, box));
    // `activeSheet` is the fallback for an unqualified locator, not a trigger.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusCell, focusSeq, sheets]);

  useEffect(() => {
    focusCellRef.current?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  }, [activeSheet, sel?.focus.row, sel?.focus.col]);

  const active = sheet && sel ? cellAt(sheet, sel.focus) : undefined;
  const selBox = sel && sheet ? clampBox(sel.box, sheet) : null;
  // A locator wholly past the parsed extent selects nothing the reader can
  // see and its snippet would be blank rows, so there is nothing to offer. One
  // partly inside is offered, and the snippet clips itself and says so.
  const offerable = offer !== null && sel !== null && sheet !== null && withinSheet(sel.box, sheet) !== null;
  const precedents = useMemo(
    () => precedentsOf(active?.formula, sheet?.name ?? ''),
    [active?.formula, sheet?.name],
  );
  // Precedent boxes bucketed by the rows they reach, built once per active
  // formula: a row with none keeps `NO_BOXES` across every selection change,
  // and a whole-column reference stops at the parsed extent.
  const rowPrecedents = useMemo(() => {
    const byRow = new Map<number, CellBox[]>();
    const last = sheet?.rows.length ?? 0;
    for (const box of precedents.local) {
      for (let r = Math.max(1, box.top); r <= Math.min(box.bottom, last); r++) {
        const list = byRow.get(r);
        if (list) list.push(box);
        else byRow.set(r, [box]);
      }
    }
    return byRow;
  }, [precedents, sheet]);

  const commit = useCallback((next: Selection, sheetName: string) => {
    setSel(next);
    setOffer(formatLocator(sheetName, next.box));
  }, []);

  const select = useCallback((row: number, col: number, extend: boolean) => {
    if (!sheet) return;
    const target = resolveMaster(sheet, { row, col });
    const anchor = extend && sel ? sel.anchor : target;
    commit({ anchor, focus: target, box: boxOf(anchor, target) }, sheet.name);
  }, [sheet, sel, commit]);

  /** A header click: the whole column or row, extended along its axis on shift. */
  const selectWhole = useCallback((axis: 'cols' | 'rows', index: number, extend: boolean) => {
    if (!sheet) return;
    // The active cell stays on its other axis, as in Excel: selecting row 5
    // from B5 leaves B5 active, so a following Ctrl+Space means column B.
    const keep = sel?.focus ?? ORIGIN.focus;
    const at = axis === 'cols' ? { row: keep.row, col: index } : { row: index, col: keep.col };
    const sameAxis = sel && (axis === 'cols' ? isWholeColumns(sel.box) : isWholeRows(sel.box));
    const anchor = extend && sameAxis ? sel.anchor : at;
    const box = axis === 'cols'
      ? { top: 1, bottom: MAX_ROW, left: Math.min(anchor.col, at.col), right: Math.max(anchor.col, at.col) }
      : { left: 1, right: MAX_COL, top: Math.min(anchor.row, at.row), bottom: Math.max(anchor.row, at.row) };
    commit({ anchor, focus: at, box }, sheet.name);
  }, [sheet, sel, commit]);

  const handleKeyDown = useCallback((e: React.KeyboardEvent<HTMLDivElement>) => {
    if (!sheet) return;
    const maxRow = sheet.rows.length;
    const maxCol = sheet.colCount;
    const at = sel?.focus ?? ORIGIN.focus;

    if (e.key === 'Escape') {
      // A selection is what this Escape dismisses. With none it is the
      // panel's, whose own Escape hands the focus back to the tree row.
      if (sel) e.stopPropagation();
      setSel(null);
      setOffer(null);
      return;
    }
    // Excel's own whole-axis shortcuts: Ctrl+Space a column, Shift+Space a row.
    if (e.key === ' ' && (e.ctrlKey || e.metaKey || e.shiftKey)) {
      e.preventDefault();
      const rows = e.shiftKey && !(e.ctrlKey || e.metaKey);
      selectWhole(rows ? 'rows' : 'cols', rows ? at.row : at.col, false);
      return;
    }
    // Excel's own show-formulas shortcut.
    if ((e.metaKey || e.ctrlKey) && e.key === '`') {
      e.preventDefault();
      setShowFormulas((v) => !v);
      return;
    }

    const move = STEP[e.key];
    if (move) {
      e.preventDefault();
      select(clamp(at.row + move[0], 1, maxRow), clamp(at.col + move[1], 1, maxCol), e.shiftKey);
      return;
    }
    if (e.key === 'Home') {
      e.preventDefault();
      select(e.ctrlKey || e.metaKey ? 1 : at.row, 1, e.shiftKey);
      return;
    }
    if (e.key === 'End') {
      e.preventDefault();
      select(e.ctrlKey || e.metaKey ? maxRow : at.row, maxCol, e.shiftKey);
    }
  }, [sheet, sel, select, selectWhole]);

  /**
   * One handler for every click in the table. A cell carries `data-r` and
   * `data-c`, a row header only `data-r`, a column header only `data-c`, so
   * rows need no closures of their own and `memo` can skip them.
   */
  const handleTableMouseDown = useCallback((e: React.MouseEvent<HTMLTableElement>) => {
    const target = (e.target as HTMLElement).closest<HTMLElement>('[data-r], [data-c]');
    if (!target) return;
    e.preventDefault();
    gridRef.current?.focus();
    const { r, c } = target.dataset;
    if (r && c) select(Number(r), Number(c), e.shiftKey);
    else if (c) selectWhole('cols', Number(c), e.shiftKey);
    else if (r) selectWhole('rows', Number(r), e.shiftKey);
  }, [select, selectWhole]);

  /** Enter or Space on a focused header selects its column or row, as a click does. */
  const handleTableKeyDown = useCallback((e: React.KeyboardEvent<HTMLTableElement>) => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const header = (e.target as HTMLElement).closest<HTMLElement>('th[data-r], th[data-c]');
    if (!header) return;
    e.preventDefault();
    e.stopPropagation();
    const { r, c } = header.dataset;
    if (c) selectWhole('cols', Number(c), e.shiftKey);
    else if (r) selectWhole('rows', Number(r), e.shiftKey);
  }, [selectWhole]);

  const handleAddContext = useCallback(() => {
    if (!sheet || !sel || !offer || !onAddContext) return;
    // A whole row or column is a pointer, not a payload: the agent gets one
    // line saying which one, and reads the cells itself if it needs them.
    const snippet = isWhole(sel.box) ? buildWholeSnippet(sheet, sel.box) : buildRangeSnippet(sheet, sel.box);
    const name = filePath ? filePath.split('/').pop() || filePath : sheet.name;
    onAddContext({ path: filePath, locator: offer, label: `${name}#${offer}`, snippet });
    setOffer(null);
  }, [sheet, sel, offer, onAddContext, filePath]);

  // Bubble to the error boundary the panel wraps this in.
  if (parseError) throw parseError;

  if (!sheets) {
    return <div className="excel-viewer-empty">{t('excelViewer.parsing')}</div>;
  }
  if (!sheet) {
    throw new Error('No sheets found in workbook');
  }
  // An empty sheet keeps the chrome around it: the sheet tabs are how you leave.
  const isEmpty = sheet.rows.length === 0 || sheet.colCount === 0;
  const columns = Array.from({ length: sheet.colCount }, (_, i) => i + 1);
  const notes: string[] = [];
  if (sheet.totalRows > sheet.rows.length) {
    notes.push(t('excelViewer.rowsShown', { shown: integer(sheet.rows.length, locale), total: integer(sheet.totalRows, locale) }));
  }
  if (sheet.totalCols > sheet.colCount) {
    notes.push(t('excelViewer.colsShown', { shown: integer(sheet.colCount, locale), total: integer(sheet.totalCols, locale) }));
  }
  if (sheet.uncalculated > 0) {
    notes.push(t('excelViewer.uncalculated', { count: sheet.uncalculated, n: integer(sheet.uncalculated, locale) }));
  }
  // The grid draws cells only. Said nowhere, a chart the agent made reads as
  // one it never made.
  if (sheet.charts + sheet.pictures > 0) {
    const charts = sheet.charts > 0 ? t('excelViewer.charts', { count: sheet.charts, n: integer(sheet.charts, locale) }) : '';
    const pictures = sheet.pictures > 0 ? t('excelViewer.pictures', { count: sheet.pictures, n: integer(sheet.pictures, locale) }) : '';
    const items = charts && pictures ? t('excelViewer.chartsAndPictures', { charts, pictures }) : charts || pictures;
    const count = sheet.charts + sheet.pictures;
    notes.push(canDownload ? t('excelViewer.notShown', { count, items }) : t('excelViewer.notShownNoDownload', { items }));
  }
  const note = notes.join(' · ');

  return (
    <div className="excel-viewer clips-focus-ring">
      {!isEmpty && (
        <div className="excel-fbar">
          <div className="excel-namebox" title={t('excelViewer.selection')}>{sel ? formatRange(sel.box) : ''}</div>
          <div className="excel-fx">
            {active?.formula ? (
              <>
                <span className="excel-fx-tag">fx</span>
                <span className="excel-fx-formula">{withEquals(active.formula)}</span>
                <span className="excel-fx-value">
                  {active.error ? (
                    <b className="excel-fx-error">{active.error}</b>
                  ) : active.calculated ? (
                    <>= <b>{active.text}</b></>
                  ) : (
                    <span className="excel-fx-muted">{t('excelViewer.notCalculated')}</span>
                  )}
                  <span className="excel-fx-fmt"> · {active.numFmt || t('excelViewer.generalFormat')}</span>
                  {precedents.external.length > 0 && (
                    <span className="excel-fx-fmt"> · {precedents.external.join(', ')}</span>
                  )}
                </span>
              </>
            ) : active && active.text ? (
              <>
                <span className="excel-fx-formula">{active.text}</span>
                <span className="excel-fx-value">
                  {active.isText ? t('excelViewer.labelCell') : t('excelViewer.inputCell')}
                  {active.numFmt && <span className="excel-fx-fmt"> · {active.numFmt}</span>}
                </span>
              </>
            ) : (
              <span className="excel-fx-muted">{t('excelViewer.emptyCell')}</span>
            )}
          </div>
          <button
            type="button"
            className={`excel-fx-toggle${showFormulas ? ' is-on' : ''}`}
            onClick={() => setShowFormulas((v) => !v)}
            aria-pressed={showFormulas}
            title={t('excelViewer.showFormulas')}
          >
            <FunctionSquare className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      {isEmpty ? (
        <div className="excel-viewer-empty">{t('excelViewer.emptySheet')}</div>
      ) : (
        <div
          className="excel-table-wrapper"
          ref={gridRef}
          role="grid"
          tabIndex={0}
          onKeyDown={handleKeyDown}
          aria-label={t('excelViewer.cellsOf', { sheet: sheet.name })}
          aria-activedescendant={sel ? focusId : undefined}
        >
          <table className="excel-table excel-grid" onMouseDown={handleTableMouseDown} onKeyDown={handleTableKeyDown}>
            <thead>
              <tr role="row">
                <th className="excel-row-num excel-corner" aria-label={t('excelViewer.rowHeader')} />
                {columns.map((c) => (
                  <th
                    key={c}
                    role="columnheader"
                    tabIndex={-1}
                    data-c={c}
                    className={selBox && c >= selBox.left && c <= selBox.right ? 'is-active' : undefined}
                    title={t('excelViewer.selectColumn', { name: columnName(c) })}
                  >
                    {columnName(c)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {sheet.rows.map((cells, ri) => {
                const r = ri + 1;
                const crosses = selBox !== null && r >= selBox.top && r <= selBox.bottom;
                return (
                  <GridRow
                    key={r}
                    r={r}
                    cells={cells}
                    showFormulas={showFormulas}
                    selLeft={crosses ? selBox.left : 0}
                    selRight={crosses ? selBox.right : 0}
                    precedents={rowPrecedents.get(r) ?? NO_BOXES}
                    focusCol={sel && sel.focus.row === r ? sel.focus.col : null}
                    focusId={focusId}
                    focusRef={focusCellRef}
                    t={t}
                  />
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {onAddContext && !isEmpty && offerable && (
        <div className="excel-selchip">
          <span>
            <Trans i18nKey="excelViewer.addOffer" values={{ locator: offer }} components={{ code: <code /> }} />
          </span>
          <button type="button" onClick={handleAddContext} title={t('context.addToContext')}>
            {t('excelViewer.add')}
          </button>
        </div>
      )}

      <div className="excel-footer">
        {sheets.length > 1 && (
          <div className="excel-sheet-tabs">
            {sheets.map((s, idx) => (
              <button
                type="button"
                key={s.name}
                className={`excel-sheet-tab${idx === activeSheet ? ' active' : ''}`}
                onClick={() => {
                  setActiveSheet(idx);
                  setSel(ORIGIN);
                  setOffer(null);
                }}
              >
                {s.name}
              </button>
            ))}
          </div>
        )}
        {note && <div className="excel-footer-note">{note}</div>}
      </div>
    </div>
  );
}
