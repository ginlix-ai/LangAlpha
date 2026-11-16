import React, { useMemo, useState } from 'react';
import type { ViewerCommentsTarget } from './comments/ViewerComments';
import { SheetComments } from './comments/sheet/SheetComments';
import type { SheetView } from './comments/sheet/cellSpots';
import './ExcelViewer.css'; // reuse the spreadsheet table styles

const MAX_PREVIEW_ROWS = 500;

/** A record and its number in the file, which a workbook opening it gives its row. */
interface CsvRow {
  n: number;
  cells: string[];
}

/**
 * Simple RFC 4180-compliant CSV parser. Blank records are left out but still
 * counted, so each row keeps the number a workbook opening the file gives it.
 */
function parseCsv(text: string): CsvRow[] {
  const rows: CsvRow[] = [];
  let row: string[] = [];
  let cell = '';
  let inQuote = false;
  let n = 0;
  const end = () => {
    row.push(cell);
    cell = '';
    n += 1;
    if (row.some((c) => c !== '')) rows.push({ n, cells: row });
    row = [];
  };

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (inQuote) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          cell += '"';
          i++;
        } else {
          inQuote = false;
        }
      } else {
        cell += ch;
      }
    } else if (ch === '"') {
      inQuote = true;
    } else if (ch === ',') {
      row.push(cell);
      cell = '';
    } else if (ch === '\n') {
      end();
    } else if (ch === '\r') {
      // handle \r\n or lone \r
      if (text[i + 1] === '\n') continue;
      end();
    } else {
      cell += ch;
    }
  }
  // last row
  if (cell || row.length > 0) end();
  return rows;
}

/** A number as a CSV writes one: what a cell holds that cannot name its row or column. */
const NUMBER = /^[-+(]?[$€£¥]?\s*[\d.,]+%?\)?$/;

interface CsvViewerProps {
  content: string;
  /** The owner's comments on the cells, in the owner's panel; left out, the file shows alone. */
  comments?: ViewerCommentsTarget | null;
}

export default function CsvViewer({ content, comments = null }: CsvViewerProps) {
  const rows = useMemo(() => parseCsv(content || ''), [content]);
  const [grid, setGrid] = useState<HTMLDivElement | null>(null);

  // The cells as a sheet addresses them, by the rows a workbook opening the
  // file gives them, blank records counted: the header is row 1, the next row 2.
  const view = useMemo<SheetView>(() => {
    const shown = new Map(rows.slice(0, MAX_PREVIEW_ROWS + 1).map((row) => [row.n, row.cells]));
    const text = (r: number, c: number) => shown.get(r)?.[c - 1] ?? '';
    return {
      sheet: null,
      sheets: [],
      rows: rows[Math.min(rows.length, MAX_PREVIEW_ROWS + 1) - 1]?.n ?? 0,
      cols: rows[0]?.cells.length ?? 0,
      text,
      isText: (r, c) => {
        const value = text(r, c).trim();
        return value !== '' && !NUMBER.test(value);
      },
    };
  }, [rows]);

  const headerRow = rows[0];
  const colCount = headerRow?.cells.length ?? 0;
  const dataRows = rows.slice(1, MAX_PREVIEW_ROWS + 1);
  const truncated = rows.length - 1 > MAX_PREVIEW_ROWS;

  // Cells carry their A1 place (`data-r`, `data-c`) and each row's number
  // cell its row alone, which is how the comment layer reads the grid. The
  // grid's box stays through an empty file, so its comments stay listed.
  return (
    <div className="excel-viewer">
      <div className="excel-grid-host">
        <div className={`excel-table-wrapper${headerRow ? '' : ' is-empty'}`} ref={setGrid}>
          {headerRow ? (
            <table className="excel-table">
              <thead>
                <tr>
                  <th className="excel-row-num" data-r={headerRow.n}>{headerRow.n}</th>
                  {headerRow.cells.map((cell, ci) => (
                    <th key={ci} data-r={headerRow.n} data-c={ci + 1}>{cell}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dataRows.map((row) => (
                  <tr key={row.n}>
                    <td className="excel-row-num" data-r={row.n}>{row.n}</td>
                    {Array.from({ length: colCount }, (_, ci) => (
                      <td key={ci} data-r={row.n} data-c={ci + 1}>{row.cells[ci] ?? ''}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="excel-viewer-empty">No data</div>
          )}
        </div>
        {comments && <SheetComments grid={grid} view={view} target={comments} />}
      </div>

      {truncated && (
        <div className="excel-truncated rule-t open-style:control-voice">
          Showing first {MAX_PREVIEW_ROWS} of {rows.length - 1} rows
        </div>
      )}
    </div>
  );
}
