import React, { useMemo } from 'react';
import './ExcelViewer.css'; // reuse the spreadsheet table styles

const MAX_PREVIEW_ROWS = 500;

/** Simple RFC 4180-compliant CSV parser */
function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = '';
  let inQuote = false;

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
      row.push(cell);
      cell = '';
      if (row.some((c) => c !== '')) rows.push(row);
      row = [];
    } else if (ch === '\r') {
      // handle \r\n or lone \r
      if (text[i + 1] === '\n') continue;
      row.push(cell);
      cell = '';
      if (row.some((c) => c !== '')) rows.push(row);
      row = [];
    } else {
      cell += ch;
    }
  }
  // last row
  if (cell || row.length > 0) {
    row.push(cell);
    if (row.some((c) => c !== '')) rows.push(row);
  }
  return rows;
}

interface CsvViewerProps {
  content: string;
}

export default function CsvViewer({ content }: CsvViewerProps) {
  const rows = useMemo(() => parseCsv(content || ''), [content]);

  if (rows.length === 0) {
    return <div className="excel-viewer-empty">No data</div>;
  }

  const headerRow = rows[0];
  const colCount = headerRow.length;
  const dataRows = rows.slice(1, MAX_PREVIEW_ROWS + 1);
  const truncated = rows.length - 1 > MAX_PREVIEW_ROWS;

  return (
    <div className="excel-viewer">
      <div className="excel-table-wrapper">
        <table className="excel-table">
          <thead>
            <tr>
              <th className="excel-row-num">#</th>
              {headerRow.map((cell, ci) => (
                <th key={ci}>{cell}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {dataRows.map((row, ri) => (
              <tr key={ri}>
                <td className="excel-row-num">{ri + 1}</td>
                {Array.from({ length: colCount }, (_, ci) => (
                  <td key={ci}>{row[ci] ?? ''}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {truncated && (
        <div className="excel-truncated rule-t open-style:control-voice">
          Showing first {MAX_PREVIEW_ROWS} of {rows.length - 1} rows
        </div>
      )}
    </div>
  );
}
