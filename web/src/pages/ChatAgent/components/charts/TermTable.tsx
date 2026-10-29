import React, { useState } from 'react';
import { cn } from '@/lib/utils';
import { TermMore } from './TermCard';
import { useTermPanel } from './termContext';

/** One column of a card's table: its head, how a row reads in it, and where it sits. */
export interface TermColumn<R> {
  key: string;
  /** The head; a table none of whose columns has one draws no head row. */
  label?: React.ReactNode;
  cell: (row: R, index: number) => React.ReactNode;
  /** A figure, set right. */
  num?: boolean;
  /** The cell's ink and weight. */
  className?: string;
  /** The cell's tooltip. */
  title?: (row: R) => string | undefined;
  /** Takes the spare width, the last such column shown: `clip` gives way
   *  first when the width runs short, `grow` never gives up its own (a
   *  strike or a symbol, which a cut would misquote). */
  fill?: 'clip' | 'grow';
  /** False for a column the card has no data or no room for. */
  show?: boolean;
}

interface Sort {
  key: string | null;
  dir: 'asc' | 'desc';
  onSort: (key: string) => void;
}

/**
 * A card's table: column heads over a rule, a row per item. With `limit` it
 * lists the first rows and counts the rest under them, out of `total` when
 * the rows are only the first of a larger answer. Nothing to list draws no
 * table, though a count of the rest still shows.
 */
export function TermTable<R>({
  rows,
  columns,
  rowKey,
  limit,
  total = rows.length,
  more,
  sort,
}: {
  rows: readonly R[];
  columns: ReadonlyArray<TermColumn<R>>;
  rowKey: (row: R, index: number) => React.Key;
  limit?: number;
  total?: number;
  /** The words for the rows left out, `+15 MORE`. */
  more?: (count: number) => React.ReactNode;
  sort?: Sort;
}): React.ReactElement | null {
  const shown = columns.filter((col) => col.show !== false);
  const grow = shown.findLast((col) => col.fill);
  const fillClass = (col: TermColumn<R>): string | false =>
    col === grow && (col.fill === 'clip' ? 'term-fill' : 'term-grow');
  const listed = limit == null ? rows : rows.slice(0, limit);
  const remaining = total - listed.length;
  const heads = shown.some((col) => col.label != null);
  // A panel is as wide as the reader drags it, and a card's narrow layout is
  // the transcript's: a table wider than the panel scrolls inside its frame.
  const panel = useTermPanel();
  const table = listed.length > 0 && (
    <table className="term-table">
      {heads && (
        <thead>
          <tr>
            {shown.map((col) => (
              <th
                key={col.key}
                className={cn(col.num && 'term-num', fillClass(col)) || undefined}
                aria-sort={sort?.key === col.key ? (sort.dir === 'asc' ? 'ascending' : 'descending') : undefined}
              >
                {sort ? (
                  <button type="button" className="term-sort" onClick={() => sort.onSort(col.key)}>
                    {col.label}
                    {sort.key === col.key && <TermSortMark dir={sort.dir} />}
                  </button>
                ) : (
                  col.label
                )}
              </th>
            ))}
          </tr>
        </thead>
      )}
      <tbody>
        {listed.map((row, i) => (
          <tr key={rowKey(row, i)}>
            {shown.map((col) => (
              <td
                key={col.key}
                className={cn(col.num && 'term-num', fillClass(col), col.className) || undefined}
                title={col.title?.(row)}
              >
                {col.cell(row, i)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
  return (
    <>
      {panel && table ? <div className="term-scroll">{table}</div> : table}
      {more && remaining > 0 && <TermMore>{more(remaining)}</TermMore>}
    </>
  );
}

/** The sort a header is under: a drawn triangle, as the cards draw a change. */
export function TermSortMark({ dir }: { dir: 'asc' | 'desc' }): React.ReactElement {
  return (
    <svg className="term-chg-mark" viewBox="0 0 10 9" aria-hidden="true" style={{ marginLeft: '0.5ch' }}>
      <path d={dir === 'asc' ? 'M5 0 L10 9 L0 9 Z' : 'M0 0 L10 0 L5 9 Z'} fill="currentColor" />
    </svg>
  );
}

/** A panel's column: sorted on its value, which also says whether a row fills it. */
export interface SortColumn<R> extends Omit<TermColumn<R>, 'cell'> {
  value: (row: R) => number | string | null | undefined;
  /** How the cell reads; the value as text when absent. */
  cell?: (row: R, index: number) => React.ReactNode;
  /** Shown even when every row shares it, for a value the edge does not carry. */
  keep?: boolean;
}

/**
 * The panel's table: every row, sortable by any column's head. A column no
 * row fills, or one word every row shares, says nothing and costs the table
 * its width, so it is not drawn: the screener's rule.
 */
export function SortTable<R>({
  rows,
  columns,
  rowKey,
}: {
  rows: readonly R[];
  columns: ReadonlyArray<SortColumn<R>>;
  rowKey: (row: R, index: number) => React.Key;
}): React.ReactElement {
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const onSort = (key: string) => {
    if (sortKey === key) {
      setSortDir((d) => (d === 'desc' ? 'asc' : 'desc'));
    } else {
      setSortKey(key);
      setSortDir('desc');
    }
  };
  const blank = (v: number | string | null | undefined): v is null | undefined | '' => v == null || v === '';
  const cols = columns.map((col): TermColumn<R> => {
    const values = rows.map(col.value).filter((v) => !blank(v));
    const says = values.length > 0 && (col.num || col.keep || rows.length < 2 || new Set(values).size > 1);
    return {
      ...col,
      show: col.show !== false && says,
      cell: (row, i) => {
        const raw = col.value(row);
        return blank(raw) ? '' : col.cell ? col.cell(row, i) : String(raw);
      },
    };
  });
  const sortCol = columns.find((col, i) => col.key === sortKey && cols[i].show);
  const sorted = !sortCol ? rows : [...rows].sort((a, b) => {
    const av = sortCol.value(a);
    const bv = sortCol.value(b);
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    const order = typeof av === 'string' ? av.localeCompare(String(bv)) : av - Number(bv);
    return sortDir === 'asc' ? order : -order;
  });
  return <TermTable rows={sorted} columns={cols} rowKey={rowKey} sort={{ key: sortKey, dir: sortDir, onSort }} />;
}
