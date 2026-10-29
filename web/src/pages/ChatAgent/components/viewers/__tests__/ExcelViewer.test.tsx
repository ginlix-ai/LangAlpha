import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import ExcelJS from 'exceljs';
import i18n from '@/i18n';
import type { ContextPayload } from '../../filePanel/types';
import ExcelViewer from '../ExcelViewer';
import type { GridRowProps } from '../excel/GridRow';
import { openpyxlWorkbook } from '../excel/__tests__/openpyxlFixture';

// Every row render is logged by row number, through the memo boundary the
// viewer imports, so a test can say which rows an interaction reached.
const rowRenders = vi.hoisted(() => ({ rows: [] as number[] }));
vi.mock('../excel/GridRow', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../excel/GridRow')>();
  const { memo } = await import('react');
  const View = (props: GridRowProps) => {
    rowRenders.rows.push(props.r);
    return mod.GridRowView(props);
  };
  return { ...mod, GridRowView: View, GridRow: memo(View) };
});

async function workbook(): Promise<ArrayBuffer> {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Model');
  wb.addWorksheet('Inputs');
  ws.getCell('A4').value = 'Units (M)';
  ws.getCell('B4').value = 12.4;
  ws.getCell('B4').numFmt = '#,##0.0';
  ws.getCell('B5').value = { formula: 'B4*2', result: 24.8 };
  ws.getCell('B5').numFmt = '#,##0.0';
  ws.getCell('B6').value = { formula: 'B5*Inputs!C3' } as ExcelJS.CellFormulaValue;
  return (await wb.xlsx.writeBuffer()) as ArrayBuffer;
}

// A1:B2 merged, so B2 is covered and the grid draws no cell for it at all.
async function mergedWorkbook(): Promise<ArrayBuffer> {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet('Model');
  ws.getCell('A1').value = 'Revenue bridge';
  ws.mergeCells('A1:B2');
  ws.getCell('A3').value = 'Units (M)';
  return (await wb.xlsx.writeBuffer()) as ArrayBuffer;
}

const cell = (ref: string) => document.querySelector<HTMLTableCellElement>(`td[data-ref="${ref}"]`)!;
const nameBox = () => document.querySelector('.excel-namebox')!.textContent;
const fx = () => document.querySelector('.excel-fx')!.textContent;
const note = () => document.querySelector('.excel-footer-note')?.textContent;

describe('ExcelViewer', () => {
  let data: ArrayBuffer;
  let merged: ArrayBuffer;

  beforeAll(async () => {
    await i18n.changeLanguage('en-US');
    // jsdom has no scrollIntoView; the grid uses it to follow the selection.
    Element.prototype.scrollIntoView = vi.fn();
    data = await workbook();
    merged = await mergedWorkbook();
  });

  beforeEach(() => {
    vi.clearAllMocks();
    rowRenders.rows.length = 0;
  });

  const open = async (props: Partial<React.ComponentProps<typeof ExcelViewer>> = {}) => {
    const view = render(<ExcelViewer data={data} {...props} />);
    await screen.findByText('Units (M)');
    return view;
  };

  it('addresses cells the way Excel does, with row 1 a data row', async () => {
    await open();
    const head = document.querySelector('thead tr')!;
    expect(within(head as HTMLElement).getByText('A')).toBeInTheDocument();
    expect(within(head as HTMLElement).getByText('B')).toBeInTheDocument();
    // The label sits on row 4 and is reachable at A4, not at A3.
    expect(cell('A4').textContent).toBe('Units (M)');
    expect(document.querySelectorAll('tbody tr').length).toBe(6);
  });

  it('reports a cached formula, its value and its format', async () => {
    await open();
    fireEvent.mouseDown(cell('B5'));
    expect(nameBox()).toBe('B5');
    expect(fx()).toContain('=B4*2');
    expect(fx()).toContain('24.8');
    expect(fx()).toContain('#,##0.0');
  });

  it('says so when a formula carries no value instead of printing an object', async () => {
    await open();
    expect(cell('B6').textContent).toBe('=B5*Inputs!C3');
    fireEvent.mouseDown(cell('B6'));
    expect(fx()).toContain('not calculated');
    // The cross-sheet half is named in the bar, and is not on screen to tint.
    expect(fx()).toContain('Inputs!C3');
    expect(document.querySelectorAll('.is-precedent').length).toBe(1);
  });

  it('tints the cells the selected formula reads', async () => {
    await open();
    fireEvent.mouseDown(cell('B5'));
    expect(cell('B4').className).toContain('is-precedent');
    expect(cell('B6').className).not.toContain('is-precedent');
  });

  it('moves with the arrow keys and extends with shift', async () => {
    await open();
    fireEvent.mouseDown(cell('B4'));
    expect(nameBox()).toBe('B4');
    const grid = screen.getByRole('grid');
    fireEvent.keyDown(grid, { key: 'ArrowDown' });
    expect(nameBox()).toBe('B5');
    fireEvent.keyDown(grid, { key: 'ArrowDown', shiftKey: true });
    expect(nameBox()).toBe('B5:B6');
    fireEvent.keyDown(grid, { key: 'ArrowLeft', shiftKey: true });
    expect(nameBox()).toBe('A5:B6');
  });

  it('keeps the Escape that clears a selection, and lets the next one reach the panel', async () => {
    const above = vi.fn();
    render(<div onKeyDown={above}><ExcelViewer data={data} /></div>);
    await screen.findByText('Units (M)');
    fireEvent.mouseDown(cell('B4'));
    expect(nameBox()).toBe('B4');
    const grid = screen.getByRole('grid');
    fireEvent.keyDown(grid, { key: 'Escape' });
    expect(nameBox()).toBe('');
    expect(above).not.toHaveBeenCalled();
    fireEvent.keyDown(grid, { key: 'Escape' });
    expect(above).toHaveBeenCalledTimes(1);
  });

  it('re-renders only the rows an arrow key leaves and lands on', async () => {
    await open();
    fireEvent.mouseDown(cell('B4'));
    rowRenders.rows.length = 0;
    fireEvent.keyDown(screen.getByRole('grid'), { key: 'ArrowDown' });
    expect(nameBox()).toBe('B5');
    // Row 4 loses the active cell, row 5 gains it; rows 1-3 and 6 are untouched.
    expect(new Set(rowRenders.rows)).toEqual(new Set([4, 5]));
  });

  it('exposes the grid, its headers and the active cell to assistive tech', async () => {
    await open();
    fireEvent.mouseDown(cell('B5'));
    const grid = screen.getByRole('grid');
    expect(grid.getAttribute('aria-activedescendant')).toBe(cell('B5').id);
    expect(cell('B5').getAttribute('role')).toBe('gridcell');
    expect(cell('B5').getAttribute('aria-selected')).toBe('true');
    expect(cell('A4').hasAttribute('aria-selected')).toBe(false);
    expect(screen.getByRole('columnheader', { name: 'B' })).toBeInTheDocument();
    // Enter on a focused header selects its column, as a click would.
    fireEvent.keyDown(screen.getByRole('columnheader', { name: 'A' }), { key: 'Enter' });
    expect(nameBox()).toBe('A:A');
    fireEvent.keyDown(screen.getByRole('rowheader', { name: '5' }), { key: ' ' });
    expect(nameBox()).toBe('5:5');
  });

  it('extends a range on a shift-click', async () => {
    await open();
    fireEvent.mouseDown(cell('A4'));
    fireEvent.mouseDown(cell('B6'), { shiftKey: true });
    expect(nameBox()).toBe('A4:B6');
  });

  it('selects a whole column from its header, and more of them on shift', async () => {
    await open();
    fireEvent.mouseDown(screen.getByTitle('Select column B'));
    expect(nameBox()).toBe('B:B');
    expect(cell('B4').classList.contains('is-selected')).toBe(true);
    expect(cell('A4').classList.contains('is-selected')).toBe(false);
    fireEvent.mouseDown(screen.getByTitle('Select column A'), { shiftKey: true });
    expect(nameBox()).toBe('A:B');
    // A cell click drops back to a plain range.
    fireEvent.mouseDown(cell('B5'));
    expect(nameBox()).toBe('B5');
  });

  it('selects a whole row from its number, and with Shift+Space', async () => {
    await open();
    fireEvent.mouseDown(screen.getByTitle('Select row 4'));
    expect(nameBox()).toBe('4:4');
    expect(cell('A4').classList.contains('is-selected')).toBe(true);
    expect(cell('A5').classList.contains('is-selected')).toBe(false);
    fireEvent.mouseDown(cell('B5'));
    fireEvent.keyDown(screen.getByLabelText('Model cells'), { key: ' ', shiftKey: true });
    expect(nameBox()).toBe('5:5');
    fireEvent.keyDown(screen.getByLabelText('Model cells'), { key: ' ', ctrlKey: true });
    expect(nameBox()).toBe('B:B');
  });

  it('hands a whole column to the composer as one line that names it, not its cells', async () => {
    const onAddContext = vi.fn<(p: ContextPayload) => void>();
    await open({ filePath: 'models/dcf.xlsx', onAddContext });
    fireEvent.mouseDown(screen.getByTitle('Select column B'));
    fireEvent.click(screen.getByText('Add'));

    const payload = onAddContext.mock.calls[0][0];
    expect(payload.locator).toBe('Model!B:B');
    expect(payload.label).toBe('dcf.xlsx#Model!B:B');
    expect(payload.snippet).toBe('Model!B:B · column B · 1 value, 2 formulas in rows 1-6');
  });

  it('opens a whole column a link named', async () => {
    await open({ focusCell: 'Model!B:B', focusSeq: 1 });
    await waitFor(() => expect(nameBox()).toBe('B:B'));
    expect(cell('B6').classList.contains('is-selected')).toBe(true);
  });

  it('swaps values for formulas on the toggle', async () => {
    await open();
    expect(cell('B5').textContent).toBe('24.8');
    fireEvent.click(screen.getByTitle('Show formulas'));
    expect(cell('B5').textContent).toBe('=B4*2');
    expect(cell('B4').textContent).toBe('12.4');
  });

  it('hands a selected range to the composer as a locator plus a TSV block', async () => {
    const onAddContext = vi.fn<(p: ContextPayload) => void>();
    await open({ filePath: 'models/dcf.xlsx', onAddContext });
    fireEvent.mouseDown(cell('A4'));
    fireEvent.mouseDown(cell('B5'), { shiftKey: true });
    fireEvent.click(screen.getByText('Add'));

    expect(onAddContext).toHaveBeenCalledTimes(1);
    const payload = onAddContext.mock.calls[0][0];
    expect(payload.path).toBe('models/dcf.xlsx');
    expect(payload.locator).toBe('Model!A4:B5');
    expect(payload.label).toBe('dcf.xlsx#Model!A4:B5');
    expect(payload.snippet).toContain('Units (M)\t12.4');
    expect(payload.snippet).toContain('B5\tB4*2');
    // The offer goes down once taken.
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('drops the old grid, selection and offer the moment new bytes arrive', async () => {
    const onAddContext = vi.fn<(p: ContextPayload) => void>();
    const { rerender } = await open({ filePath: 'models/dcf.xlsx', onAddContext });
    fireEvent.mouseDown(cell('A4'));
    expect(screen.getByText('Add')).toBeInTheDocument();

    rerender(<ExcelViewer data={merged} filePath="models/dcf.xlsx" onAddContext={onAddContext} />);
    // Nothing from the previous workbook survives the swap, not even for a frame.
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
    expect(document.querySelector('td[data-ref="A4"]')).toBeNull();

    await waitFor(() => expect(cell('A3')?.textContent).toBe('Units (M)'));
    // The new workbook opens on its own first cell, not on the old selection.
    expect(nameBox()).toBe('A1');
  });

  it('offers nothing when the panel gave it nowhere to send a selection', async () => {
    await open();
    fireEvent.mouseDown(cell('B5'));
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('opens the cell a link named, on the sheet it named', async () => {
    await open({ focusCell: 'Model!B5' });
    await waitFor(() => expect(nameBox()).toBe('B5'));
    expect(cell('B5').className).toContain('is-focus');
    expect(Element.prototype.scrollIntoView).toHaveBeenCalled();
  });

  it('selects nothing for a link to a sheet the workbook does not have', async () => {
    await open({ filePath: 'models/dcf.xlsx', onAddContext: vi.fn(), focusCell: 'Missing!B7' });
    await waitFor(() => expect(nameBox()).toBe(''));
    expect(screen.getByRole('button', { name: 'Model' }).className).toContain('active');
    expect(cell('B4').classList.contains('is-selected')).toBe(false);
    // No `Model!B7` offered in place of a cell that was never there.
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
  });

  it('offers no citation for a link past the sheet\'s cells', async () => {
    await open({ filePath: 'models/dcf.xlsx', onAddContext: vi.fn(), focusCell: 'Model!Z50' });
    await waitFor(() => expect(nameBox()).toBe('Z50'));
    expect(screen.queryByText('Add')).not.toBeInTheDocument();
    // A selection inside the sheet brings the offer back.
    fireEvent.mouseDown(cell('B5'));
    expect(screen.getByText('Add')).toBeInTheDocument();
  });

  it('clips a link that runs past the sheet to the cells it has, and says so in the snippet', async () => {
    const onAddContext = vi.fn<(p: ContextPayload) => void>();
    await open({ filePath: 'models/dcf.xlsx', onAddContext, focusCell: 'Model!B5:B900' });
    await waitFor(() => expect(nameBox()).toBe('B5:B900'));
    fireEvent.click(screen.getByText('Add'));
    const payload = onAddContext.mock.calls[0][0];
    expect(payload.locator).toBe('Model!B5:B900');
    expect(payload.snippet?.split('\n')[0]).toBe('Model!B5:B900 · values (TSV), B5:B6 is inside the parsed sheet (rows 1-6, columns A-B)');
    expect(payload.snippet).toContain('B5\tB4*2');
  });

  it('reads a link into a covered cell as the merge that draws it', async () => {
    const onAddContext = vi.fn<(p: ContextPayload) => void>();
    render(<ExcelViewer data={merged} filePath="models/dcf.xlsx" onAddContext={onAddContext} focusCell="Model!B2" />);
    await screen.findByText('Units (M)');
    // Nothing is offered under the opening selection, so the chip appearing is
    // the locator effect having run; waiting on the name box would pass on
    // `A1` before it did.
    const add = await screen.findByText('Add');
    // B2 is covered: without the merge rule nothing carries the active cell.
    expect(nameBox()).toBe('A1');
    expect(cell('B2')).toBeNull();
    expect(cell('A1').className).toContain('is-focus');
    expect(fx()).toContain('Revenue bridge');

    fireEvent.click(add);
    const payload = onAddContext.mock.calls[0][0];
    expect(payload.locator).toBe('Model!A1');
    expect(payload.snippet).toContain('Revenue bridge');
  });

  it('lists every sheet and switches between them', async () => {
    await open();
    expect(screen.getByRole('button', { name: 'Model' }).className).toContain('active');
    fireEvent.click(screen.getByRole('button', { name: 'Inputs' }));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Inputs' }).className).toContain('active'));
  });

  it('says per sheet how many charts and pictures the grid leaves out, and sends the reader to the download', async () => {
    render(<ExcelViewer data={openpyxlWorkbook()} canDownload />);
    await screen.findByRole('button', { name: 'Charts' });
    expect(note()).toBe('1 formula has no cached value · 1 chart and 1 picture not shown here. Download the file to see them');
    fireEvent.click(screen.getByRole('button', { name: 'Charts' }));
    expect(note()).toBe('2 charts not shown here. Download the file to see them');
    fireEvent.click(screen.getByRole('button', { name: 'Notes' }));
    expect(note()).toBe('1 picture not shown here. Download the file to see it');
    fireEvent.click(screen.getByRole('button', { name: 'Inputs' }));
    expect(note()).toBeUndefined();
  });

  it('points at no download where the page offers none', async () => {
    render(<ExcelViewer data={openpyxlWorkbook()} />);
    fireEvent.click(await screen.findByRole('button', { name: 'Charts' }));
    expect(note()).toBe('2 charts not shown here');
  });
});
