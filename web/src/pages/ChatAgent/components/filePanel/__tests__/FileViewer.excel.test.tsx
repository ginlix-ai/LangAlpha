import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { FileViewer, type FileViewerProps } from '../FileViewer';

// The spreadsheet viewer parses workbooks; here it only has to report the
// focus it was handed.
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'dark' }) }));
vi.mock('../../viewers/ExcelViewer', () => ({
  default: ({ focusCell, focusSeq }: { focusCell?: string | null; focusSeq?: number }) => (
    <div data-testid="excel" data-cell={focusCell ?? ''} data-seq={String(focusSeq)} />
  ),
}));

const focusAt = (seq: number) => ({
  focusAt: () => {}, jump: () => {}, dismiss: () => {}, chip: null, lineRange: null,
  focusPage: null, htmlAnchor: null, focusCell: 'Model!B4', seq,
}) as unknown as FileViewerProps['focus'];

const props = (seq: number): FileViewerProps => ({
  path: 'model.xlsx',
  body: { mode: 'buffer', content: null, mime: 'excel', buffer: new ArrayBuffer(8), truncated: false },
  loading: false,
  error: null,
  onRetry: () => {},
  workspaceId: 'ws',
  focus: focusAt(seq),
  onPageCount: () => {},
  isEditing: false,
  editContent: null,
  originalContent: null,
  showDiff: false,
  editorRef: React.createRef(),
  onEditorChange: () => {},
  onUndoRedoChange: () => {},
  onEditorTextSelect: () => {},
  onAddContext: null,
  onContentMouseUp: () => {},
  onViewerLink: () => {},
  onAnchorLink: () => {},
});

describe('FileViewer spreadsheet focus', () => {
  it('hands the same locator to the sheet again when it is asked for twice', async () => {
    // A second click on `model.xlsx#Model!B4` names the cell the sheet is
    // already on, so the locator alone cannot re-fire the scroll; the visit
    // count is what tells the sheet to return there, the way PdfViewer does.
    const { rerender } = render(<FileViewer {...props(1)} />);
    const sheet = await screen.findByTestId('excel');
    expect(sheet.dataset.cell).toBe('Model!B4');
    expect(sheet.dataset.seq).toBe('1');

    rerender(<FileViewer {...props(2)} />);
    expect((await screen.findByTestId('excel')).dataset.seq).toBe('2');
  });
});
