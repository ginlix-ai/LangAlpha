import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import ExportPreviewModal, {
  PRINT_FONTS,
  PRINT_PRESETS,
  GOOGLE_FONTS_URL,
} from '../ExportPreviewModal';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('react-to-print', () => ({
  useReactToPrint: () => vi.fn(),
}));

// Render Dialog inline (no portal) so Testing Library can find its children.
vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({ children, open }: any) =>
    open ? <div data-testid="dialog">{children}</div> : null,
  DialogContent: ({ children, className }: any) => (
    <div data-testid="dialog-content" className={className}>
      {children}
    </div>
  ),
  DialogHeader: ({ children }: any) => <div>{children}</div>,
  DialogTitle: ({ children }: any) => <h2>{children}</h2>,
}));

// Markdown just renders raw text so we can assert on content.
vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-preview">{content}</div>
  ),
}));

vi.mock('../toolDisplayConfig', () => ({
  stripLineNumbers: (s: string) => s?.replace(/^\d+\s*/gm, ''),
}));

// Suppress CSS import (vitest handles this via transforms, but just in case)
vi.mock('../ExportPreviewModal.css', () => ({}));

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const defaultProps = {
  open: true,
  onOpenChange: vi.fn(),
  content: '# Test Report\n\nSome content here.',
  fileName: 'report.md',
  workspaceId: 'ws-123',
  readFileFullFn: vi
    .fn()
    .mockResolvedValue({ content: '# Full Test Report\n\nFull content here.' }),
};

function renderModal(overrides: Partial<typeof defaultProps> = {}) {
  return render(<ExportPreviewModal {...defaultProps} {...overrides} />);
}

// ---------------------------------------------------------------------------
// Tests — exported constants
// ---------------------------------------------------------------------------

describe('Exported constants', () => {
  it('PRINT_PRESETS has 4 presets (Equity Research, Academic, Technical, General)', () => {
    expect(PRINT_PRESETS).toHaveLength(4);
    const labels = PRINT_PRESETS.map((p) => p.label);
    expect(labels).toEqual(['Equity Research', 'Academic', 'Technical', 'General']);
  });

  it('PRINT_FONTS has 12 fonts across 3 groups (Sans-serif, Serif, Mono)', () => {
    expect(PRINT_FONTS).toHaveLength(12);
    const groups = [...new Set(PRINT_FONTS.map((f) => f.group))];
    expect(groups).toEqual(expect.arrayContaining(['Sans-serif', 'Serif', 'Mono']));
    expect(groups).toHaveLength(3);
  });

  it('GOOGLE_FONTS_URL starts with the expected Google Fonts base', () => {
    expect(GOOGLE_FONTS_URL).toMatch(/^https:\/\/fonts\.googleapis\.com\/css2\?/);
  });
});

// ---------------------------------------------------------------------------
// Tests — component rendering & behaviour
// ---------------------------------------------------------------------------

describe('ExportPreviewModal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('does not render when open is false', () => {
    renderModal({ open: false });
    expect(screen.queryByTestId('dialog')).not.toBeInTheDocument();
  });

  it('renders modal with title when open', () => {
    renderModal();
    expect(screen.getByTestId('dialog')).toBeInTheDocument();
    // Title uses the i18n key
    expect(screen.getByText('filePanel.exportPreview')).toBeInTheDocument();
  });

  it('shows loading skeleton initially before readFileFullFn resolves', () => {
    // Make readFileFullFn hang so we observe the loading state.
    const neverResolve = vi.fn(() => new Promise(() => {}));
    renderModal({ readFileFullFn: neverResolve });

    // Skeleton bars are rendered when loading.
    const skeletonBars = document.querySelectorAll('.export-preview-skeleton-bar');
    expect(skeletonBars.length).toBeGreaterThan(0);
  });

  it('shows full content after readFileFullFn resolves', async () => {
    renderModal();

    await waitFor(() => {
      expect(screen.getByTestId('markdown-preview')).toHaveTextContent(
        '# Full Test Report',
      );
    });
  });

  it('shows default preset selection (Equity Research: font Inter, size 11, height 1.4)', () => {
    renderModal();

    // Style (preset) select
    const selects = screen.getAllByRole('combobox');
    const presetSelect = selects[0];
    expect(presetSelect).toHaveValue('Equity Research');

    // Font select
    const fontSelect = selects[1];
    expect(fontSelect).toHaveValue('"Inter", sans-serif');

    // Font size stepper shows 11px
    expect(screen.getByText('11px')).toBeInTheDocument();

    // Line height stepper shows 1.4
    expect(screen.getByText('1.4')).toBeInTheDocument();
  });

  it('preset change updates font, size, and line height', () => {
    renderModal();

    const selects = screen.getAllByRole('combobox');
    const presetSelect = selects[0];

    fireEvent.change(presetSelect, { target: { value: 'Academic' } });

    // Font should switch to Source Serif 4
    const fontSelect = selects[1];
    expect(fontSelect).toHaveValue('"Source Serif 4", serif');

    // Size should be 12
    expect(screen.getByText('12px')).toBeInTheDocument();

    // Line height should be 1.6
    expect(screen.getByText('1.6')).toBeInTheDocument();
  });

  it('font size stepper: clicking + increments, clicking - decrements', () => {
    renderModal();

    // Default size is 11px
    expect(screen.getByText('11px')).toBeInTheDocument();

    // All stepper buttons (font size has 2, line height has 2 = 4 total)
    const buttons = screen.getAllByRole('button');
    // Font size stepper buttons are the first pair of minus/plus buttons.
    // Find by the stepper value context: the button immediately before "11px" text.
    const steppers = document.querySelectorAll('.export-preview-stepper');
    const fontSizeStepper = steppers[0]; // first stepper is font size
    const [minusBtn, plusBtn] = fontSizeStepper.querySelectorAll('button');

    // Click +
    fireEvent.click(plusBtn);
    expect(screen.getByText('12px')).toBeInTheDocument();

    // Click -
    fireEvent.click(minusBtn);
    expect(screen.getByText('11px')).toBeInTheDocument();
  });

  it('font size stepper: min button disabled at 10, max button disabled at 22', () => {
    renderModal();

    const steppers = document.querySelectorAll('.export-preview-stepper');
    const fontSizeStepper = steppers[0];
    const [minusBtn, plusBtn] = fontSizeStepper.querySelectorAll('button');

    // Click minus down to 10 (from default 11)
    fireEvent.click(minusBtn);
    expect(screen.getByText('10px')).toBeInTheDocument();
    expect(minusBtn).toBeDisabled();

    // Click plus up to 22 (from 10, need 12 clicks)
    for (let i = 0; i < 12; i++) {
      fireEvent.click(plusBtn);
    }
    expect(screen.getByText('22px')).toBeInTheDocument();
    expect(plusBtn).toBeDisabled();
  });

  it('line height stepper: min button disabled at 1.2', () => {
    renderModal();

    const steppers = document.querySelectorAll('.export-preview-stepper');
    const lineHeightStepper = steppers[1];
    const [minusBtn] = lineHeightStepper.querySelectorAll('button');

    // Default is 1.4 — click minus once to get 1.2
    fireEvent.click(minusBtn);
    expect(screen.getByText('1.2')).toBeInTheDocument();
    expect(minusBtn).toBeDisabled();
  });

  it('renders "Save as PDF" button with i18n key', () => {
    renderModal();

    expect(screen.getByText('filePanel.saveAsPdf')).toBeInTheDocument();
  });

  it('calls readFileFullFn with correct workspaceId and fileName on open', () => {
    renderModal();

    expect(defaultProps.readFileFullFn).toHaveBeenCalledWith('ws-123', 'report.md');
  });

  it('applies stripLineNumbers for /large_tool_results/ paths', async () => {
    const numberedContent = '1 First line\n2 Second line\n3 Third line';
    const readFn = vi.fn().mockResolvedValue({ content: numberedContent });

    renderModal({
      fileName: '/large_tool_results/output.txt',
      readFileFullFn: readFn,
    });

    await waitFor(() => {
      const preview = screen.getByTestId('markdown-preview');
      // stripLineNumbers mock removes leading digits + space
      expect(preview).toHaveTextContent('First line');
      expect(preview.textContent).not.toMatch(/^\d+\s/);
    });
  });

  it('shows error state with retry and export-anyway buttons on fetch failure', async () => {
    const failFn = vi.fn().mockRejectedValue(new Error('Network error'));
    renderModal({ readFileFullFn: failFn });

    await waitFor(() => {
      expect(screen.getByText('filePanel.exportLoadError')).toBeInTheDocument();
      expect(screen.getByText('filePanel.tryAgain')).toBeInTheDocument();
      expect(screen.getByText('filePanel.exportAnyway')).toBeInTheDocument();
    });
  });

  it('retry button calls readFileFullFn again after error', async () => {
    const failFn = vi.fn().mockRejectedValue(new Error('Network error'));
    renderModal({ readFileFullFn: failFn });

    await waitFor(() => {
      expect(screen.getByText('filePanel.tryAgain')).toBeInTheDocument();
    });

    // First call was on mount
    expect(failFn).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('filePanel.tryAgain'));

    await waitFor(() => {
      expect(failFn).toHaveBeenCalledTimes(2);
    });
  });

  it('export-anyway button dismisses error and shows truncated content', async () => {
    const failFn = vi.fn().mockRejectedValue(new Error('Network error'));
    renderModal({ readFileFullFn: failFn });

    await waitFor(() => {
      expect(screen.getByText('filePanel.exportAnyway')).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText('filePanel.exportAnyway'));

    await waitFor(() => {
      // Error should be gone, markdown preview shows the fallback content
      expect(screen.queryByText('filePanel.exportLoadError')).not.toBeInTheDocument();
      expect(screen.getByTestId('markdown-preview')).toHaveTextContent('# Test Report');
    });
  });

  it('zoom stepper: clicking + increments, clicking - decrements', () => {
    renderModal();

    // Zoom is the 3rd stepper (font size, line height, zoom)
    const steppers = document.querySelectorAll('.export-preview-stepper');
    const zoomStepper = steppers[2];
    const [minusBtn, plusBtn] = zoomStepper.querySelectorAll('button');

    // Default zoom is 50%
    expect(screen.getByText('50%')).toBeInTheDocument();

    fireEvent.click(plusBtn);
    expect(screen.getByText('60%')).toBeInTheDocument();

    fireEvent.click(minusBtn);
    expect(screen.getByText('50%')).toBeInTheDocument();
  });

  it('zoom stepper: min clamp at 25%, max clamp at 100%', () => {
    renderModal();

    const steppers = document.querySelectorAll('.export-preview-stepper');
    const zoomStepper = steppers[2];
    const [minusBtn, plusBtn] = zoomStepper.querySelectorAll('button');

    // Go down to 25% (from 50%, need 3 clicks of -10%)
    for (let i = 0; i < 3; i++) fireEvent.click(minusBtn);
    // After 3 clicks: 50 -> 40 -> 30 -> 25 (clamped since 30-10 = 20 < 25)
    // Actually Math.max(0.25, 0.3-0.1) = 0.25 on 3rd click
    expect(minusBtn).toBeDisabled();

    // Go up to 100% (from 25%, need 8 clicks of +10%)
    for (let i = 0; i < 8; i++) fireEvent.click(plusBtn);
    expect(screen.getByText('100%')).toBeInTheDocument();
    expect(plusBtn).toBeDisabled();
  });

  it('line height stepper: max button disabled at 2.4', () => {
    renderModal();

    const steppers = document.querySelectorAll('.export-preview-stepper');
    const lineHeightStepper = steppers[1];
    const [, plusBtn] = lineHeightStepper.querySelectorAll('button');

    // Default is 1.4, click plus 5 times to reach 2.4
    for (let i = 0; i < 5; i++) fireEvent.click(plusBtn);
    expect(screen.getByText('2.4')).toBeInTheDocument();
    expect(plusBtn).toBeDisabled();
  });

  it('shows "Custom" when font is changed independently from preset', () => {
    renderModal();

    const selects = screen.getAllByRole('combobox');
    const fontSelect = selects[1];
    const presetSelect = selects[0];

    // Change font to Merriweather (not part of default Equity Research preset)
    fireEvent.change(fontSelect, { target: { value: '"Merriweather", serif' } });

    // Preset should now show Custom
    expect(presetSelect).toHaveValue('');
    // The "Custom" option text
    expect(screen.getByText('filePanel.custom')).toBeInTheDocument();
  });
});
