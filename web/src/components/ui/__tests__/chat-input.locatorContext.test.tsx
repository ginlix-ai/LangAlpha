/**
 * A context pill can name where it came from in the file's own vocabulary. A
 * spreadsheet range writes `Model!B4:D9`, and the summary it sends is the link
 * that reopens exactly that range — lines stay the unit for everything else.
 */
import { createRef } from 'react';
import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle } from '../chat-input';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

function renderInput(onSend = vi.fn()) {
  const ref = createRef<ChatInputHandle>();
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput ref={ref} onSend={onSend} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ref, textarea: screen.getByRole('textbox') as HTMLTextAreaElement };
}

const RANGE = {
  path: 'models/dcf.xlsx',
  locator: 'Model!B4:D9',
  label: 'dcf.xlsx#Model!B4:D9',
  snippet: 'Model!B4:D9 — values (TSV)\n12.4\t13.1\t13.9',
};

describe('ChatInput — locator context', () => {
  it('shows the range on the pill', () => {
    const { ref } = renderInput();
    act(() => ref.current!.addContext(RANGE));
    expect(screen.getByText('dcf.xlsx#Model!B4:D9')).toBeInTheDocument();
  });

  it('sends the locator as a reopenable summary, sized in cells', () => {
    const onSend = vi.fn();
    const { ref, textarea } = renderInput(onSend);
    act(() => ref.current!.addContext(RANGE));
    fireEvent.keyDown(textarea, { key: 'Enter' });

    const sent = onSend.mock.calls[0][0] as string;
    expect(sent).toContain('<summary>@models/dcf.xlsx#Model!B4:D9 (18 cells)</summary>');
    expect(sent).toContain('12.4\t13.1\t13.9');
  });

  it('sizes a whole column by name, not by the sheet capacity it spans', () => {
    const onSend = vi.fn();
    const { ref, textarea } = renderInput(onSend);
    act(() => ref.current!.addContext({
      ...RANGE, locator: 'Model!B:B', label: 'dcf.xlsx#Model!B:B', snippet: 'Model!B:B · column B · header "FY2025E" · 40 values, 8 formulas in rows 1-48',
    }));
    fireEvent.keyDown(textarea, { key: 'Enter' });

    const sent = onSend.mock.calls[0][0] as string;
    expect(sent).toContain('<summary>@models/dcf.xlsx#Model!B:B (column B)</summary>');
    expect(sent).not.toContain('1048576');
  });

  it('keeps the line-range summary for a line snippet', () => {
    const onSend = vi.fn();
    const { ref, textarea } = renderInput(onSend);
    act(() => ref.current!.addContext({
      path: 'work/model.py',
      snippet: 'def npv(rate, flows):',
      label: 'model.py:L40-42',
      lineStart: 40,
      lineEnd: 42,
      lineCount: 3,
    }));
    fireEvent.keyDown(textarea, { key: 'Enter' });

    const sent = onSend.mock.calls[0][0] as string;
    expect(sent).toContain('<summary>@work/model.py (lines 40-42, 3 lines)</summary>');
    expect(sent).not.toContain('#');
  });

  it('keeps two different ranges of the same file apart', () => {
    const { ref } = renderInput();
    act(() => {
      ref.current!.addContext(RANGE);
      ref.current!.addContext({ ...RANGE, locator: 'Model!B7', label: 'dcf.xlsx#Model!B7', snippet: 'Model!B7 — values (TSV)\n1,851' });
      ref.current!.addContext(RANGE);
    });
    expect(screen.getByText('dcf.xlsx#Model!B4:D9')).toBeInTheDocument();
    expect(screen.getByText('dcf.xlsx#Model!B7')).toBeInTheDocument();
  });
});
