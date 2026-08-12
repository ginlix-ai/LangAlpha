/**
 * Long text pastes condense into a removable pill instead of flooding the
 * draft; the pill flattens back into the outgoing message on send. Small
 * pastes keep the native textarea behavior.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput from '../chat-input';
import { isLargePaste, PASTE_PILL_MIN_CHARS } from '../chat-input.helpers';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('@/lib/modelCapabilities', () => ({
  supportsXhighEffort: () => false,
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

function renderInput(onSend = vi.fn()) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput onSend={onSend} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return screen.getByRole('textbox') as HTMLTextAreaElement;
}

function paste(textarea: HTMLTextAreaElement, text: string) {
  fireEvent.paste(textarea, {
    clipboardData: { items: [], getData: () => text },
  });
}

const LONG_TEXT = Array.from({ length: 40 }, (_, i) => `line ${i + 1} of the pasted document`).join('\n');

describe('isLargePaste', () => {
  it('condenses by character count', () => {
    expect(isLargePaste('x'.repeat(PASTE_PILL_MIN_CHARS))).toBe(true);
    expect(isLargePaste('x'.repeat(PASTE_PILL_MIN_CHARS - 1))).toBe(false);
  });

  it('condenses by line count even when short', () => {
    expect(isLargePaste(Array.from({ length: 15 }, () => 'ln').join('\n'))).toBe(true);
    expect(isLargePaste(Array.from({ length: 14 }, () => 'ln').join('\n'))).toBe(false);
  });
});

describe('ChatInput — paste pill', () => {
  it('stages a large paste as a pill and keeps the draft empty', () => {
    const textarea = renderInput();
    paste(textarea, LONG_TEXT);
    expect(screen.getByText('Pasted text · 40 lines')).toBeInTheDocument();
    expect(textarea.value).toBe('');
  });

  it('leaves small pastes to the native textarea (no pill)', () => {
    const textarea = renderInput();
    paste(textarea, 'just a sentence');
    expect(screen.queryByText(/Pasted text ·/)).toBeNull();
  });

  it('flattens the pill into the outgoing message on send', () => {
    const onSend = vi.fn();
    const textarea = renderInput(onSend);
    paste(textarea, LONG_TEXT);
    fireEvent.keyDown(textarea, { key: 'Enter' });
    expect(onSend).toHaveBeenCalledTimes(1);
    const sent = onSend.mock.calls[0][0] as string;
    expect(sent).toContain('<summary>[Pasted text]</summary>');
    expect(sent).toContain('line 40 of the pasted document');
  });

  it('removing the pill drops the staged text without touching the draft', () => {
    const onSend = vi.fn();
    const textarea = renderInput(onSend);
    fireEvent.change(textarea, { target: { value: 'summarize @ noon' } });
    paste(textarea, LONG_TEXT);
    fireEvent.click(screen.getByTitle('Remove'));
    expect(screen.queryByText(/Pasted text ·/)).toBeNull();
    // A pathless pill must not run the @path scrub — the draft (including the
    // standalone "@") stays exactly as typed.
    expect(textarea.value).toBe('summarize @ noon');
    fireEvent.keyDown(textarea, { key: 'Enter' });
    expect(onSend.mock.calls[0][0]).toBe('summarize @ noon');
  });
});
