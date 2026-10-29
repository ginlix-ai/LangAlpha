/**
 * `send()` on the handle is what a host with its own Send button (the welcome
 * conversation) calls: it does what Enter does, and says whether there was
 * anything to send.
 */
import { describe, it, expect, vi } from 'vitest';
import { render, fireEvent, act, screen } from '@testing-library/react';
import { createRef } from 'react';
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
  toast: vi.fn(),
}));

function renderInput(disabled = false) {
  const ref = createRef<ChatInputHandle>();
  const onSend = vi.fn();
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput ref={ref} onSend={onSend} disabled={disabled} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  const box = screen.getAllByRole('textbox')[0] as HTMLTextAreaElement;
  return { ref, box, onSend };
}

describe('ChatInput: send() on the handle', () => {
  it('sends what is written and returns true', () => {
    const { ref, box, onSend } = renderInput();
    fireEvent.change(box, { target: { value: 'what moved the market today' } });
    let sent = false;
    act(() => {
      sent = ref.current!.send();
    });
    expect(sent).toBe(true);
    expect(onSend).toHaveBeenCalledTimes(1);
    expect(onSend.mock.calls[0][0]).toBe('what moved the market today');
    expect(box.value).toBe('');
  });

  it('returns false and sends nothing when the box is empty', () => {
    const { ref, onSend } = renderInput();
    let sent = true;
    act(() => {
      sent = ref.current!.send();
    });
    expect(sent).toBe(false);
    expect(onSend).not.toHaveBeenCalled();
  });

  it('returns false and sends nothing when the composer is disabled', () => {
    const { ref, box, onSend } = renderInput(true);
    fireEvent.change(box, { target: { value: 'written before it was disabled' } });
    let sent = true;
    act(() => {
      sent = ref.current!.send();
    });
    expect(sent).toBe(false);
    expect(onSend).not.toHaveBeenCalled();
  });
});

describe('ChatInput: Enter under an IME', () => {
  it('leaves the draft alone when Enter commits a composition', () => {
    const { box, onSend } = renderInput();
    fireEvent.change(box, { target: { value: '帮我看看 NVDA' } });
    fireEvent.keyDown(box, { key: 'Enter', isComposing: true });
    fireEvent.keyDown(box, { key: 'Enter', keyCode: 229 });
    expect(onSend).not.toHaveBeenCalled();
    expect(box.value).toBe('帮我看看 NVDA');
    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSend).toHaveBeenCalledTimes(1);
  });
});
