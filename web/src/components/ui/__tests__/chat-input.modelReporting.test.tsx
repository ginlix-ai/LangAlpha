/**
 * ChatInput reports its live model selection to the host via onModelChange —
 * on mount (initial selection) and on every change, including the imperative
 * setModel() used by ChatView's fallback-suggestion pill. ChatView gates that
 * pill on this reported value (the model the next send actually uses).
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { createRef } from 'react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle } from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';

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

function renderInput(props: { initialModel?: string | null; onModelChange?: (m: string | null) => void } = {}) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const ref = createRef<ChatInputHandle>();
  const utils = render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput ref={ref} onSend={vi.fn()} {...props} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...utils, ref };
}

describe('ChatInput — onModelChange reporting', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('reports the initial selection on mount', () => {
    const onModelChange = vi.fn();
    renderInput({ initialModel: 'model-alpha', onModelChange });
    expect(onModelChange).toHaveBeenCalledWith('model-alpha');
  });

  it('reports imperative setModel() changes', () => {
    const onModelChange = vi.fn();
    const { ref } = renderInput({ initialModel: 'model-alpha', onModelChange });
    onModelChange.mockClear();
    act(() => {
      ref.current?.setModel('model-beta');
    });
    expect(onModelChange).toHaveBeenCalledWith('model-beta');
  });
});
