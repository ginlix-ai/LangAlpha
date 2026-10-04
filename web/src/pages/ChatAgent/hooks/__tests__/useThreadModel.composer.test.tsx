/**
 * The composer and the fallback pill driven by `useThreadModel`, the way the
 * chat view and the market panel wire them. Pins the two overlapping-pick
 * races end to end, on what the user sees and what the next send names: a
 * stale failure must never put the pill back on an older model while the row
 * holds the newer pick. Also pins that a pick made before the thread exists
 * reaches the first send.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { useState } from 'react';
import { MemoryRouter } from 'react-router';
import { QueryClientProvider, type QueryClient } from '@tanstack/react-query';
import ChatInput from '@/components/ui/chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';
import { createTestQueryClient } from '@/test/utils';
import type { Thread } from '@/types/api';
import { FallbackSuggestionPill } from '../../components/chatView/FallbackSuggestionPill';
import type { FallbackSuggestion } from '../../session/types';
import { useThreadModel } from '../useThreadModel';

const mocks = vi.hoisted(() => ({
  getThread: vi.fn(),
  updateThread: vi.fn(),
  toast: vi.fn(),
  preferences: { model_preference: { preferred_model: 'model-default' } } as unknown,
}));

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getThread: mocks.getThread,
  updateThread: mocks.updateThread,
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: true }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    modelAccessMap: undefined,
    validModelNames: new Set<string>(),
    catalogModelNames: new Set<string>(),
    metadata: {},
    isLoading: false,
    systemDefaults: { default_model: 'model-default', flash_model: '' },
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: vi.fn(), mutate: vi.fn() }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: mocks.toast }),
  toast: mocks.toast,
}));

// The menu's own rendering is not under test; the buttons reach the same
// onSelectModel the dropdown items call.
vi.mock('@/components/ui/chat-input.modelMenu', () => ({
  ChatInputModelMenu: ({ onSelectModel, selectedModel }: {
    onSelectModel: (m: string) => void;
    selectedModel: string | null;
  }) => (
    <>
      <span>{`pill:${selectedModel}`}</span>
      <button type="button" onClick={() => onSelectModel('model-x')}>pick-x</button>
      <button type="button" onClick={() => onSelectModel('model-y')}>pick-y</button>
    </>
  ),
  ModelTriggerMeasure: () => null,
}));

const THREAD = 'thread-1';
const SUGGESTION: FallbackSuggestion = { fromModel: 'model-thread', toModel: 'model-fallback' };

function row(llm_model: string | null): Thread {
  return { thread_id: THREAD, llm_model } as unknown as Thread;
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

const failure = () => Object.assign(new Error('400'), { response: { status: 400 } });

/** A thread host as the chat view builds one: the hook owns the model, the
 *  composer is controlled by it, and the fallback pill's switch is a pick
 *  that clears the suggestion once saved. */
function Host({ threadId, onSend }: { threadId: string; onSend: (...args: unknown[]) => void }) {
  const threadModel = useThreadModel({ threadId, mode: 'ptc', isLoading: false });
  const [suggestion, setSuggestion] = useState<FallbackSuggestion | null>(SUGGESTION);
  const { pickModel } = threadModel;
  return (
    <>
      <FallbackSuggestionPill
        fallbackSuggestion={suggestion}
        isLoading={false}
        composerModel={threadModel.model}
        onSwitchModel={(model) => { void pickModel(model).then((ok) => { if (ok) setSuggestion(null); }); }}
        onDismiss={() => setSuggestion(null)}
      />
      <ChatInput model={threadModel.model} onPickModel={pickModel} onSend={onSend} mode="ptc" />
    </>
  );
}

let queryClient: QueryClient;

function renderHost(threadId: string, onSend = vi.fn()) {
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <Host threadId={threadId} onSend={onSend} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return onSend;
}

function send(text: string) {
  const textarea = screen.getByRole('textbox');
  fireEvent.change(textarea, { target: { value: text } });
  fireEvent.keyDown(textarea, { key: 'Enter' });
}

function cachedModel() {
  return queryClient.getQueryData<Thread>(queryKeys.threads.detail(THREAD))?.llm_model;
}

describe('the composer on a thread-owned model', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    mocks.getThread.mockReset();
    mocks.updateThread.mockReset();
    mocks.toast.mockReset();
    sessionStorage.clear();
    queryClient = createTestQueryClient();
    queryClient.setQueryData(queryKeys.user.preferences(), mocks.preferences);
    queryClient.setQueryData(queryKeys.threads.detail(THREAD), row('model-thread'));
    mocks.getThread.mockResolvedValue(row('model-thread'));
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it("keeps a menu pick when a fallback switch made before it fails, and sends the pick", async () => {
    const fallback = deferred<Thread>();
    const y = deferred<Thread>();
    mocks.updateThread.mockReturnValueOnce(fallback.promise).mockReturnValueOnce(y.promise);
    const onSend = renderHost(THREAD);

    fireEvent.click(screen.getByRole('button', { name: 'Switch to model-fallback' }));
    expect(screen.getByText('pill:model-fallback')).toBeInTheDocument();
    await waitFor(() => expect(cachedModel()).toBe('model-fallback'));

    fireEvent.click(screen.getByText('pick-y'));
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();
    await waitFor(() => expect(cachedModel()).toBe('model-y'));

    await act(async () => { fallback.reject(failure()); });
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();
    expect(cachedModel()).toBe('model-y');
    expect(mocks.toast).not.toHaveBeenCalled();
    // The suggestion still stands, and the composer is off its model.
    expect(screen.getByRole('button', { name: 'Switch to model-fallback' })).toBeInTheDocument();

    send('next');
    expect(onSend.mock.calls[0][4]).toMatchObject({ model: 'model-y' });

    await waitFor(() => expect(mocks.updateThread).toHaveBeenLastCalledWith(THREAD, { llm_model: 'model-y' }));
    await act(async () => { y.resolve(row('model-y')); });
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();
  });

  it('keeps a re-pick when the first save of that model fails behind a newer pick, and sends it', async () => {
    const x1 = deferred<Thread>();
    const y = deferred<Thread>();
    const x2 = deferred<Thread>();
    mocks.updateThread
      .mockReturnValueOnce(x1.promise)
      .mockReturnValueOnce(y.promise)
      .mockReturnValueOnce(x2.promise);
    const onSend = renderHost(THREAD);

    fireEvent.click(screen.getByText('pick-x'));
    await waitFor(() => expect(cachedModel()).toBe('model-x'));
    fireEvent.click(screen.getByText('pick-y'));
    await waitFor(() => expect(cachedModel()).toBe('model-y'));
    fireEvent.click(screen.getByText('pick-x'));
    await waitFor(() => expect(cachedModel()).toBe('model-x'));

    await act(async () => { x1.reject(failure()); });
    expect(screen.getByText('pill:model-x')).toBeInTheDocument();
    send('next');
    expect(onSend.mock.calls[0][4]).toMatchObject({ model: 'model-x' });

    await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(2));
    await act(async () => { y.resolve(row('model-y')); });
    await waitFor(() => expect(mocks.updateThread).toHaveBeenCalledTimes(3));
    await act(async () => { x2.resolve(row('model-x')); });

    expect(screen.getByText('pill:model-x')).toBeInTheDocument();
    expect(cachedModel()).toBe('model-x');
    expect(mocks.toast).not.toHaveBeenCalled();
  });

  it('carries a pick made before the thread exists into the first send', () => {
    const onSend = renderHost('__default__');
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();

    fireEvent.click(screen.getByText('pick-y'));
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();
    send('start a thread');

    expect(onSend.mock.calls[0][4]).toMatchObject({ model: 'model-y' });
    expect(mocks.updateThread).not.toHaveBeenCalled();
  });
});
