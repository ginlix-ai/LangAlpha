/**
 * A composer's reasoning effort and fast mode are its thread's own, over what
 * Settings gives the model. A thread host owns them (the hook behind `tuning`
 * is pinned in useThreadTuning's tests); a composer with no thread holds them
 * and hands them to the thread its send starts. These pin what each shows and
 * sends: the thread's own level, shown clamped onto the model's ladder and
 * sent as the row holds it, else the Settings level; a pick handed to the host, null for the level Settings
 * gives, and never written to the account; and every send carrying the level
 * the pill shows.
 */
import { createRef } from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, fireEvent, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle } from '../chat-input';
import type { ThreadTuning } from '../chat-input.types';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  mutateAsync: vi.fn(),
  mutate: vi.fn(),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: true }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    payers: {},
    validModelNames: new Set(['model-default']),
    metadata: {
      'model-default': { sdk: 'codex', reasoning_efforts: ['low', 'medium', 'high'], reasoning_effort_default: 'medium' },
    },
    isLoading: false,
    systemDefaults: { default_model: 'model-default' },
    offPlanModels: new Set(),
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: mocks.mutateAsync, mutate: mocks.mutate }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(),
}));

// The menu's own rendering is pinned in chatInputModelMenu's tests; this one
// reads the props it is handed and reaches the same handlers its options call.
vi.mock('../chat-input.modelMenu', () => ({
  ChatInputModelMenu: () => null,
  ChatInputEffortMenu: ({ reasoningEffort, defaultEffort, fastMode, onReasoningEffortChange, onFastModeChange }: {
    reasoningEffort: string | null;
    defaultEffort: string | null;
    fastMode: boolean | null;
    onReasoningEffortChange: (effort: string | null) => void;
    onFastModeChange: (fast: boolean) => void;
  }) => (
    <>
      <span>{`effort:${reasoningEffort}`}</span>
      <span>{`default:${defaultEffort}`}</span>
      <span>{`fast:${fastMode}`}</span>
      <button type="button" onClick={() => onReasoningEffortChange('high')}>pick-high</button>
      <button type="button" onClick={() => onReasoningEffortChange(null)}>pick-default</button>
      <button type="button" onClick={() => onFastModeChange(true)}>pick-fast</button>
      <button type="button" onClick={() => onFastModeChange(false)}>pick-standard</button>
    </>
  ),
  ModelTriggerMeasure: () => null,
}));

let queryClient: QueryClient;

interface TreeProps {
  onSend?: (...args: unknown[]) => void;
  tuning?: ThreadTuning;
  onPickReasoningEffort?: (effort: string | null) => void;
  onPickFastMode?: (fast: boolean | null) => void;
  inputRef?: React.Ref<ChatInputHandle>;
}

function tree({ onSend = vi.fn(), tuning, onPickReasoningEffort, onPickFastMode, inputRef }: TreeProps = {}) {
  queryClient.setQueryData(queryKeys.user.preferences(), mocks.preferences);
  return (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput
          ref={inputRef}
          onSend={onSend}
          model="model-default"
          tuning={tuning}
          onPickReasoningEffort={onPickReasoningEffort}
          onPickFastMode={onPickFastMode}
        />
      </MemoryRouter>
    </QueryClientProvider>
  );
}

function send(text: string) {
  const textarea = screen.getByRole('textbox');
  fireEvent.change(textarea, { target: { value: text } });
  fireEvent.keyDown(textarea, { key: 'Enter' });
}

function sentOptions(onSend: ReturnType<typeof vi.fn>) {
  return onSend.mock.calls.at(-1)?.[3] as Record<string, unknown>;
}

describe('ChatInput: the thread owns its effort and fast mode', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    // Settings: the model's profile says low, the account says fast.
    mocks.preferences = {
      model_preference: { fast_mode: true, profiles: { 'model-default': { reasoning_effort: 'low' } } },
    };
    mocks.mutate.mockReset();
    mocks.mutateAsync.mockReset();
    localStorage.clear();
  });

  it('shows and sends what Settings gives a thread with none of its own', () => {
    const onSend = vi.fn();
    render(tree({ onSend, tuning: { reasoningEffort: null, fastMode: null } }));
    expect(screen.getByText('effort:low')).toBeInTheDocument();
    expect(screen.getByText('default:low')).toBeInTheDocument();
    expect(screen.getByText('fast:true')).toBeInTheDocument();

    send('hi');
    expect(sentOptions(onSend)).toMatchObject({ reasoningEffort: 'low', fastMode: true });
  });

  it("shows the thread's own clamped onto the model's ladder, and sends it as held", () => {
    const onSend = vi.fn();
    render(tree({ onSend, tuning: { reasoningEffort: 'max', fastMode: false } }));
    expect(screen.getByText('effort:high')).toBeInTheDocument();
    expect(screen.getByText('default:low')).toBeInTheDocument();

    send('hi');
    expect(sentOptions(onSend)).toMatchObject({ reasoningEffort: 'max', fastMode: false });
    expect(sentOptions(onSend)).not.toHaveProperty('reasoningEffortOverride');
  });

  it('claims and sends no level while the thread is unread, so the server runs its own', () => {
    const onSend = vi.fn();
    render(tree({ onSend, tuning: { reasoningEffort: undefined, fastMode: undefined } }));
    expect(screen.getByText('effort:null')).toBeInTheDocument();

    send('hi');
    expect(sentOptions(onSend)).toMatchObject({ reasoningEffort: null, fastMode: null });
  });

  it('hands a pick to the host and never writes Settings', () => {
    const onPickReasoningEffort = vi.fn();
    const onPickFastMode = vi.fn();
    render(tree({ tuning: { reasoningEffort: null, fastMode: null }, onPickReasoningEffort, onPickFastMode }));

    fireEvent.click(screen.getByText('pick-high'));
    expect(onPickReasoningEffort).toHaveBeenCalledWith('high');
    // Speed on the side Settings is on follows Settings; the other side is the thread's own.
    fireEvent.click(screen.getByText('pick-standard'));
    expect(onPickFastMode).toHaveBeenCalledWith(false);
    expect(mocks.mutate).not.toHaveBeenCalled();
    expect(mocks.mutateAsync).not.toHaveBeenCalled();
  });

  it("returns the thread to Settings with a pick of the side Settings is on", () => {
    const onPickReasoningEffort = vi.fn();
    const onPickFastMode = vi.fn();
    render(tree({ tuning: { reasoningEffort: 'high', fastMode: false }, onPickReasoningEffort, onPickFastMode }));

    fireEvent.click(screen.getByText('pick-default'));
    expect(onPickReasoningEffort).toHaveBeenCalledWith(null);
    fireEvent.click(screen.getByText('pick-fast'));
    expect(onPickFastMode).toHaveBeenCalledWith(null);
  });

  it('with no host, holds its picks and hands them to the thread its send starts', () => {
    const onSend = vi.fn();
    const inputRef = createRef<ChatInputHandle>();
    render(tree({ onSend, inputRef }));
    send('first');
    expect(sentOptions(onSend)).not.toHaveProperty('reasoningEffortOverride');
    expect(sentOptions(onSend)).not.toHaveProperty('fastModeOverride');

    fireEvent.click(screen.getByText('pick-high'));
    fireEvent.click(screen.getByText('pick-standard'));
    expect(screen.getByText('effort:high')).toBeInTheDocument();
    expect(screen.getByText('fast:false')).toBeInTheDocument();

    send('second');
    expect(sentOptions(onSend)).toMatchObject({
      reasoningEffort: 'high',
      fastMode: false,
      reasoningEffortOverride: 'high',
      fastModeOverride: false,
    });
    // A start with nothing typed reads the options rather than sending, and
    // hands on the same picks.
    expect(inputRef.current?.getModelOptions()).toMatchObject({
      reasoningEffortOverride: 'high',
      fastModeOverride: false,
    });
    expect(mocks.mutate).not.toHaveBeenCalled();
  });
});
