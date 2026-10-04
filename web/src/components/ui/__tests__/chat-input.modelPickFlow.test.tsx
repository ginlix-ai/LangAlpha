/**
 * A composer's model belongs to its thread. A thread host owns it (the hook
 * behind `model` is pinned in useThreadModel's tests); a composer with no
 * thread holds it itself. These pin what each shows and sends: the host's
 * model as given, or the account default for the mode (the deployment's with
 * nothing saved) followed until a pick; a pick handed to the host or held for
 * the next send, never written to the account; and every send naming exactly
 * the model the pill shows.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, fireEvent, screen } from '@testing-library/react';
import { createRef, type Ref } from 'react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import ChatInput, { type ChatInputHandle } from '../chat-input';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  prefsLoaded: true,
  modelsLoading: false,
  validModelNames: new Set<string>(),
  systemDefaults: { default_model: 'model-default', flash_model: 'model-flash-default' },
  mutateAsync: vi.fn(),
  mutate: vi.fn(),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: mocks.prefsLoaded }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    modelAccessMap: undefined,
    validModelNames: mocks.validModelNames,
    metadata: { 'model-default': { reasoning_efforts: ['low', 'high'], reasoning_effort_default: 'high' } },
    isLoading: mocks.modelsLoading,
    systemDefaults: mocks.systemDefaults,
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: mocks.mutateAsync, mutate: mocks.mutate }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(),
}));

// The menu's own rendering is not under test; the buttons reach the same
// onSelectModel the dropdown items call.
vi.mock('../chat-input.modelMenu', () => ({
  ChatInputModelMenu: ({ onSelectModel, disabled, selectedModel, reasoningEfforts }: {
    onSelectModel: (m: string) => void;
    disabled?: boolean;
    selectedModel: string | null;
    reasoningEfforts: string[];
  }) => (
    <>
      <span>{disabled ? 'menu-disabled' : 'menu-enabled'}</span>
      <span>{`pill:${selectedModel}`}</span>
      <span>{`efforts:${reasoningEfforts.join(',')}`}</span>
      <button type="button" onClick={() => onSelectModel('model-beta')}>pick-beta</button>
      <button type="button" onClick={() => onSelectModel('model-gamma')}>pick-gamma</button>
      <button type="button" onClick={() => onSelectModel('model-default')}>pick-default</button>
    </>
  ),
  ModelTriggerMeasure: () => null,
}));

let queryClient: QueryClient;

interface TreeProps {
  onSend?: (...args: unknown[]) => void;
  onPickModel?: (model: string) => void;
  /** Set for a thread host; left out, the composer holds its own model. */
  model?: string | null;
  mode?: 'fast' | 'ptc';
  ref?: Ref<ChatInputHandle>;
}

function tree({ onSend = vi.fn(), onPickModel, model, mode, ref }: TreeProps = {}) {
  if (!queryClient.getQueryData(queryKeys.user.preferences())) {
    queryClient.setQueryData(queryKeys.user.preferences(), mocks.preferences);
  }
  return (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <ChatInput
          ref={ref}
          onSend={onSend}
          onPickModel={onPickModel}
          model={model}
          mode={mode}
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

/** Whatever the preference cache holds after a test, no account model key may
 *  have been written by a pick. */
function expectNoModelPreferenceWrite() {
  for (const fn of [mocks.mutateAsync, mocks.mutate]) {
    for (const [patch] of fn.mock.calls) {
      const model = (patch as { model_preference?: Record<string, unknown> }).model_preference ?? {};
      expect(model).not.toHaveProperty('preferred_model');
      expect(model).not.toHaveProperty('preferred_flash_model');
    }
  }
}

describe('ChatInput: the thread owns the model', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    mocks.preferences = { model_preference: { preferred_model: 'model-alpha' } };
    mocks.prefsLoaded = true;
    mocks.modelsLoading = false;
    mocks.validModelNames = new Set();
    mocks.systemDefaults = { default_model: 'model-default', flash_model: 'model-flash-default' };
    mocks.mutateAsync.mockReset();
    mocks.mutate.mockReset();
    localStorage.clear();
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it("shows the host's model as given, reachable or not", () => {
    mocks.validModelNames = new Set(['model-alpha']);
    const { rerender } = render(tree({ model: 'model-thread', onPickModel: vi.fn() }));
    expect(screen.getByText('pill:model-thread')).toBeInTheDocument();

    rerender(tree({ model: 'model-moved', onPickModel: vi.fn() }));
    expect(screen.getByText('pill:model-moved')).toBeInTheDocument();
  });

  it('hands a pick to the host and leaves the pill to the model it passes back', () => {
    const onPickModel = vi.fn();
    const { rerender } = render(tree({ model: 'model-thread', onPickModel }));

    fireEvent.click(screen.getByText('pick-beta'));
    expect(onPickModel).toHaveBeenCalledWith('model-beta');
    expect(screen.getByText('pill:model-thread')).toBeInTheDocument();

    rerender(tree({ model: 'model-beta', onPickModel }));
    expect(screen.getByText('pill:model-beta')).toBeInTheDocument();
    expectNoModelPreferenceWrite();
  });

  it("sends the host's model, from a send and from getModelOptions", () => {
    const onSend = vi.fn();
    const ref = createRef<ChatInputHandle>();
    const onPickModel = vi.fn();
    const { rerender } = render(tree({ onSend, ref, model: 'model-thread', onPickModel }));

    send('hello');
    expect(onSend.mock.calls[0][4]).toMatchObject({ model: 'model-thread' });

    rerender(tree({ onSend, ref, model: 'model-beta', onPickModel }));
    expect(ref.current?.getModelOptions()).toMatchObject({ model: 'model-beta' });
    send('again');
    expect(onSend.mock.calls[1][4]).toMatchObject({ model: 'model-beta' });
  });

  it('opens on the account default with no host, and follows it when it changes', () => {
    const { rerender } = render(tree());
    expect(screen.getByText('pill:model-alpha')).toBeInTheDocument();

    mocks.preferences = { model_preference: { preferred_model: 'model-gamma' } };
    rerender(tree());
    expect(screen.getByText('pill:model-gamma')).toBeInTheDocument();
  });

  it('names and sends the deployment default for the mode when nothing is saved', () => {
    mocks.preferences = { model_preference: {} };
    const ref = createRef<ChatInputHandle>();
    const { rerender } = render(tree({ mode: 'ptc', ref }));
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();
    // The controls are the named model's, so its ladder is offered.
    expect(screen.getByText('efforts:low,high')).toBeInTheDocument();
    expect(ref.current?.getModelOptions()).toMatchObject({ model: 'model-default' });

    rerender(tree({ mode: 'fast', ref }));
    expect(screen.getByText('pill:model-flash-default')).toBeInTheDocument();
    expect(ref.current?.getModelOptions()).toMatchObject({ model: 'model-flash-default' });
  });

  it('names the primary default on a flash composer when the deployment names no flash model', () => {
    mocks.preferences = { model_preference: {} };
    mocks.systemDefaults = { default_model: 'model-default', flash_model: '' };
    render(tree({ mode: 'fast' }));
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();
  });

  it("holds a pick with no host until the first send carries it, as a composer before its thread exists does", () => {
    const onSend = vi.fn();
    render(tree({ onSend }));

    fireEvent.click(screen.getByText('pick-beta'));
    expect(screen.getByText('pill:model-beta')).toBeInTheDocument();
    send('start a thread');
    expect(onSend.mock.calls[0][4]).toMatchObject({ model: 'model-beta' });
    expectNoModelPreferenceWrite();
  });

  it('does nothing for a pick of the model the pill already shows', () => {
    mocks.preferences = { model_preference: {} };
    const onPickModel = vi.fn();
    render(tree({ onPickModel, model: 'model-default', mode: 'ptc' }));

    fireEvent.click(screen.getByText('pick-default'));
    expect(onPickModel).not.toHaveBeenCalled();
  });

  it('sends no model while the preference is unknown, then the default and its tuning', () => {
    mocks.preferences = null;
    mocks.prefsLoaded = false;
    const ref = createRef<ChatInputHandle>();
    const { rerender } = render(tree({ mode: 'ptc', ref }));
    expect(ref.current?.getModelOptions()).toMatchObject({ model: null, reasoningEffort: null });

    mocks.preferences = { model_preference: {} };
    mocks.prefsLoaded = true;
    queryClient.setQueryData(queryKeys.user.preferences(), mocks.preferences);
    rerender(tree({ mode: 'ptc', ref }));
    expect(ref.current?.getModelOptions()).toMatchObject({ model: 'model-default', reasoningEffort: 'high' });
  });

  it('names the deployment defaults for an account with no preferences row', () => {
    // A reset deletes the row and the read answers 404, which is a known answer:
    // nothing is saved, so the server runs the deployment's defaults.
    mocks.preferences = null;
    const ref = createRef<ChatInputHandle>();
    const { rerender } = render(tree({ mode: 'ptc', ref }));
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();
    expect(ref.current?.getModelOptions()).toMatchObject({ model: 'model-default' });

    rerender(tree({ mode: 'fast', ref }));
    expect(screen.getByText('pill:model-flash-default')).toBeInTheDocument();
  });

  it('lifts device-local tuning for the default when nothing is saved', () => {
    mocks.preferences = { model_preference: {} };
    localStorage.setItem('reasoning_effort:model-default', 'low');
    render(tree({ mode: 'ptc' }));
    expect(mocks.mutate).toHaveBeenCalledWith(
      { model_preference: { profiles: { 'model-default': { reasoning_effort: 'low' } } } },
      expect.anything(),
    );
  });

  it('holds the model menu shut until the model list has loaded', () => {
    mocks.modelsLoading = true;
    const { rerender } = render(tree());
    expect(screen.getByText('menu-disabled')).toBeInTheDocument();

    mocks.modelsLoading = false;
    rerender(tree());
    expect(screen.getByText('menu-enabled')).toBeInTheDocument();
  });
});
