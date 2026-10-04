/**
 * A default change answers one question the server cannot: whether threads on
 * the old default move with it. These pin the body each answer sends, when
 * the question is asked at all, and what the toast and the thread caches do
 * with the server's count.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { useDefaultModelChange } from '../useDefaultModelChange';

const mocks = vi.hoisted(() => ({
  preferences: null as unknown,
  systemDefaults: { default_model: 'deploy-default', flash_model: 'deploy-flash' } as Record<string, string> | null,
  put: vi.fn(),
  toast: vi.fn(),
}));

vi.mock('../usePreferences', () => ({
  usePreferences: () => ({ preferences: mocks.preferences, isLoading: false, isLoaded: true }),
}));

vi.mock('../useAllModels', () => ({
  useAllModels: () => ({ systemDefaults: mocks.systemDefaults }),
}));

// A real mutation over a stubbed request, so `saving` is the mutation's own
// pending state rather than a value the stub hands back.
vi.mock('../useUpdatePreferences', async () => {
  const { useMutation } = await import('@tanstack/react-query');
  return {
    useUpdatePreferences: () => useMutation({
      mutationFn: (body: Record<string, unknown>) => mocks.put(body) as Promise<Record<string, unknown>>,
    }),
  };
});

vi.mock('@/components/ui/use-toast', () => ({
  toast: mocks.toast,
}));

const label = (model: string) => `label:${model}`;

function setup() {
  return renderHookWithProviders(() => useDefaultModelChange(label));
}

type View = ReturnType<typeof setup>['result'];

async function confirm(result: View, applyTo: 'new_threads' | 'existing_threads', remember: boolean) {
  await act(async () => { result.current.question!.onConfirm(applyTo, remember); });
}

describe('useDefaultModelChange', () => {
  beforeEach(() => {
    mocks.preferences = { model_preference: { preferred_model: 'model-old' } };
    mocks.systemDefaults = { default_model: 'deploy-default', flash_model: 'deploy-flash' };
    mocks.put.mockReset();
    mocks.put.mockResolvedValue({});
    mocks.toast.mockReset();
  });

  it('asks when no answer is saved, and writes nothing until it is answered', () => {
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });

    expect(result.current.draft).toEqual({ ptc: 'model-new' });
    expect(result.current.question).toMatchObject({
      models: ['label:model-new'],
      previous: ['label:model-old'],
      saving: false,
    });
    expect(mocks.put).not.toHaveBeenCalled();

    act(() => { result.current.question!.onCancel(); });
    expect(result.current.question).toBeNull();
    expect(result.current.draft).toBeNull();
    expect(mocks.put).not.toHaveBeenCalled();
  });

  it('saves a remembered answer in the same write as the default', async () => {
    mocks.put.mockResolvedValue({ threads_reassigned: 2 });
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });
    await confirm(result, 'existing_threads', true);

    await waitFor(() => expect(result.current.question).toBeNull());
    expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'model-new', default_model_scope: 'existing_threads' },
      apply_default_to: 'existing_threads',
    });
    expect(mocks.toast).toHaveBeenCalledWith({ description: 'Default set to label:model-new. 2 threads switched.' });
  });

  it('sends a one-time answer without saving it', async () => {
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });
    await confirm(result, 'new_threads', false);

    await waitFor(() => expect(mocks.toast).toHaveBeenCalledWith({
      description: 'Default set to label:model-new for new threads.',
    }));
    expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'model-new' },
      apply_default_to: 'new_threads',
    });
  });

  it('applies a saved answer at once', async () => {
    mocks.preferences = {
      model_preference: { preferred_model: 'model-old', default_model_scope: 'existing_threads' },
    };
    mocks.put.mockResolvedValue({ threads_reassigned: 1 });
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });

    expect(result.current.question).toBeNull();
    await waitFor(() => expect(mocks.toast).toHaveBeenCalledWith({
      description: 'Default set to label:model-new. 1 thread switched.',
    }));
    expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'model-new' },
      apply_default_to: 'existing_threads',
    });
  });

  it('writes the flash default from a fast composer, measured against the flash it replaces', async () => {
    mocks.preferences = {
      model_preference: { preferred_model: 'model-old', default_model_scope: 'existing_threads' },
    };
    mocks.put.mockResolvedValue({ threads_reassigned: 0 });
    const { result } = setup();
    act(() => { result.current.request({ fast: 'model-quick' }); });

    // Fast falls through to the primary before the deployment's flash model.
    await waitFor(() => expect(mocks.toast).toHaveBeenCalledWith({
      description: 'Default set to label:model-quick. No threads were using label:model-old.',
    }));
    expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_flash_model: 'model-quick', flash_follows: null },
      apply_default_to: 'existing_threads',
    });
  });

  it('does not ask about a model the default already resolves to', async () => {
    const { result } = setup();
    act(() => { result.current.request({ fast: 'model-old' }); });

    expect(result.current.question).toBeNull();
    await waitFor(() => expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_flash_model: 'model-old', flash_follows: null },
      apply_default_to: 'new_threads',
    }));
  });

  it('joins a second pick to the open question instead of dropping the first', async () => {
    mocks.put.mockResolvedValue({ threads_reassigned: 3 });
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new', fast: 'model-fill' }); });
    act(() => { result.current.request({ fast: 'model-quick' }); });

    expect(result.current.draft).toEqual({ ptc: 'model-new', fast: 'model-quick' });
    expect(result.current.question).toMatchObject({
      models: ['label:model-new', 'label:model-quick'],
      previous: ['label:model-old'],
    });
    expect(mocks.put).not.toHaveBeenCalled();

    await confirm(result, 'existing_threads', false);
    await waitFor(() => expect(mocks.toast).toHaveBeenCalledWith({
      description: 'Defaults set to label:model-new and label:model-quick. 3 threads switched.',
    }));
    expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'model-new', preferred_flash_model: 'model-quick', flash_follows: null },
      apply_default_to: 'existing_threads',
    });
  });

  it('names a draft that changes nothing once, even when both defaults name the same model', async () => {
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });
    act(() => { result.current.request({ fast: 'model-quick' }); });
    act(() => { result.current.request({ ptc: 'model-old' }); });
    // Both now resolve to what they already were, so the draft applies as is.
    act(() => { result.current.request({ fast: 'model-old' }); });

    await waitFor(() => expect(result.current.question).toBeNull());
    expect(mocks.toast).toHaveBeenCalledWith({ description: 'Default set to label:model-old for new threads.' });
    expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_model: 'model-old', preferred_flash_model: 'model-old', flash_follows: null },
      apply_default_to: 'new_threads',
    });
  });

  it('drops the whole draft on cancel', () => {
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new', fast: 'model-fill' }); });
    act(() => { result.current.question!.onCancel(); });
    act(() => { result.current.request({ fast: 'model-quick' }); });

    expect(result.current.draft).toEqual({ fast: 'model-quick' });
    expect(result.current.question).toMatchObject({ models: ['label:model-quick'], previous: ['label:model-old'] });
  });

  it('keeps a pick made while the answer saves for a question of its own', async () => {
    let answer!: (value: Record<string, unknown>) => void;
    mocks.put.mockReturnValue(new Promise((resolve) => { answer = resolve; }));
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });
    await confirm(result, 'new_threads', false);
    await waitFor(() => expect(result.current.saving).toBe(true));
    expect(result.current.question?.saving).toBe(true);

    act(() => { result.current.request({ fast: 'model-quick' }); });
    await act(async () => { answer({}); });

    await waitFor(() => expect(result.current.saving).toBe(false));
    expect(mocks.toast).toHaveBeenCalledWith({ description: 'Default set to label:model-new for new threads.' });
    expect(result.current.draft).toEqual({ ptc: 'model-new', fast: 'model-quick' });
    expect(result.current.question).not.toBeNull();
  });

  it('clears a default without asking, and takes it out of the draft', async () => {
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new', fast: 'model-fill' }); });
    act(() => { result.current.clear('ptc'); });

    await waitFor(() => expect(mocks.put).toHaveBeenCalledWith({ model_preference: { preferred_model: null } }));
    expect(result.current.draft).toEqual({ fast: 'model-fill' });
    expect(result.current.question).toMatchObject({ models: ['label:model-fill'], previous: ['label:model-old'] });

    act(() => { result.current.clear('fast'); });
    expect(result.current.draft).toBeNull();
    expect(result.current.question).toBeNull();
    await waitFor(() => expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_flash_model: null, flash_follows: null },
    }));
    expect(mocks.toast).not.toHaveBeenCalled();
  });

  it('writes Auto for flash as following the deployment, without asking', async () => {
    const { result } = setup();
    act(() => { result.current.clear('fast', 'deployment'); });

    await waitFor(() => expect(mocks.put).toHaveBeenCalledWith({
      model_preference: { preferred_flash_model: null, flash_follows: 'deployment' },
    }));
    expect(result.current.question).toBeNull();
  });

  it('keeps the question open when the write fails', async () => {
    mocks.put.mockRejectedValue(new Error('offline'));
    const { result } = setup();
    act(() => { result.current.request({ ptc: 'model-new' }); });
    await confirm(result, 'existing_threads', false);

    await waitFor(() => expect(mocks.toast).toHaveBeenCalledWith({
      description: "Couldn't change your default model",
      variant: 'destructive',
    }));
    await waitFor(() => expect(result.current.saving).toBe(false));
    expect(result.current.question).not.toBeNull();
  });
});
