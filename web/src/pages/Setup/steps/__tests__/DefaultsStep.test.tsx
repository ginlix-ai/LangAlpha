/**
 * A first run fills both defaults: the primary starts on the deployment's
 * default when the user can run it, else the first model they picked or can
 * run, and an empty Background slot takes a model to go
 * with the primary. The fill is a local pick like any other, so it stays put
 * when the primary changes and nothing is saved before Continue.
 *
 * Compaction and fetch left on their default save unset, not pinned to that
 * day's Background pick, so the server keeps resolving them from the current model
 * per turn (`compaction_name`/`fetch_name` in `src/ptc_agent/config/agent.py`).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  preferences: null as Record<string, unknown> | null,
  /** Whether the preferences read has come back. */
  loaded: true,
  /** Whether it is still in flight; not loaded and not loading is a failed read. */
  loading: false,
  systemDefaults: null as { default_model: string } | null,
  models: {
    anthropic: { models: ['claude-big', 'claude-small'] },
    openai: { models: ['gpt-sol', 'gpt-terra'] },
  } as Record<string, { models: string[] }>,
  /** What the slots list; null lists every model. */
  slotModels: null as Record<string, { models: string[] }> | null,
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: h.models,
    slotModels: h.slotModels ?? h.models,
    metadata: {},
    payers: {},
    systemDefaults: h.systemDefaults,
    isLoading: false,
  }),
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.loaded ? h.preferences : null, isLoaded: h.loaded, isLoading: h.loading }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: h.mutateAsync }),
}));
vi.mock('@/components/model/ModelSelector', () => ({
  ModelSelector: (props: {
    label: string;
    value: string;
    onChange: (v: string) => void;
    models: Record<string, { models?: string[] }>;
    placeholder?: string;
  }) => (
    <select aria-label={props.label} value={props.value} onChange={(e) => props.onChange(e.target.value)}>
      <option value="">{props.placeholder}</option>
      {Object.values(props.models).flatMap((p) => p.models ?? []).map((m) => (
        <option key={m} value={m}>{m}</option>
      ))}
    </select>
  ),
}));
vi.mock('@/components/model/FallbackModelsPicker', () => ({
  FallbackModelsPicker: () => null,
}));

import DefaultsStep from '../DefaultsStep';

const primary = () => screen.getByRole('combobox', { name: 'Default model' }) as HTMLSelectElement;
const background = () => screen.getByRole('combobox', { name: 'Background model' }) as HTMLSelectElement;

beforeEach(() => {
  h.mutateAsync.mockClear();
  h.slotModels = null;
  h.preferences = null;
  h.systemDefaults = null;
  h.loaded = true;
  h.loading = false;
});

describe('DefaultsStep first-run primary', () => {
  it("starts on the deployment's default when the user can run it", () => {
    h.systemDefaults = { default_model: 'gpt-sol' };
    renderWithProviders(<DefaultsStep />);
    expect(primary().value).toBe('gpt-sol');
    expect(background().value).toBe('gpt-terra');
  });

  it('starts on the first model the user picked when the default is out of reach', () => {
    h.systemDefaults = { default_model: 'deployment-only' };
    h.preferences = { other_preference: { starred_models: ['gpt-terra'] } };
    renderWithProviders(<DefaultsStep />);
    expect(primary().value).toBe('gpt-terra');
  });

  // A conversation-only model (Claude Code) is the user's to choose, never
  // the fill's, even as the deployment's default or the first one listed.
  it('fills from the slot models, leaving a conversation-only model to a pick by hand', () => {
    h.models = { 'claude-code': { models: ['claude-code-row'] }, anthropic: { models: ['claude-big'] } };
    h.slotModels = { anthropic: { models: ['claude-big'] } };
    h.systemDefaults = { default_model: 'claude-code-row' };
    try {
      renderWithProviders(<DefaultsStep />);
      expect(primary().value).toBe('claude-big');
      fireEvent.change(primary(), { target: { value: 'claude-code-row' } });
      expect(primary().value).toBe('claude-code-row');
    } finally {
      h.models = { anthropic: { models: ['claude-big', 'claude-small'] }, openai: { models: ['gpt-sol', 'gpt-terra'] } };
    }
  });

  it('keeps a saved default', () => {
    h.systemDefaults = { default_model: 'gpt-sol' };
    h.preferences = { model_preference: { preferred_model: 'claude-small' } };
    renderWithProviders(<DefaultsStep />);
    expect(primary().value).toBe('claude-small');
  });
});

describe('DefaultsStep background fill', () => {
  it('fills an empty Background slot beside a saved primary without saving it', async () => {
    h.preferences = { model_preference: { preferred_model: 'claude-big' } };
    renderWithProviders(<DefaultsStep />);

    await waitFor(() => expect(background().value).toBe('claude-small'));
    expect(h.mutateAsync).not.toHaveBeenCalled();
  });

  it('keeps the fill when the primary changes, and fills again only once the slot is cleared', async () => {
    renderWithProviders(<DefaultsStep />);
    expect(primary().value).toBe('claude-big');
    expect(background().value).toBe('claude-small');

    fireEvent.change(primary(), { target: { value: 'gpt-sol' } });
    expect(background().value).toBe('claude-small');

    fireEvent.change(background(), { target: { value: '' } });
    await waitFor(() => expect(background().value).toBe('gpt-terra'));
    expect(h.mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledWith({
      model_preference: {
        preferred_model: 'gpt-sol',
        preferred_background_model: 'gpt-terra',
        compaction_model: null,
        fetch_model: null,
        fallback_models: [],
      },
    }));
  });
});

describe('DefaultsStep compaction and fetch', () => {
  it('saves an explicit advanced pick as-is, not folded into the Background pick', async () => {
    renderWithProviders(<DefaultsStep />);
    fireEvent.change(primary(), { target: { value: 'gpt-sol' } });
    expect(background().value).toBe('claude-small');

    fireEvent.click(screen.getByRole('button', { name: 'Advanced' }));
    fireEvent.change(screen.getByRole('combobox', { name: 'Web fetch model' }), {
      target: { value: 'gpt-sol' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));

    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledTimes(1));
    expect(h.mutateAsync.mock.calls[0][0]).toMatchObject({
      model_preference: { fetch_model: 'gpt-sol', compaction_model: null },
    });
  });
});

describe('DefaultsStep before the preferences arrive', () => {
  it('fills nothing and saves nothing until they do, then opens on the saved defaults', async () => {
    h.systemDefaults = { default_model: 'gpt-sol' };
    h.loaded = false;
    h.loading = true;
    h.preferences = {
      model_preference: {
        preferred_model: 'claude-small',
        preferred_background_model: 'gpt-terra',
        fallback_models: ['claude-big'],
        fetch_model: 'gpt-sol',
      },
    };
    const { rerender } = renderWithProviders(<DefaultsStep />);

    expect(primary().value).toBe('');
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
    expect(screen.queryByRole('alert')).toBeNull();

    h.loaded = true;
    h.loading = false;
    rerender(<DefaultsStep />);

    expect(primary().value).toBe('claude-small');
    expect(background().value).toBe('gpt-terra');
    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledWith({
      model_preference: {
        preferred_model: 'claude-small',
        preferred_background_model: 'gpt-terra',
        compaction_model: null,
        fetch_model: 'gpt-sol',
        fallback_models: ['claude-big'],
      },
    }));
  });

  it('says so when the read fails, and still saves nothing', () => {
    h.loaded = false;
    renderWithProviders(<DefaultsStep />);

    expect(screen.getByRole('alert')).toHaveTextContent('Failed to load model settings');
    expect(screen.getByRole('button', { name: 'Continue' })).toBeDisabled();
  });
});
