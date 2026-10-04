/**
 * A first run fills both defaults: an empty flash slot takes a model to go
 * with the primary. The fill is a local pick like any other, so it stays put
 * when the primary changes and nothing is saved before Continue.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

const h = vi.hoisted(() => ({
  mutateAsync: vi.fn(async (_payload: unknown) => ({})),
  preferences: null as Record<string, unknown> | null,
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {
      anthropic: { models: ['claude-big', 'claude-small'] },
      openai: { models: ['gpt-sol', 'gpt-terra'] },
    },
    metadata: {},
    modelAccessMap: {},
    isLoading: false,
  }),
}));
vi.mock('@/hooks/usePreferences', () => ({ usePreferences: () => ({ preferences: h.preferences }) }));
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

const primary = () => screen.getByRole('combobox', { name: 'Primary Model' }) as HTMLSelectElement;
const flash = () => screen.getByRole('combobox', { name: 'Flash Model' }) as HTMLSelectElement;

beforeEach(() => {
  h.mutateAsync.mockClear();
  h.preferences = null;
});

describe('DefaultsStep flash fill', () => {
  it('fills an empty flash slot beside a saved primary without saving it', async () => {
    h.preferences = { model_preference: { preferred_model: 'claude-big' } };
    renderWithProviders(<DefaultsStep />);

    await waitFor(() => expect(flash().value).toBe('claude-small'));
    expect(h.mutateAsync).not.toHaveBeenCalled();
  });

  it('keeps the fill when the primary changes, and fills again only once the slot is cleared', async () => {
    renderWithProviders(<DefaultsStep />);
    expect(flash().value).toBe('');

    fireEvent.change(primary(), { target: { value: 'claude-big' } });
    await waitFor(() => expect(flash().value).toBe('claude-small'));

    fireEvent.change(primary(), { target: { value: 'gpt-sol' } });
    expect(flash().value).toBe('claude-small');

    fireEvent.change(flash(), { target: { value: '' } });
    await waitFor(() => expect(flash().value).toBe('gpt-terra'));
    expect(h.mutateAsync).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Continue' }));
    await waitFor(() => expect(h.mutateAsync).toHaveBeenCalledWith({
      model_preference: {
        preferred_model: 'gpt-sol',
        preferred_flash_model: 'gpt-terra',
        compaction_model: 'gpt-terra',
        fetch_model: 'gpt-terra',
        fallback_models: [],
      },
    }));
  });
});
