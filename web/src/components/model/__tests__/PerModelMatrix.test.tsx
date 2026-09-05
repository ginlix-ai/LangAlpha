import { render, screen } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { describe, it, expect, vi } from 'vitest';
import { PerModelMatrix } from '../PerModelMatrix';
import { queryKeys } from '@/lib/queryKeys';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';

/**
 * A tuning cell has to name what "Default" resolves to, and it reads that off
 * the preferences cache the same way the server reads the stored row. These pin
 * the three layers in order: the model's own answer, the account value that
 * outranks it, and the fail-safe when neither declares one.
 */

vi.mock('@/pages/Dashboard/utils/api', () => ({
  getPreferences: vi.fn(async () => ({})),
  updatePreferences: vi.fn(async () => ({})),
}));

const profilesCatalog = {
  aggressive: { token_threshold: 100000, keep_messages: 5, truncate_args_trigger_messages: 30 },
  moderate: { token_threshold: 130000, keep_messages: 8, truncate_args_trigger_messages: 40 },
  extended: { token_threshold: 200000, keep_messages: 10, truncate_args_trigger_messages: 60 },
  relaxed: { token_threshold: 300000, keep_messages: 15, truncate_args_trigger_messages: 70 },
};

const metadata: Record<string, ModelMetadataEntry> = {
  'wide-model': { provider: 'p', compaction_profile: 'relaxed' },
  'narrow-model': { provider: 'p', compaction_profile: 'moderate' },
  'unknown-window': { provider: 'p' },
  'lean-model': { provider: 'p', prompt_guidance: 'lean' },
};

/** A row only appears for a model that already carries some override, so give
 *  each one an unrelated setting and leave the cell under test inheriting. */
const PROFILES = Object.fromEntries(
  Object.keys(metadata).map((m) => [m, { reasoning_effort: 'high' }]),
);

function renderMatrix(account: Record<string, unknown> = {}, over: { validModelNames?: Set<string> } = {}) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity } },
  });
  queryClient.setQueryData(queryKeys.user.preferences(), {
    model_preference: { ...account, profiles: PROFILES },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <PerModelMatrix
        metadata={metadata}
        validModelNames={over.validModelNames ?? new Set(Object.keys(metadata))}
        compactionProfiles={profilesCatalog}
      />
    </QueryClientProvider>,
  );
}

/** The placeholder option (value "") is the "what do I get if I leave this
 *  alone" surface; the other options are the override list and always present. */
const inheritedLabel = (model: string, column: string) => {
  const cell = screen.getByLabelText(`${model} ${column}`) as HTMLSelectElement;
  return [...cell.options].find((o) => o.value === '')?.textContent ?? '';
};

describe('PerModelMatrix context cell', () => {
  it('names the preset each model derives, rather than a bare Default', () => {
    renderMatrix();
    expect(inheritedLabel('wide-model', 'Context')).toBe('Default (Relaxed)');
    expect(inheritedLabel('narrow-model', 'Context')).toBe('Default (Moderate)');
  });

  it('stays a bare Default when the model declares no window', () => {
    renderMatrix();
    expect(inheritedLabel('unknown-window', 'Context')).toBe('Default');
  });

  it('lets the account default outrank what the model derives', () => {
    renderMatrix({ compaction_profile: 'aggressive' });
    expect(inheritedLabel('wide-model', 'Context')).toBe('Default (Aggressive)');
  });
});

describe('PerModelMatrix guidance cell', () => {
  it('names the level the model declares for itself', () => {
    renderMatrix();
    expect(inheritedLabel('lean-model', 'Guidance')).toBe('Default (Concise)');
  });

  it('falls back to the server fail-safe when nobody declares one', () => {
    renderMatrix();
    expect(inheritedLabel('wide-model', 'Guidance')).toBe('Default (Thorough)');
  });

  it('lets the account default outrank the model declaration', () => {
    renderMatrix({ prompt_guidance: 'lean' });
    expect(inheritedLabel('wide-model', 'Guidance')).toBe('Default (Concise)');
  });
});

describe('PerModelMatrix rows', () => {
  it('keeps an unreachable profile on screen, inert but resettable', () => {
    // Hiding it left overrides the user could neither see nor clear, and
    // nothing scrubs them: the server sweep only fires on an unresolvable
    // *selected* model, so a profile on its own never reaches it. They are
    // live again the moment the model is, which is why the row cannot just
    // disappear. Its cells have no ladder to offer, so it states that and
    // keeps only the reset.
    renderMatrix({}, { validModelNames: new Set(['wide-model']) });
    expect(screen.getByText('wide-model')).toBeTruthy();
    expect(screen.getByText('narrow-model')).toBeTruthy();
    expect(screen.getByLabelText('Remove overrides narrow-model')).toBeTruthy();
    expect(screen.queryByLabelText('narrow-model Effort')).toBeNull();
    // The reachable row still renders its controls.
    expect(screen.getByLabelText('wide-model Effort')).toBeTruthy();
  });
});
