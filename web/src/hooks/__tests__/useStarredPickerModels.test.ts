import { describe, it, expect } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useStarredPickerModels } from '../useStarredPickerModels';
import type { ProviderModelsData } from '@/components/model/types';

const models = (entries: Record<string, string[]>): Record<string, ProviderModelsData> =>
  Object.fromEntries(
    Object.entries(entries).map(([k, m]) => [k, { models: m } as ProviderModelsData]),
  );

describe('useStarredPickerModels', () => {
  it('passes through the full map when starred is empty (fresh account)', () => {
    const input = models({ openai: ['gpt-5.4', 'gpt-4o'], anthropic: ['claude-5'] });

    const { result } = renderHook(() => useStarredPickerModels(input, [], [null, null]));

    expect(result.current).toBe(input);
  });

  it('filters each group down to the starred set', () => {
    const input = models({ openai: ['gpt-5.4', 'gpt-4o'], anthropic: ['claude-5'] });

    const { result } = renderHook(() =>
      useStarredPickerModels(input, ['gpt-5.4'], [null, null]),
    );

    expect(result.current.openai.models).toEqual(['gpt-5.4']);
    expect(result.current.anthropic).toBeUndefined();
  });

  it('keeps current picks even when they are not in starred — avoids mid-edit vanish', () => {
    const input = models({ openai: ['gpt-5.4', 'gpt-4o'] });

    const { result } = renderHook(() =>
      useStarredPickerModels(input, ['gpt-5.4'], ['gpt-4o', null]),
    );

    expect(result.current.openai.models?.sort()).toEqual(['gpt-4o', 'gpt-5.4']);
  });

  it('omits groups whose models are all filtered out', () => {
    const input = models({
      openai: ['gpt-5.4'],
      anthropic: ['claude-5'],
      google: ['gemini-3'],
    });

    const { result } = renderHook(() =>
      useStarredPickerModels(input, ['gpt-5.4', 'claude-5'], [null, null]),
    );

    expect(Object.keys(result.current).sort()).toEqual(['anthropic', 'openai']);
  });

  it('memo result is stable when picks reorder but equal-by-value', () => {
    const input = models({ openai: ['gpt-5.4', 'gpt-4o'] });
    const starred = ['gpt-5.4'];

    const { result, rerender } = renderHook(
      ({ picks }: { picks: (string | null)[] }) =>
        useStarredPickerModels(input, starred, picks),
      { initialProps: { picks: ['gpt-5.4', null] } },
    );
    const first = result.current;

    // Identity change but same values — picksKey collapses to the same string.
    rerender({ picks: ['gpt-5.4', null] });

    expect(result.current).toBe(first);
  });
});
