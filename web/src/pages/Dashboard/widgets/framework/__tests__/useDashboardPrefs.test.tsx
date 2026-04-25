import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '../../../../../test/utils';

const prefsState: { current: { other_preference?: Record<string, unknown> } | null } = {
  current: { other_preference: { theme: 'dark' } },
};
const mockMutate = vi.fn();
const mockToast = vi.fn();

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: prefsState.current, isLoading: false }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: mockMutate }),
}));
vi.mock('@/components/ui/use-toast', () => ({
  useToast: () => ({ toast: mockToast, dismiss: vi.fn(), toasts: [] }),
}));

import { useDashboardPrefs } from '../useDashboardPrefs';

describe('useDashboardPrefs', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockMutate.mockReset();
    mockToast.mockReset();
    prefsState.current = { other_preference: { theme: 'dark' } };
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('debounces non-immediate writes by 800ms', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs());
    act(() => {
      result.current.update({ mode: 'custom' });
    });
    expect(mockMutate).not.toHaveBeenCalled();
    act(() => {
      vi.advanceTimersByTime(800);
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('flushes immediately when {immediate:true}', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs());
    act(() => {
      result.current.setMode('custom');
    });
    // setMode uses immediate:true under the hood.
    expect(mockMutate).toHaveBeenCalledTimes(1);
  });

  it('preserves sibling other_preference keys (theme) when flushing', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs());
    act(() => {
      result.current.setMode('custom');
    });
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { theme?: string; dashboard?: { mode: string } };
    };
    expect(payload.other_preference.theme).toBe('dark');
    expect(payload.other_preference.dashboard?.mode).toBe('custom');
  });

  it('shows a destructive toast when the server rejects the write', () => {
    mockMutate.mockImplementation((_payload, opts: { onError: () => void }) => {
      opts.onError();
    });
    const { result } = renderHookWithProviders(() => useDashboardPrefs());
    act(() => {
      result.current.setMode('custom');
    });
    expect(mockToast).toHaveBeenCalledTimes(1);
    expect(mockToast.mock.calls[0][0]).toMatchObject({ variant: 'destructive' });
  });

  it('caps history at 3 prior layouts on repeated applyPreset', () => {
    const { result } = renderHookWithProviders(() => useDashboardPrefs());
    act(() => {
      result.current.applyPreset('morning-brief');
    });
    act(() => {
      result.current.applyPreset('trader');
    });
    act(() => {
      result.current.applyPreset('researcher');
    });
    act(() => {
      result.current.applyPreset('agent-desk');
    });
    expect(result.current.prefs.history?.length ?? 0).toBeLessThanOrEqual(3);
  });
});
