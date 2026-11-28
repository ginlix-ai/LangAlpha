import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  STATUS_HOLD_MS,
  SUBAGENT_BETWEEN_GRACE_MS,
  SUBAGENT_STEP_DWELL_MS,
  SUBAGENT_STEP_LINGER_MS,
} from '../../liveZoneTiming';
import { useSteadyStep } from '../useSteadyStep';

describe('useSteadyStep', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  type Props = { step: string; steady: boolean; between?: boolean };
  const render = (step: string, steady = true) => {
    const hook = renderHook(
      (p: Props) => useSteadyStep({ text: p.step, ...(p.between ? { between: true as const } : {}) }, p.steady).text,
      { initialProps: { step, steady } as Props },
    );
    return hook;
  };

  it('takes a step at once onto an empty line', () => {
    const { result, rerender } = render('');
    rerender({ step: 'Searching', steady: true });
    expect(result.current).toBe('Searching');
  });

  it('holds a step for its dwell before a newer one takes the line', () => {
    const { result, rerender } = render('Searching');
    rerender({ step: 'Fetching', steady: true });
    act(() => { vi.advanceTimersByTime(SUBAGENT_STEP_DWELL_MS - 100); });
    expect(result.current).toBe('Searching');
    act(() => { vi.advanceTimersByTime(100); });
    expect(result.current).toBe('Fetching');
  });

  it('never shows a call that returned within the hold', () => {
    const { result, rerender } = render('Searching');
    act(() => { vi.advanceTimersByTime(SUBAGENT_STEP_DWELL_MS); });
    rerender({ step: 'Running a command', steady: true });
    act(() => { vi.advanceTimersByTime(STATUS_HOLD_MS - 10); });
    rerender({ step: '', steady: true });
    act(() => { vi.advanceTimersByTime(SUBAGENT_STEP_LINGER_MS); });
    expect(result.current).toBe('');
  });

  it('keeps a step through the gap between two calls', () => {
    const { result, rerender } = render('Searching');
    rerender({ step: '', steady: true });
    act(() => { vi.advanceTimersByTime(SUBAGENT_STEP_LINGER_MS - 100); });
    expect(result.current).toBe('Searching');
    rerender({ step: 'Fetching', steady: true });
    act(() => { vi.advanceTimersByTime(STATUS_HOLD_MS); });
    expect(result.current).toBe('Fetching');
  });

  it('follows the line as given once the run is not working', () => {
    const { result, rerender } = render('Searching');
    rerender({ step: 'Margins held', steady: false });
    expect(result.current).toBe('Margins held');
  });

  it('names the time between calls only once it lasts', () => {
    const { result, rerender } = render('Searching');
    act(() => { vi.advanceTimersByTime(SUBAGENT_STEP_DWELL_MS); });
    rerender({ step: 'Thinking', steady: true, between: true });
    act(() => { vi.advanceTimersByTime(SUBAGENT_BETWEEN_GRACE_MS - 10); });
    expect(result.current).toBe('Searching');
    act(() => { vi.advanceTimersByTime(10); });
    expect(result.current).toBe('Thinking');
  });

  it('never shows a quick turn between two calls', () => {
    const { result, rerender } = render('Searching');
    act(() => { vi.advanceTimersByTime(SUBAGENT_STEP_DWELL_MS); });
    rerender({ step: 'Thinking', steady: true, between: true });
    act(() => { vi.advanceTimersByTime(SUBAGENT_BETWEEN_GRACE_MS / 2); });
    rerender({ step: 'Fetching', steady: true, between: false });
    act(() => { vi.advanceTimersByTime(STATUS_HOLD_MS); });
    expect(result.current).toBe('Fetching');
  });
});
