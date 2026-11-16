import { act, renderHook } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { useHeldModifier } from '../useHeldModifier';

const key = (type: 'keydown' | 'keyup', init: KeyboardEventInit) => act(() => {
  window.dispatchEvent(new KeyboardEvent(type, init));
});

describe('whether Cmd is held over a draft that takes parts', () => {
  it('forgets a Cmd whose release it stopped listening for', () => {
    const { result, rerender } = renderHook(({ active }) => useHeldModifier(active, false), { initialProps: { active: true } });
    key('keydown', { key: 'Meta', metaKey: true });
    expect(result.current).toBe(true);
    // A save with Cmd-Enter ends the draft before Cmd comes up.
    rerender({ active: false });
    rerender({ active: true });
    expect(result.current).toBe(false);
  });

  it('takes Cmd with another key as a shortcut being typed, not a part being picked', () => {
    const { result } = renderHook(() => useHeldModifier(true, false));
    key('keydown', { key: 'Meta', metaKey: true });
    key('keydown', { key: 'z', metaKey: true });
    expect(result.current).toBe(false);
  });

  it('takes the page\'s word for keys pressed with the focus in the frame', () => {
    const { result, rerender } = renderHook(({ page }) => useHeldModifier(true, page), { initialProps: { page: false } });
    rerender({ page: true });
    expect(result.current).toBe(true);
    key('keyup', { key: 'Meta' });
    expect(result.current).toBe(false);
  });
});
