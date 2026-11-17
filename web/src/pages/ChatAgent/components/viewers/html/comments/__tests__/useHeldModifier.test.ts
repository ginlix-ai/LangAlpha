import { act, renderHook } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import { ADD_KEY } from '../../../comments/partModifier';
import { useHeldModifier } from '../useHeldModifier';

// The add key and its flag, whichever this platform uses.
const ADD = { key: ADD_KEY, metaKey: ADD_KEY === 'Meta', ctrlKey: ADD_KEY === 'Control' };

const key = (type: 'keydown' | 'keyup', init: KeyboardEventInit) => act(() => {
  window.dispatchEvent(new KeyboardEvent(type, init));
});

describe('whether the add key is held over a draft that takes parts', () => {
  it('forgets an add key whose release it stopped listening for', () => {
    const { result, rerender } = renderHook(({ active }) => useHeldModifier(active, false), { initialProps: { active: true } });
    key('keydown', ADD);
    expect(result.current).toBe(true);
    // A save with the add key and Enter ends the draft before the key comes up.
    rerender({ active: false });
    rerender({ active: true });
    expect(result.current).toBe(false);
  });

  it('takes the add key with another key as a shortcut being typed, not a part being picked', () => {
    const { result } = renderHook(() => useHeldModifier(true, false));
    key('keydown', ADD);
    key('keydown', { ...ADD, key: 'z' });
    expect(result.current).toBe(false);
  });

  it('takes the page\'s word for keys pressed with the focus in the frame', () => {
    const { result, rerender } = renderHook(({ page }) => useHeldModifier(true, page), { initialProps: { page: false } });
    rerender({ page: true });
    expect(result.current).toBe(true);
    key('keyup', { key: ADD_KEY });
    expect(result.current).toBe(false);
  });
});
