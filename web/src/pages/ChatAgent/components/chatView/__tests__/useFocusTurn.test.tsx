/** A jump to an old turn pages back toward it a bounded page at a time, and asks once more for a page that failed. */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import type { OlderHistoryStatus } from '../../../session/history/historyWindow';
import { useFocusTurn } from '../useFocusTurn';

const turns = (from: number, to: number) =>
  Array.from({ length: to - from + 1 }, (_, i) => ({ id: `u${from + i}`, role: 'user', turnIndex: from + i }));

const load = vi.fn(async () => {});
const reveal = vi.fn(() => true);
const toNewest = vi.fn();
const place = vi.fn();

function jumpTo(turn: number, messages: Record<string, unknown>[], status: OlderHistoryStatus = 'idle') {
  return renderHook(
    (props: { messages: Record<string, unknown>[]; status: OlderHistoryStatus }) =>
      useFocusTurn({
        isActive: true,
        ready: true,
        onMain: true,
        showMain: () => {},
        messages: props.messages,
        status: props.status,
        load,
        place,
        reveal,
        toNewest,
        scrollRoot: () => null,
      }),
    {
      initialProps: { messages, status },
      wrapper: ({ children }) => (
        <MemoryRouter initialEntries={[{ pathname: '/chat/t/x', state: { focusTurn: turn } }]}>{children}</MemoryRouter>
      ),
    },
  );
}

// The steps run on the next frame; the fake clock drives it.
const frame = () => act(async () => { await vi.advanceTimersByTimeAsync(20); });

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
});
afterEach(() => vi.useRealTimers());

describe('useFocusTurn', () => {
  it('asks for older pages again until the turn is in, then reveals it', async () => {
    const view = jumpTo(3, turns(400, 420));
    await frame();
    expect(load).toHaveBeenCalledTimes(1);
    expect(load).toHaveBeenLastCalledWith(place, 3);

    // A page landed short of the turn: the jump asks for the next.
    view.rerender({ messages: turns(300, 420), status: 'idle' });
    await frame();
    expect(load).toHaveBeenCalledTimes(2);

    view.rerender({ messages: turns(0, 420), status: 'idle' });
    await frame();
    expect(load).toHaveBeenCalledTimes(2);
    expect(reveal).toHaveBeenCalledWith('u3');
    expect(toNewest).not.toHaveBeenCalled();
  });

  it('asks once more for a page that failed, and gives up on a second failure', async () => {
    const view = jumpTo(3, turns(400, 420), 'failed');
    await frame();
    expect(load).not.toHaveBeenCalled();
    expect(toNewest).not.toHaveBeenCalled();

    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    expect(load).toHaveBeenCalledTimes(1);

    view.rerender({ messages: turns(400, 420), status: 'loading' });
    view.rerender({ messages: turns(400, 420), status: 'failed' });
    await act(async () => { await vi.advanceTimersByTimeAsync(1100); });
    expect(load).toHaveBeenCalledTimes(1);
    expect(toNewest).toHaveBeenCalledTimes(1);
  });

  it('asks again a second after the failure while the thread streams', async () => {
    const view = jumpTo(3, turns(400, 420), 'failed');
    // A streaming thread re-renders the view with new messages every few hundred ms.
    for (let i = 0; i < 4; i++) {
      await act(async () => { await vi.advanceTimersByTimeAsync(300); });
      view.rerender({ messages: [...turns(400, 420), { id: `s${i}`, role: 'assistant', turnIndex: 420 }], status: 'failed' });
    }
    expect(load).toHaveBeenCalledTimes(1);
    expect(load).toHaveBeenLastCalledWith(place, 3);
  });
});
