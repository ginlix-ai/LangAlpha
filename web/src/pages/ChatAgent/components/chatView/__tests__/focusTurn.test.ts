// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { focusTurnStep, turnOpenerId } from '../focusTurn';

const user = (turn: number, extra: Record<string, unknown> = {}) => ({ id: `u${turn}`, role: 'user', turnIndex: turn, ...extra });
const reply = (turn: number) => ({ id: `a${turn}`, role: 'assistant', turnIndex: turn });

describe('turnOpenerId', () => {
  it('names the message that opens the turn, not a steer inside it', () => {
    const msgs = [user(3), reply(3), { id: 's3', role: 'user', turnIndex: 4, steering: true }, user(4), reply(4)];
    expect(turnOpenerId(msgs, 4)).toBe('u4');
  });

  it('falls back to the first bubble of a turn with no opener drawn (a wakeup row)', () => {
    const msgs = [user(1), reply(1), { id: 'n2', role: 'notification', turnIndex: 2 }, reply(2)];
    expect(turnOpenerId(msgs, 2)).toBe('a2');
  });
});

describe('focusTurnStep', () => {
  it('reveals a turn that is loaded', () => {
    expect(focusTurnStep([user(5), reply(5), user(6)], 5, 'idle')).toEqual({ kind: 'reveal', id: 'u5' });
  });

  it('pages back while the turn is older than the oldest loaded one', () => {
    const msgs = [user(40), reply(40), user(41)];
    expect(focusTurnStep(msgs, 7, 'idle')).toEqual({ kind: 'load' });
    expect(focusTurnStep(msgs, 7, 'loading')).toEqual({ kind: 'wait' });
    expect(focusTurnStep(msgs, 7, 'reloading')).toEqual({ kind: 'wait' });
    expect(focusTurnStep(msgs, 7, 'held')).toEqual({ kind: 'wait' });
  });

  it('gives up at the start of the thread, and names a page that failed apart from it', () => {
    const msgs = [user(40), reply(40)];
    expect(focusTurnStep(msgs, 7, 'end')).toEqual({ kind: 'missing' });
    expect(focusTurnStep(msgs, 7, 'failed')).toEqual({ kind: 'failed' });
  });

  it('calls a turn missing once the transcript reaches past it without it', () => {
    // A fork cut turn 4, or the index is past the newest turn.
    const msgs = [user(3), reply(3), user(5), reply(5)];
    expect(focusTurnStep(msgs, 4, 'idle')).toEqual({ kind: 'missing' });
    expect(focusTurnStep(msgs, 9, 'idle')).toEqual({ kind: 'missing' });
  });

  it('waits on a transcript still loading, and gives up on an empty thread', () => {
    expect(focusTurnStep([], 2, 'reloading')).toEqual({ kind: 'wait' });
    expect(focusTurnStep([], 2, 'end')).toEqual({ kind: 'missing' });
  });
});
