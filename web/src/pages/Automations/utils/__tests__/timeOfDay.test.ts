// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { parseTime, stepTime } from '../timeOfDay';

describe('parseTime', () => {
  it.each([
    ['14:15', { hour: 14, minute: 15 }],
    ['2:15 pm', { hour: 14, minute: 15 }],
    ['2:15p', { hour: 14, minute: 15 }],
    ['215p', { hour: 14, minute: 15 }],
    ['2p', { hour: 14, minute: 0 }],
    ['12am', { hour: 0, minute: 0 }],
    ['12:30 PM', { hour: 12, minute: 30 }],
    ['9.30', { hour: 9, minute: 30 }],
    ['0930', { hour: 9, minute: 30 }],
    ['下午2:15', { hour: 14, minute: 15 }],
    ['上午9点', { hour: 9, minute: 0 }],
  ])('reads %s', (text, expected) => {
    expect(parseTime(text)).toEqual(expected);
  });

  it.each(['', 'soon', '25:00', '9:75', '13pm', '12345'])('rejects %s', (text) => {
    expect(parseTime(text)).toBeNull();
  });
});

describe('stepTime', () => {
  it('snaps onto the quarter hour before stepping', () => {
    expect(stepTime({ hour: 15, minute: 40 }, 1)).toEqual({ hour: 15, minute: 45 });
    expect(stepTime({ hour: 15, minute: 40 }, -1)).toEqual({ hour: 15, minute: 30 });
  });

  it('wraps round midnight', () => {
    expect(stepTime({ hour: 23, minute: 45 }, 1)).toEqual({ hour: 0, minute: 0 });
    expect(stepTime({ hour: 0, minute: 0 }, -1)).toEqual({ hour: 23, minute: 45 });
  });
});
