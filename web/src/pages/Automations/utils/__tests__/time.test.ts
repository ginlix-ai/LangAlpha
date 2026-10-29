import { describe, it, expect } from 'vitest';
import i18n from '@/i18n';
import { formatDuration } from '../time';

const START = '2026-09-25T14:00:00.000Z';
const after = (ms: number) => new Date(Date.parse(START) + ms).toISOString();
const en = i18n.getFixedT('en-US');

describe('formatDuration', () => {
  it('reads a sub-second run as under a second, never in milliseconds', () => {
    expect(formatDuration(START, after(109), en)).toBe('<1s');
  });

  it('keeps whole seconds at every scale', () => {
    expect(formatDuration(START, after(47_000), en)).toBe('47s');
    expect(formatDuration(START, after(12 * 60_000 + 17_000), en)).toBe('12m 17s');
    expect(formatDuration(START, after(63 * 60_000), en)).toBe('1h 3m');
  });

  it('drops a zero tail, since a finished run never ticks', () => {
    expect(formatDuration(START, after(15 * 60_000), en)).toBe('15m');
    expect(formatDuration(START, after(120 * 60_000), en)).toBe('2h');
    expect(formatDuration(START, after(15 * 60_000 + 400), en)).toBe('15m');
  });

  it('shows a dash when either end is missing', () => {
    expect(formatDuration(START, null, en)).toBe('—');
    expect(formatDuration(undefined, START, en)).toBe('—');
  });

  describe('in Chinese', () => {
    const zh = i18n.getFixedT('zh-CN');

    it('words the units in the reader\'s language', () => {
      expect(formatDuration(START, after(109), zh)).toBe('不到 1 秒');
      expect(formatDuration(START, after(12 * 60_000 + 17_000), zh)).toBe('12 分 17 秒');
      expect(formatDuration(START, after(15 * 60_000), zh)).toBe('15 分钟');
    });
  });
});
