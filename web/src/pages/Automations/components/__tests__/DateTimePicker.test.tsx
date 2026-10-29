import { afterEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import i18n from '@/i18n';
import DateTimePicker from '../DateTimePicker';

afterEach(() => vi.useRealTimers());

describe('DateTimePicker across a New York clock change', () => {
  it.each([
    // 06:20Z is 1:20 EST, the second pass of the repeated hour: 1:45's first
    // pass (05:45Z) is gone but its second is still ahead today.
    ['fall back, second pass still ahead', '2026-11-01T06:20:00Z', '1:45a', '2026-11-01T06:45:00.000Z'],
    // Both passes gone: the next 1:45 is tomorrow's.
    ['fall back, both passes gone', '2026-11-01T06:50:00Z', '1:45a', '2026-11-02T06:45:00.000Z'],
    // 1:50 EST; 2:30 is skipped, and lands as far past the jump: 3:30 EDT.
    ['spring forward, skipped time', '2027-03-14T06:50:00Z', '2:30a', '2027-03-14T07:30:00.000Z'],
  ])('%s', (_name, now, typed, expected) => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date(now));
    const onChange = vi.fn();
    render(<DateTimePicker value="" onChange={onChange} timeZone="America/New_York" />);
    fireEvent.click(screen.getByRole('button', { name: i18n.t('automation.pickMoment') }));
    const time = screen.getByRole('textbox', { name: i18n.t('automation.time') });
    fireEvent.change(time, { target: { value: typed } });
    fireEvent.keyDown(time, { key: 'Enter' });
    expect(onChange).toHaveBeenLastCalledWith(expected);
  });

  it('picking today keeps a repeated time whose second pass is still ahead', () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    // 1:05 EST, the second pass; the run is set for 1:45 EST the next day.
    vi.setSystemTime(new Date('2026-11-01T06:05:00Z'));
    const onChange = vi.fn();
    render(<DateTimePicker value="2026-11-02T06:45:00.000Z" onChange={onChange} timeZone="America/New_York" />);
    fireEvent.click(screen.getByRole('button', { name: /Nov 2/ }));
    fireEvent.click(screen.getByRole('button', { name: /November 1, 2026/ }));
    // 1:45's second pass today, not the next quarter hour (06:30Z).
    expect(onChange).toHaveBeenLastCalledWith('2026-11-01T06:45:00.000Z');
  });
});
