import { createElement } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import i18n from '@/i18n';
import CronScheduleBuilder from '../../components/CronScheduleBuilder';
import { buildCron, cronToHuman as cronToHumanIn, MINUTE_INTERVALS, scheduleDraft } from '../cron';

const cronToHuman = (expression: string) => cronToHumanIn(expression, i18n.getFixedT('en-US'), 'en-US');

describe('cronToHuman monthly', () => {
  it('writes the ordinal the day takes', () => {
    expect(cronToHuman('0 7 1 * *')).toBe('At 7:00 AM, 1st of month');
    expect(cronToHuman('0 7 11 * *')).toBe('At 7:00 AM, 11th of month');
    expect(cronToHuman('0 7 22 * *')).toBe('At 7:00 AM, 22nd of month');
    expect(cronToHuman('0 7 23 * *')).toBe('At 7:00 AM, 23rd of month');
    expect(cronToHuman('0 7 31 * *')).toBe('At 7:00 AM, 31st of month');
  });

  it('names the last day', () => {
    expect(cronToHuman('0 7 L * *')).toBe('At 7:00 AM, month end');
  });
});

describe('cronToHuman in Chinese', () => {
  const zh = (expression: string) => cronToHumanIn(expression, i18n.getFixedT('zh-CN'), 'zh-CN');

  it('reads in the reader\'s language and clock', () => {
    expect(zh('30 7 * * 1-5')).toBe('周一至周五 7:30');
    expect(zh('0 9 * * 1,3,5')).toBe('周一、周三、周五 9:00');
    expect(zh('0 18 * * *')).toBe('每天 18:00');
    expect(zh('0 7 22 * *')).toBe('每月 22 日 7:00');
  });
});

const ALL = [true, true, true, true, true, true, true];
const base = { interval: 30, minute: 0, hour: 9, days: ALL, dayOfMonth: 1, raw: '' };

describe('scheduleDraft', () => {
  it('parses empty string as every day at 09:00', () => {
    const s = scheduleDraft('');
    expect(s.kind).toBe('days');
    expect(s.days).toEqual(ALL);
    expect(s.hour).toBe(9);
    expect(s.minute).toBe(0);
  });

  it('parses */N * * * * as minutes', () => {
    const s = scheduleDraft('*/15 * * * *');
    expect(s.kind).toBe('minutes');
    expect(s.interval).toBe(15);
  });

  it('parses M * * * * as hourly', () => {
    const s = scheduleDraft('30 * * * *');
    expect(s.kind).toBe('hourly');
    expect(s.minute).toBe(30);
  });

  it('parses M H * * * as every day', () => {
    const s = scheduleDraft('0 9 * * *');
    expect(s.kind).toBe('days');
    expect(s.days).toEqual(ALL);
  });

  it('parses a range, a list and a single day, Monday first', () => {
    expect(scheduleDraft('30 13 * * 1-5').days).toEqual([true, true, true, true, true, false, false]);
    expect(scheduleDraft('0 9 * * 1,2').days).toEqual([true, true, false, false, false, false, false]);
    expect(scheduleDraft('0 22 * * 5').days).toEqual([false, false, false, false, true, false, false]);
    expect(scheduleDraft('0 8 * * 0').days).toEqual([false, false, false, false, false, false, true]);
    expect(scheduleDraft('0 8 * * 7').days).toEqual([false, false, false, false, false, false, true]);
  });

  it('parses M H D * * as monthly', () => {
    const s = scheduleDraft('0 10 15 * *');
    expect(s.kind).toBe('monthly');
    expect(s.hour).toBe(10);
    expect(s.dayOfMonth).toBe(15);
  });

  it('parses the last day and the 29th-31st as monthly', () => {
    expect(scheduleDraft('0 7 L * *').dayOfMonth).toBe('L');
    expect(scheduleDraft('0 7 31 * *').dayOfMonth).toBe(31);
    expect(scheduleDraft('0 7 32 * *').kind).toBe('custom');
  });

  it('falls back to custom for complex expressions', () => {
    const s = scheduleDraft('0 9 1,15 * *');
    expect(s.kind).toBe('custom');
    expect(s.raw).toBe('0 9 1,15 * *');
  });

  it('falls back to custom for invalid format', () => {
    expect(scheduleDraft('not a cron').kind).toBe('custom');
    expect(scheduleDraft('0 9 * * MON').kind).toBe('custom');
  });
});

describe('buildCron', () => {
  it('builds minutes and hourly cron', () => {
    expect(buildCron({ ...base, kind: 'minutes', interval: 15 })).toBe('*/15 * * * *');
    expect(buildCron({ ...base, kind: 'hourly', minute: 30 })).toBe('30 * * * *');
  });

  it('writes every day as *, Mon-Fri as a range and any other set as a list', () => {
    expect(buildCron({ ...base, kind: 'days' })).toBe('0 9 * * *');
    expect(buildCron({ ...base, kind: 'days', minute: 30, hour: 13, days: [true, true, true, true, true, false, false] }))
      .toBe('30 13 * * 1-5');
    expect(buildCron({ ...base, kind: 'days', days: [true, true, false, false, false, false, false] }))
      .toBe('0 9 * * 1,2');
    expect(buildCron({ ...base, kind: 'days', days: [false, false, false, false, false, true, true] }))
      .toBe('0 9 * * 6,0');
  });

  it('builds monthly cron', () => {
    expect(buildCron({ ...base, kind: 'monthly', hour: 10, dayOfMonth: 15 })).toBe('0 10 15 * *');
    expect(buildCron({ ...base, kind: 'monthly', hour: 7, dayOfMonth: 'L' })).toBe('0 7 L * *');
  });

  it('returns raw for custom', () => {
    expect(buildCron({ ...base, kind: 'custom', raw: '0 9 1,15 * *' })).toBe('0 9 1,15 * *');
  });
});

describe('scheduleDraft → buildCron round-trip', () => {
  const expressions = [
    '*/30 * * * *',
    '15 * * * *',
    '0 9 * * *',
    '30 13 * * 1-5',
    '0 22 * * 5',
    '0 9 * * 1,2',
    '0 9 * * 1,3,5',
    '0 8 * * 6,0',
    '0 10 15 * *',
    '0 7 31 * *',
    '0 7 L * *',
  ];

  expressions.forEach((expr) => {
    it(`round-trips "${expr}"`, () => {
      expect(buildCron(scheduleDraft(expr))).toBe(expr);
    });
  });
});

describe('a schedule the page cannot read as written', () => {
  // A step that does not divide its hour or day restarts at the top of it, so
  // "every 45 minutes" (:00, :45, :00) would be a lie; so would a day name
  // read as a guess.
  it.each([
    '*/90 * * * *',
    '*/45 * * * *',
    '*/7 * * * *',
    '0 */25 * * *',
    '0 */5 * * *',
    '0 9 * * sun',
  ])('keeps "%s" as written and opens it as custom', (expr) => {
    expect(cronToHuman(expr)).toBe(expr);
    expect(scheduleDraft(expr)).toMatchObject({ kind: 'custom', raw: expr });
  });
});

describe('cronToHuman days of the week', () => {
  it('reads a range as the days it covers', () => {
    expect(cronToHuman('0 9 * * 1-3')).toBe('At 9:00 AM, Mon, Tue, Wed');
  });

  it('reads the working week listed day by day as the working week', () => {
    expect(cronToHuman('0 9 * * 1,2,3,4,5')).toBe('At 9:00 AM, Mon–Fri');
  });

  it('lists the days Monday first, whatever order the field has them in', () => {
    expect(cronToHuman('0 9 * * 0,1')).toBe('At 9:00 AM, Mon, Sun');
  });
});

describe('the builder\'s interval', () => {
  it('offers only the intervals that divide the hour', async () => {
    const onChange = vi.fn();
    render(createElement(CronScheduleBuilder, { value: '*/30 * * * *', onChange, labelledBy: 'when' }));
    const user = userEvent.setup();
    await user.click(screen.getByRole('button', { name: /Minutes between runs/ }));
    const offered = screen.getAllByRole('option').map((o) => Number(o.textContent));
    expect(offered).toEqual([1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30]);
    await user.click(screen.getByRole('option', { name: '15' }));
    expect(onChange).toHaveBeenLastCalledWith('*/15 * * * *');
  });

  it('reads every interval it offers back as that interval', () => {
    for (const n of MINUTE_INTERVALS) expect(scheduleDraft(`*/${n} * * * *`)).toMatchObject({ kind: 'minutes', interval: n });
  });
});
