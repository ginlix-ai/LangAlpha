import { describe, it, expect } from 'vitest';
import { parseCron, buildCron } from '../CronScheduleBuilder';

describe('parseCron', () => {
  it('parses empty string as daily at 09:00', () => {
    const s = parseCron('');
    expect(s.frequency).toBe('daily');
    expect(s.hour).toBe(9);
    expect(s.minute).toBe(0);
  });

  it('parses */N * * * * as minutes', () => {
    const s = parseCron('*/15 * * * *');
    expect(s.frequency).toBe('minutes');
    expect(s.interval).toBe(15);
  });

  it('parses M * * * * as hourly', () => {
    const s = parseCron('30 * * * *');
    expect(s.frequency).toBe('hourly');
    expect(s.minute).toBe(30);
  });

  it('parses M H * * * as daily', () => {
    const s = parseCron('0 9 * * *');
    expect(s.frequency).toBe('daily');
    expect(s.hour).toBe(9);
    expect(s.minute).toBe(0);
  });

  it('parses M H * * 1-5 as weekdays', () => {
    const s = parseCron('30 13 * * 1-5');
    expect(s.frequency).toBe('weekdays');
    expect(s.hour).toBe(13);
    expect(s.minute).toBe(30);
  });

  it('parses M H * * D as weekly', () => {
    const s = parseCron('0 22 * * 5');
    expect(s.frequency).toBe('weekly');
    expect(s.hour).toBe(22);
    expect(s.minute).toBe(0);
    expect(s.dayOfWeek).toBe(5);
  });

  it('parses M H D * * as monthly', () => {
    const s = parseCron('0 10 15 * *');
    expect(s.frequency).toBe('monthly');
    expect(s.hour).toBe(10);
    expect(s.dayOfMonth).toBe(15);
  });

  it('falls back to custom for complex expressions', () => {
    const s = parseCron('0 9 1,15 * *');
    expect(s.frequency).toBe('custom');
    expect(s.raw).toBe('0 9 1,15 * *');
  });

  it('falls back to custom for invalid format', () => {
    const s = parseCron('not a cron');
    expect(s.frequency).toBe('custom');
  });
});

describe('buildCron', () => {
  it('builds minutes cron', () => {
    expect(buildCron({ frequency: 'minutes', interval: 15, minute: 0, hour: 9, dayOfWeek: 1, dayOfMonth: 1, raw: '' }))
      .toBe('*/15 * * * *');
  });

  it('builds hourly cron', () => {
    expect(buildCron({ frequency: 'hourly', interval: 30, minute: 30, hour: 9, dayOfWeek: 1, dayOfMonth: 1, raw: '' }))
      .toBe('30 * * * *');
  });

  it('builds daily cron', () => {
    expect(buildCron({ frequency: 'daily', interval: 30, minute: 0, hour: 9, dayOfWeek: 1, dayOfMonth: 1, raw: '' }))
      .toBe('0 9 * * *');
  });

  it('builds weekdays cron', () => {
    expect(buildCron({ frequency: 'weekdays', interval: 30, minute: 30, hour: 13, dayOfWeek: 1, dayOfMonth: 1, raw: '' }))
      .toBe('30 13 * * 1-5');
  });

  it('builds weekly cron', () => {
    expect(buildCron({ frequency: 'weekly', interval: 30, minute: 0, hour: 22, dayOfWeek: 5, dayOfMonth: 1, raw: '' }))
      .toBe('0 22 * * 5');
  });

  it('builds monthly cron', () => {
    expect(buildCron({ frequency: 'monthly', interval: 30, minute: 0, hour: 10, dayOfWeek: 1, dayOfMonth: 15, raw: '' }))
      .toBe('0 10 15 * *');
  });

  it('returns raw for custom', () => {
    expect(buildCron({ frequency: 'custom', interval: 30, minute: 0, hour: 9, dayOfWeek: 1, dayOfMonth: 1, raw: '0 9 1,15 * *' }))
      .toBe('0 9 1,15 * *');
  });
});

describe('parseCron → buildCron round-trip', () => {
  const expressions = [
    '*/30 * * * *',
    '15 * * * *',
    '0 9 * * *',
    '30 13 * * 1-5',
    '0 22 * * 5',
    '0 10 15 * *',
  ];

  expressions.forEach((expr) => {
    it(`round-trips "${expr}"`, () => {
      expect(buildCron(parseCron(expr))).toBe(expr);
    });
  });
});
