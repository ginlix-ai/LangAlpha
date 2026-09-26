import { describe, expect, it } from 'vitest';
import type { AutomationRun } from '@/types/automation';
import { groupRunsByDay, nearDayKeys } from '../feed';
import { localDayKey } from '../time';

function run(id: string, started: Date | null, created: Date): AutomationRun {
  return {
    automation_execution_id: id,
    automation_id: 'auto-1',
    automation_name: 'Morning briefing',
    agent_mode: 'flash',
    trigger_type: 'cron',
    workspace_id: null,
    status: started ? 'completed' : 'waiting',
    conversation_thread_id: null,
    scheduled_at: created.toISOString(),
    started_at: started?.toISOString() ?? null,
    completed_at: null,
    error_message: null,
    skip_reason: null,
    failure_reason: null,
    delivery_result: null,
    created_at: created.toISOString(),
    excerpt: null,
    dismissed_at: null,
  };
}

const at = (day: number, hour: number) => new Date(2026, 8, day, hour);

describe('groupRunsByDay', () => {
  it('gathers a day from anywhere in the list, newest first', () => {
    // Server order is by creation. The first run waited overnight, so it
    // started after the second, which was created later but ran at once.
    const waited = run('waited', at(25, 8), at(24, 22));
    const ranAtOnce = run('ranAtOnce', at(24, 23), at(24, 23));
    const earlier = run('earlier', at(24, 9), at(24, 9));
    const today = run('today', at(25, 10), at(25, 10));

    const days = groupRunsByDay([today, waited, ranAtOnce, earlier]);

    expect(days.map((d) => d.key)).toEqual([localDayKey(at(25, 0)), localDayKey(at(24, 0))]);
    expect(days[0].runs.map((r) => r.automation_execution_id)).toEqual(['today', 'waited']);
    expect(days[1].runs.map((r) => r.automation_execution_id)).toEqual(['ranAtOnce', 'earlier']);
  });

  it('places a run that has not started by when it was due', () => {
    const due = run('due', null, at(23, 9));
    expect(groupRunsByDay([due])[0].key).toBe(localDayKey(at(23, 9)));
  });
});

describe('nearDayKeys', () => {
  it('takes yesterday from the calendar', () => {
    // Just after midnight on the day after a spring-forward day, which is
    // 23 hours long where the clock moves: a day's worth of milliseconds back
    // lands two dates earlier there.
    const now = new Date(2026, 2, 9, 0, 30);
    expect(nearDayKeys(now)).toEqual({
      today: localDayKey(new Date(2026, 2, 9)),
      yesterday: localDayKey(new Date(2026, 2, 8)),
    });
  });

  it('crosses a month and a year', () => {
    expect(nearDayKeys(new Date(2027, 0, 1, 8)).yesterday).toBe(localDayKey(new Date(2026, 11, 31)));
  });
});
