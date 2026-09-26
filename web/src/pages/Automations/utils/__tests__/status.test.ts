import { describe, expect, it } from 'vitest';
import type {
  Automation,
  AutomationExecution,
  AutomationStatus,
  DisableReason,
  ExecutionStatus,
  FailureReason,
  TriggerType,
} from '@/types/automation';
import {
  attentionKind,
  automationActions,
  automationCensus,
  automationGroup,
  automationStatusUi,
  describeRun,
  isUpcoming,
  isWatching,
  needsAttention,
  rowTrailing,
  type AttentionKind,
  type AutomationGroup,
} from '../status';

const NEXT = '2026-10-01T13:00:00Z';

function run(status: ExecutionStatus, over: Partial<AutomationExecution> = {}): AutomationExecution {
  return {
    automation_execution_id: 'exec-1',
    automation_id: 'auto-1',
    status,
    conversation_thread_id: null,
    scheduled_at: '2026-09-25T13:00:00Z',
    started_at: '2026-09-25T13:00:01Z',
    completed_at: '2026-09-25T13:02:00Z',
    error_message: null,
    skip_reason: null,
    failure_reason: null,
    delivery_result: null,
    created_at: '2026-09-25T13:00:00Z',
    excerpt: null,
    ...over,
  };
}

function automation(trigger: TriggerType, status: AutomationStatus, last: ExecutionStatus | null): Automation {
  return {
    automation_id: 'auto-1',
    user_id: 'user-1',
    name: 'Morning briefing',
    description: null,
    trigger_type: trigger,
    cron_expression: trigger === 'cron' ? '0 9 * * 1-5' : null,
    timezone: 'America/New_York',
    trigger_config: null,
    next_run_at: trigger === 'price' || status === 'completed' || status === 'disabled' ? null : NEXT,
    last_run_at: null,
    agent_mode: 'flash',
    instruction: 'Summarize the market.',
    workspace_id: null,
    llm_model: null,
    thread_strategy: 'new',
    conversation_thread_id: null,
    status,
    max_failures: 3,
    failure_count: 0,
    delivery_config: null,
    created_at: '2026-09-01T00:00:00Z',
    updated_at: '2026-09-01T00:00:00Z',
    last_execution: last ? run(last) : null,
  };
}

describe('automation state', () => {
  // trigger, status, last run -> group, state label, attention, upcoming, watching
  const cases: Array<[TriggerType, AutomationStatus, ExecutionStatus | null, AutomationGroup, string, boolean, boolean, boolean]> = [
    ['cron', 'active', 'completed', 'scheduled', 'stateScheduled', false, true, false],
    ['cron', 'active', 'running', 'scheduled', 'stateRunning', false, true, false],
    // Paused while its run was in flight: the run still shows until it ends.
    ['cron', 'paused', 'running', 'scheduled', 'stateRunning', false, false, false],
    ['cron', 'active', 'waiting', 'scheduled', 'stateWaiting', false, true, false],
    ['cron', 'active', 'failed', 'attention', 'stateLastRunFailed', true, true, false],
    ['cron', 'active', 'skipped', 'scheduled', 'stateScheduled', false, true, false],
    // A pause is the person's answer to a failure, and keeps its group.
    ['cron', 'paused', 'failed', 'scheduled', 'statePaused', false, false, false],
    ['cron', 'disabled', 'failed', 'attention', 'stateDisabled', true, false, false],
    ['once', 'active', null, 'scheduled', 'stateScheduled', false, true, false],
    // Ended, but its report never came: a retry is still owed.
    ['once', 'completed', 'failed', 'attention', 'stateLastRunFailed', true, false, false],
    ['once', 'completed', 'completed', 'finished', 'stateFinished', false, false, false],
    ['price', 'active', null, 'watching', 'stateWatching', false, false, true],
    ['price', 'active', 'timeout', 'attention', 'stateLastRunFailed', true, false, true],
    ['price', 'executing', 'running', 'watching', 'stateRunning', false, false, true],
    // Running outranks a failure it may be about to fix.
    ['price', 'executing', 'failed', 'watching', 'stateRunning', false, false, true],
    // A price automation reads `executing` while its run waits; the run decides.
    ['price', 'executing', 'waiting', 'watching', 'stateWaiting', false, false, true],
    ['price', 'paused', null, 'watching', 'statePaused', false, false, false],
  ];

  it.each(cases)('%s %s, last %s', (trigger, status, last, group, label, attention, upcoming, watching) => {
    const a = automation(trigger, status, last);
    expect(automationGroup(a)).toBe(group);
    expect(automationStatusUi(a).labelKey).toBe(`automation.${label}`);
    expect(needsAttention(a)).toBe(attention);
    expect(isUpcoming(a)).toBe(upcoming);
    expect(isWatching(a)).toBe(watching);
  });

  it('is not upcoming without a next run', () => {
    expect(isUpcoming({ ...automation('cron', 'active', null), next_run_at: null })).toBe(false);
  });
});

describe('why an automation needs attention', () => {
  function failedWith(status: AutomationStatus, disable: DisableReason | null, reason: FailureReason | null): Automation {
    const a = automation('cron', status, 'failed');
    return {
      ...a,
      disable_reason: disable,
      last_execution: { ...a.last_execution!, failure_reason: reason },
    };
  }

  // status, why it was switched off, why its last run failed -> kind, state label
  const cases: Array<[AutomationStatus, DisableReason | null, FailureReason | null, AttentionKind | null, string]> = [
    ['disabled', 'provider_auth', 'provider_auth', 'key_rejected', 'stateKeyRejected'],
    ['disabled', 'max_failures', null, 'switched_off', 'stateDisabled'],
    ['disabled', null, null, 'switched_off', 'stateDisabled'],
    // A usage limit leaves the schedule on and waits for the reader.
    ['active', null, 'usage_limit', 'usage_limit', 'stateUsageLimit'],
    ['active', null, null, 'failed', 'stateLastRunFailed'],
    // Resumed after a rejected key: an ordinary failure until the next run.
    ['active', null, 'provider_auth', 'failed', 'stateLastRunFailed'],
    // A pause is the reader's decision already.
    ['paused', null, 'usage_limit', null, 'statePaused'],
    // A one-shot price alert a limit ended: the report is still owed.
    ['completed', null, 'usage_limit', 'usage_limit', 'stateUsageLimit'],
  ];

  it.each(cases)('%s, switched off for %s, failed with %s', (status, disable, reason, kind, label) => {
    const a = failedWith(status, disable, reason);
    expect(attentionKind(a)).toBe(kind);
    expect(needsAttention(a)).toBe(kind !== null);
    expect(automationStatusUi(a).labelKey).toBe(`automation.${label}`);
  });
});

describe('automationCensus', () => {
  it('counts each automation once, with a run in flight counted on top', () => {
    const census = automationCensus([
      automation('cron', 'active', 'completed'),
      automation('cron', 'active', 'running'),
      automation('cron', 'active', 'failed'),
      automation('price', 'active', null),
      automation('price', 'completed', 'failed'),
      automation('cron', 'paused', null),
      automation('once', 'completed', 'completed'),
    ]);
    expect(census).toEqual({ running: 1, attention: 2, scheduled: 2, watching: 1, paused: 1, finished: 1 });
  });
});

describe('describeRun', () => {
  it.each<[ExecutionStatus, Partial<AutomationExecution>, boolean]>([
    ['completed', {}, true],
    ['failed', {}, true],
    ['timeout', {}, true],
    ['completed', { started_at: null }, false],
    ['failed', { completed_at: null }, false],
    ['running', { completed_at: null }, false],
    ['pending', { started_at: null, completed_at: null }, false],
    ['waiting', { started_at: null, completed_at: null }, false],
    ['skipped', {}, false],
  ])('%s %o shows a duration: %s', (status, over, shown) => {
    expect(describeRun(run(status, over)).showDuration).toBe(shown);
  });

  // The feed's watch link and its error line read these off the glyph.
  it.each<[string, boolean, boolean, string]>([
    ['pending', true, false, 'runQueued'],
    ['running', true, false, 'runRunning'],
    ['waiting', false, false, 'runWaiting'],
    ['failed', false, true, 'runFailed'],
    ['timeout', false, true, 'runTimedOut'],
    ['completed', false, false, 'runCompleted'],
    // A newer server's status: neither live nor failed until this build knows it.
    ['cancelled', false, false, 'runUnknown'],
  ])('%s is live: %s, failed: %s', (status, live, danger, label) => {
    const { ui } = describeRun(run(status as ExecutionStatus));
    expect(!!ui.live).toBe(live);
    expect(!!ui.danger).toBe(danger);
    expect(ui.labelKey).toBe(`automation.${label}`);
  });

  it('notes why a run waits or was skipped, unless the reader skipped it', () => {
    expect(describeRun(run('waiting')).noteKey).toBe('automation.waitingNote');
    expect(describeRun(run('skipped', { skip_reason: 'thread_busy' })).noteKey).toBe('automation.skippedThreadBusy');
    expect(describeRun(run('skipped', { skip_reason: 'user' })).noteKey).toBeNull();
    expect(describeRun(run('completed')).noteKey).toBeNull();
  });
});

describe('automationActions', () => {
  // The server pauses only from active and resumes from paused or disabled;
  // a manual run is refused while disabled, so that is answered by a resume.
  it.each([
    ['active', null, { canPause: true, canResume: false, canRun: true, runBusy: false, remedy: 'retry' }],
    ['active', 'failed', { canPause: true, canResume: false, canRun: true, runBusy: false, remedy: 'retry' }],
    ['paused', null, { canPause: false, canResume: true, canRun: true, runBusy: false, remedy: 'retry' }],
    ['disabled', 'failed', { canPause: false, canResume: true, canRun: false, runBusy: false, remedy: 'resume' }],
    ['completed', 'completed', { canPause: false, canResume: false, canRun: true, runBusy: false, remedy: 'retry' }],
    ['active', 'running', { canPause: true, canResume: false, canRun: true, runBusy: true, remedy: null }],
    ['active', 'waiting', { canPause: true, canResume: false, canRun: true, runBusy: true, remedy: null }],
  ] as const)('%s with a %s run', (status, last, expected) => {
    expect(automationActions(automation('cron', status, last))).toEqual(expected);
  });
});

describe('rowTrailing', () => {
  it('names a state the glyph leaves unnamed, before any time', () => {
    expect(rowTrailing(automation('cron', 'active', 'waiting'), 'scheduled')).toEqual({ kind: 'state', labelKey: 'automation.stateWaiting' });
    expect(rowTrailing(automation('cron', 'paused', null), 'scheduled')).toEqual({ kind: 'state', labelKey: 'automation.statePaused' });
    expect(rowTrailing(automation('cron', 'paused', 'running'), 'scheduled')).toEqual({ kind: 'state', labelKey: 'automation.stateRunning' });
  });

  it('reads a schedule by its next run and a finished one by its last', () => {
    expect(rowTrailing(automation('cron', 'active', null), 'scheduled')).toEqual({ kind: 'next', at: NEXT });
    expect(rowTrailing(automation('once', 'completed', 'completed'), 'finished')).toEqual({
      kind: 'last',
      at: '2026-09-25T13:02:00Z',
    });
  });
});
