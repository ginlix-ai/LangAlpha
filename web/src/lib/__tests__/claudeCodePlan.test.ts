/**
 * The words and the rules every Claude plan surface shares: when a window
 * resets, told in the reader's zone and language; what a turn the plan's
 * limit ended says; and which models a limit holds, for the composer's row
 * and the picker, by the user's choice of Stop or usage credits.
 */
import { describe, it, expect } from 'vitest';
import i18n from '@/i18n';
import type { ClaudeCodePlan, ClaudeCodePlanHeld, ClaudeCodePlanWindow } from '@/api/model';
import {
  composerPlanLine, creditsRefused, limitDetail, pickerPlanHold, planActionText, planBilledText, planRefusalText, planTime, readPlanBilled, readPlanRefusal,
  savedTokenInSentence, type PlanClock,
} from '../claudeCodePlan';

const en = i18n.getFixedT('en-US');
const zh = i18n.getFixedT('zh-CN');

/** Fri Nov 6, 2:05 PM in New York. */
const NOW = Date.parse('2026-11-06T19:05:00Z');
const NY: PlanClock = { locale: 'en-US', zone: 'America/New_York', now: NOW };
const NY_ZH: PlanClock = { ...NY, locale: 'zh-CN' };
/** 3:40 PM the same day in New York. */
const TODAY = '2026-11-06T20:40:00Z';
/** Tue 9:00 AM in New York, four days out. */
const TUESDAY = '2026-11-10T14:00:00Z';
/** Thu 9:00 AM in New York, six days out: yesterday's weekday. */
const THURSDAY = '2026-11-12T14:00:00Z';
/** Fri 9:00 AM in New York, a week out: today's weekday. */
const NEXT_FRIDAY = '2026-11-13T14:00:00Z';

const FAMILIES: Record<ClaudeCodePlanWindow, ClaudeCodePlanHeld['family']> = {
  five_hour: null, seven_day: null, seven_day_opus: 'opus', seven_day_sonnet: 'sonnet', seven_day_overage_included: 'fable',
};
const limit = (window: ClaudeCodePlanWindow, until: string): ClaudeCodePlanHeld => ({ window, until, family: FAMILIES[window], reset_known: true });
const planWith = (limits: ClaudeCodePlanHeld[], credits: ClaudeCodePlan['credits'] = null): ClaudeCodePlan => ({
  five_hour: null, seven_day: null, limits, credits, observed_at: '2026-11-06T19:00:00Z',
});
const planAt = (window: ClaudeCodePlanWindow, until: string, credits: ClaudeCodePlan['credits'] = null) => planWith([limit(window, until)], credits);

describe('the saved token named in a sentence', () => {
  it('starts lowercase in en and keeps zh-CN as its label', () => {
    const name = 'modelAccess.claudeCode.setup.savedName';
    expect(en(name)).toBe('Your setup token');
    expect(savedTokenInSentence(en(name), 'en-US')).toBe('your setup token');
    expect(savedTokenInSentence(zh(name), 'zh-CN')).toBe(zh(name));
  });
});

describe('planTime', () => {
  it('tells a reset today by its time alone, and another day this week by its weekday too', () => {
    expect(planTime(TODAY, NY)).toBe('3:40 PM');
    expect(planTime(TODAY, NY_ZH)).toBe('15:40');
    expect(planTime(TUESDAY, NY)).toBe('Tue 9:00 AM');
    expect(planTime(TUESDAY, NY_ZH)).toBe('周二 9:00');
  });

  it('dates a reset six days out or more, whose weekday alone reads as one just past', () => {
    expect(planTime(THURSDAY, NY)).toBe('Thu, Nov 12, 9:00 AM');
    expect(planTime(THURSDAY, NY_ZH)).toBe('11月12日周四 9:00');
    expect(planTime(NEXT_FRIDAY, NY)).toBe('Fri, Nov 13, 9:00 AM');
    expect(planTime(NEXT_FRIDAY, NY_ZH)).toBe('11月13日周五 9:00');
  });

  it('dates a reset whose day is past, as a replayed reply tells it', () => {
    const replayed = { ...NY, now: Date.parse('2026-11-30T19:05:00Z') };
    expect(planTime(THURSDAY, replayed)).toBe('Thu, Nov 12, 9:00 AM');
    expect(planTime(THURSDAY, { ...replayed, locale: 'zh-CN' })).toBe('11月12日周四 9:00');
  });

  it("tells today by the reader's zone, not this device's", () => {
    const lateFriday = '2026-11-07T03:30:00Z';
    expect(planTime(lateFriday, NY)).toBe('10:30 PM');
    expect(planTime(lateFriday, { ...NY, zone: 'Europe/London' })).toBe('Sat 3:30 AM');
    expect(planTime(THURSDAY, { ...NY, zone: 'Asia/Tokyo', now: Date.parse('2026-11-06T16:00:00Z') })).toBe('Thu 11:00 PM');
  });
});

describe('a turn a plan limit ended', () => {
  const frame = (fields: Record<string, unknown>) => ({ type: 'claude_code_off', message: 'Server words.', ...fields });

  it('says a stopped turn stopped, and adds what was billed to usage credits', () => {
    const stopped = readPlanRefusal(frame({ reason: 'plan_stopped', window: 'five_hour', until: TODAY, credits_billed: false }))!;
    expect(planRefusalText(en, stopped, NY)).toBe(
      "Your Claude plan's 5-hour limit is used up, so this turn stopped. It resets at 3:40 PM. Nothing ran on another model.",
    );
    const billed = readPlanRefusal(frame({ reason: 'plan_stopped', window: 'five_hour', until: TODAY, credits_billed: true }))!;
    expect(planRefusalText(en, billed, NY)).toBe(
      "Your Claude plan's 5-hour limit is used up, so this turn stopped. It resets at 3:40 PM. Nothing ran on another model. "
      + 'Requests already under way were billed to your usage credits.',
    );
    expect(planRefusalText(zh, billed, NY_ZH)).toBe(
      'Claude 套餐的 5 小时限额已用完，本轮已停止，将于 15:40 重置。没有改用其他模型运行。已在进行中的请求计入了你的额外用量。',
    );
  });

  it('says a refused turn waits for the reset', () => {
    const refused = readPlanRefusal(frame({ reason: 'plan_limited', window: 'seven_day', until: TUESDAY }))!;
    expect(planRefusalText(en, refused, NY)).toBe(
      "Your Claude plan's weekly limit is used up until Tue 9:00 AM. Pick another model, or send this again after it resets.",
    );
    expect(planRefusalText(zh, refused, NY_ZH)).toBe('Claude 套餐的每周限额已用完，周二 9:00 重置前无法使用。请换一个模型，或在重置后再发送。');
  });

  it('names the weekly Fable limit', () => {
    const refused = readPlanRefusal(frame({ reason: 'plan_limited', window: 'seven_day_overage_included', until: TUESDAY }))!;
    expect(planRefusalText(en, refused, NY)).toBe(
      "Your Claude plan's weekly Fable limit is used up until Tue 9:00 AM. Pick another model, or send this again after it resets.",
    );
    expect(planRefusalText(zh, refused, NY_ZH)).toBe('Claude 套餐的每周 Fable 限额已用完，周二 9:00 重置前无法使用。请换一个模型，或在重置后再发送。');
  });

  it('says a turn stopped or refused where Claude said no reset, naming no time', () => {
    const stopped = readPlanRefusal(frame({ reason: 'plan_stopped', credits_billed: false }))!;
    expect(planRefusalText(en, stopped, NY)).toBe(
      "Your Claude plan's limit is used up, so this turn stopped. Nothing ran on another model.",
    );
    expect(planRefusalText(zh, stopped, NY_ZH)).toBe('Claude 套餐的限额已用完，本轮已停止。没有改用其他模型运行。');
    const billed = readPlanRefusal(frame({ reason: 'plan_stopped', credits_billed: true }))!;
    expect(planRefusalText(en, billed, NY)).toBe(
      "Your Claude plan's limit is used up, so this turn stopped. Nothing ran on another model. "
      + 'Requests already under way were billed to your usage credits.',
    );
    expect(planRefusalText(zh, billed, NY_ZH)).toBe(
      'Claude 套餐的限额已用完，本轮已停止。没有改用其他模型运行。已在进行中的请求计入了你的额外用量。',
    );
    const refused = readPlanRefusal(frame({ reason: 'plan_limited' }))!;
    expect(planRefusalText(en, refused, NY)).toBe(
      "Your Claude plan's limit is used up, and Claude didn't say when it resets. Pick another model, or send this again later.",
    );
    expect(planRefusalText(zh, refused, NY_ZH)).toBe('Claude 套餐的限额已用完，Claude 没有告知重置时间。请换一个模型，或稍后再发送。');
  });

  it('says a reply was billed to usage credits, with its reset where Claude said one', () => {
    expect(planBilledText(en, readPlanBilled({ reason: 'billed_credits', window: 'five_hour', until: TODAY })!, NY)).toBe(
      "Your Claude plan's 5-hour limit ran out during this reply, so its last request was billed to your usage credits. "
      + 'Claude Code turns wait until it resets at 3:40 PM.',
    );
    const unknown = readPlanBilled({ reason: 'billed_credits' })!;
    expect(planBilledText(en, unknown, NY)).toBe(
      "Your Claude plan's limit ran out during this reply, so its last request was billed to your usage credits. "
      + 'Claude Code turns wait until it resets.',
    );
    expect(planBilledText(zh, unknown, NY_ZH)).toBe(
      'Claude 套餐的限额在这次回复中用完，最后一次请求计入了你的额外用量。Claude Code 将等到限额重置后再运行。',
    );
    expect(readPlanBilled({ reason: 'billed_credits', window: 'five_hour', until: 'soon' })).toBeNull();
  });

  it.each([
    ['no window', { reason: 'plan_stopped', until: TODAY }],
    ['a window the app does not know', { reason: 'plan_limited', window: 'thirty_day', until: TODAY }],
    ['a window named after an object member', { reason: 'plan_limited', window: 'toString', until: TODAY }],
    ['no time it can read', { reason: 'plan_limited', window: 'five_hour', until: 'soon' }],
    ['another reason', { reason: 'token_refused', window: 'five_hour', until: TODAY }],
  ])("keeps the server's words for a frame with %s", (_, fields) => {
    expect(readPlanRefusal(frame(fields))).toBeNull();
  });
});

describe('which models a limit holds', () => {
  const OPUS = 'claude-opus-5-5-code';
  const SONNET = 'claude-sonnet-5-5-code';
  const HAIKU = 'claude-haiku-5-5-code';
  const FABLE = 'claude-fable-5-1-code';

  it('stops at the limit under Stop, and under usage credits the account refused', () => {
    const line = "Your Claude plan's 5-hour limit is used up until 3:40 PM. Pick another model to keep going.";
    expect(composerPlanLine(en, planAt('five_hour', TODAY, 'on'), 'stop', HAIKU, NY)).toBe(line);
    expect(composerPlanLine(en, planAt('five_hour', TODAY, 'off'), 'credits', HAIKU, NY)).toBe(line);
    expect(pickerPlanHold(en, planAt('five_hour', TODAY, 'on'), 'stop', HAIKU, NY)).toBe('Plan limit used up until 3:40 PM');
    expect(pickerPlanHold(en, planAt('five_hour', TODAY, 'off'), 'credits', HAIKU, NY)).toBe('Plan limit used up until 3:40 PM');
  });

  it('runs on usage credits the user chose and the account has not refused', () => {
    for (const credits of ['on', null] as const) {
      const plan = planAt('seven_day', TUESDAY, credits);
      expect(composerPlanLine(en, plan, 'credits', OPUS, NY)).toBe(
        "Your Claude plan's weekly limit is used up, so Claude Code runs on your usage credits until it resets at Tue 9:00 AM.",
      );
      expect(pickerPlanHold(en, plan, 'credits', OPUS, NY)).toBeNull();
    }
  });

  it('holds only the family a weekly Opus, Sonnet or Fable limit is for', () => {
    const opus = planAt('seven_day_opus', TUESDAY);
    expect(pickerPlanHold(en, opus, 'stop', OPUS, NY)).toBe('Plan limit used up until Tue 9:00 AM');
    expect(pickerPlanHold(en, opus, 'stop', SONNET, NY)).toBeNull();
    expect(composerPlanLine(en, opus, 'stop', HAIKU, NY)).toBeNull();
    const sonnet = planAt('seven_day_sonnet', TUESDAY);
    expect(composerPlanLine(zh, sonnet, 'stop', SONNET, NY_ZH)).toBe('Claude 套餐的每周 Sonnet 限额已用完，周二 9:00 重置前无法使用。如需继续，请换一个模型。');
    expect(composerPlanLine(en, sonnet, 'stop', OPUS, NY)).toBeNull();
    const fable = planAt('seven_day_overage_included', TUESDAY);
    expect(composerPlanLine(en, fable, 'stop', FABLE, NY)).toBe(
      "Your Claude plan's weekly Fable limit is used up until Tue 9:00 AM. Pick another model to keep going.",
    );
    expect(pickerPlanHold(en, fable, 'stop', OPUS, NY)).toBeNull();
  });

  it('binds every model for a limit with no family, and only the named family otherwise', () => {
    const only = (family: ClaudeCodePlanHeld['family']) => planWith([{ window: 'seven_day', until: TUESDAY, family, reset_known: true }]);
    expect(pickerPlanHold(en, only(null), 'stop', HAIKU, NY)).toBe('Plan limit used up until Tue 9:00 AM');
    expect(pickerPlanHold(en, only('sonnet'), 'stop', HAIKU, NY)).toBeNull();
    expect(pickerPlanHold(en, only('sonnet'), 'stop', SONNET, NY)).toBe('Plan limit used up until Tue 9:00 AM');
  });

  it('holds a model at the binding limit that lasts longest, among those for its family', () => {
    const plan = planWith([limit('seven_day_opus', THURSDAY), limit('five_hour', TODAY), limit('seven_day_sonnet', NEXT_FRIDAY)]);
    expect(pickerPlanHold(en, plan, 'stop', OPUS, NY)).toBe('Plan limit used up until Thu, Nov 12, 9:00 AM');
    expect(pickerPlanHold(en, plan, 'stop', HAIKU, NY)).toBe('Plan limit used up until 3:40 PM');
    expect(pickerPlanHold(en, plan, 'stop', SONNET, NY)).toBe('Plan limit used up until Fri, Nov 13, 9:00 AM');
    const afterTheFiveHour = { ...NY, now: Date.parse(TODAY) };
    expect(pickerPlanHold(en, planWith([limit('five_hour', TODAY), limit('seven_day', TUESDAY)]), 'stop', HAIKU, afterTheFiveHour))
      .toBe('Plan limit used up until Tue 9:00 AM');
  });

  it('names no time for a limit Claude said no reset for, and stops holding at its bound', () => {
    const unknown = (credits: ClaudeCodePlan['credits']) => planWith([{ window: null, until: TODAY, family: null, reset_known: false }], credits);
    expect(composerPlanLine(en, unknown(null), 'stop', OPUS, NY)).toBe("Your Claude plan's limit is used up. Pick another model to keep going.");
    expect(composerPlanLine(zh, unknown(null), 'stop', OPUS, NY_ZH)).toBe('Claude 套餐的限额已用完。如需继续，请换一个模型。');
    expect(composerPlanLine(en, unknown('on'), 'credits', OPUS, NY)).toBe(
      "Your Claude plan's limit is used up, so Claude Code runs on your usage credits until it resets.",
    );
    expect(composerPlanLine(zh, unknown('on'), 'credits', OPUS, NY_ZH)).toBe('Claude 套餐的限额已用完，重置前 Claude Code 将使用你的额外用量运行。');
    expect(pickerPlanHold(en, unknown(null), 'stop', HAIKU, NY)).toBe('Plan limit used up');
    expect(pickerPlanHold(zh, unknown(null), 'stop', HAIKU, NY_ZH)).toBe('套餐限额已用完');
    expect(pickerPlanHold(en, unknown('on'), 'credits', HAIKU, NY)).toBeNull();
    const opus = planWith([{ window: 'seven_day_opus', until: TODAY, family: 'opus', reset_known: false }]);
    expect(composerPlanLine(en, opus, 'stop', OPUS, NY)).toBe("Your Claude plan's limit is used up. Pick another model to keep going.");
    expect(pickerPlanHold(en, opus, 'stop', OPUS, NY)).toBe('Plan limit used up');
    expect(pickerPlanHold(en, unknown(null), 'stop', HAIKU, { ...NY, now: Date.parse(TODAY) })).toBeNull();
  });

  it("names a token row's window and reset, and only that the limit is used up where Claude said no reset", () => {
    expect(limitDetail(en, limit('five_hour', TODAY), NY)).toBe('5h limit used up, resets 3:40 PM');
    expect(limitDetail(en, { window: null, until: TODAY, family: null, reset_known: false }, NY)).toBe('Plan limit used up');
    expect(limitDetail(en, { window: null, until: TODAY, family: null, reset_known: true }, NY)).toBe('Plan limit used up until 3:40 PM');
    expect(limitDetail(zh, { window: 'seven_day_opus', until: TODAY, family: 'opus', reset_known: false }, NY_ZH)).toBe('套餐限额已用完');
  });

  it('offers a check on a model held because the account has the usage credits the user chose off', () => {
    expect(creditsRefused(planAt('five_hour', TODAY, 'off'), 'credits', HAIKU, NOW)).toBe(true);
    expect(creditsRefused(planAt('five_hour', TODAY, 'off'), 'stop', HAIKU, NOW)).toBe(false);
    expect(creditsRefused(planAt('five_hour', TODAY, null), 'credits', HAIKU, NOW)).toBe(false);
    expect(creditsRefused(planAt('seven_day_opus', TODAY, 'off'), 'credits', SONNET, NOW)).toBe(false);
    expect(creditsRefused(planAt('five_hour', TODAY, 'off'), 'credits', HAIKU, Date.parse(TODAY))).toBe(false);
  });

  it('holds nothing without a limit, or once its reset has passed', () => {
    expect(composerPlanLine(en, planWith([]), 'stop', HAIKU, NY)).toBeNull();
    expect(composerPlanLine(en, null, 'stop', HAIKU, NY)).toBeNull();
    expect(pickerPlanHold(en, planAt('five_hour', TODAY), 'stop', HAIKU, { ...NY, now: Date.parse(TODAY) })).toBeNull();
  });
});

describe('a compaction the Claude plan refused', () => {
  it('reads as a refused send, in the reader\'s zone and language, whatever the server called it', () => {
    const detail = { message: 'Server words in UTC.', reason: 'plan_stopped', window: 'seven_day', until: TUESDAY, credits_billed: true };
    expect(planActionText(en, detail, NY)).toBe("Your Claude plan's weekly limit is used up until Tue 9:00 AM. Pick another model, or send this again after it resets.");
    expect(planActionText(zh, detail, NY_ZH)).toBe('Claude 套餐的每周限额已用完，周二 9:00 重置前无法使用。请换一个模型，或在重置后再发送。');
    expect(planActionText(en, { reason: 'plan_limited' }, NY)).toBe(
      "Your Claude plan's limit is used up, and Claude didn't say when it resets. Pick another model, or send this again later.",
    );
  });

  it('leaves any other refusal to the server\'s words', () => {
    expect(planActionText(en, { message: 'Compaction is already running.', reason: 'busy' }, NY)).toBeNull();
  });
});
