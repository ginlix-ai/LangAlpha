import type {
  ClaudeCodeComputer, ClaudeCodePlan, ClaudeCodePlanHeld, ClaudeCodePlanLimit, ClaudeCodePlanWindow, PlanBilled, PlanCredential, PlanRefusal,
} from '@/api/model';
import { createDateFormatter } from '@/lib/format';
import { utcOffsetMinutes } from '@/lib/timezones';

type Translate = (key: string, opts?: Record<string, unknown>) => string;

/** What Claude Code turns do once a Claude plan's limit is used up, as the
 *  user chose under Model access (`other_preference.claude_code_plan_out`):
 *  stop and wait for the reset (the default), or carry on on the account's
 *  usage credits. */
export type PlanOut = 'stop' | 'credits';

export const PLAN_OUT_KEY = 'claude_code_plan_out';

/** The user's choice read from `other_preference`: anything but usage
 *  credits reads as Stop. */
export const planOutOf = (other: unknown): PlanOut =>
  (other as Record<string, unknown> | null | undefined)?.[PLAN_OUT_KEY] === 'credits' ? 'credits' : 'stop';

/** The credential a computer's turns sign in with, whose plan its entry
 *  carries: the saved token where the server relays it, its own token, or
 *  none with neither. */
export function planCredentialOf(entry: Pick<ClaudeCodeComputer, 'source' | 'computer_id'>): PlanCredential | null {
  if (entry.source === 'saved') return { kind: 'saved' };
  return entry.source === 'machine' ? { kind: 'computer', computerId: entry.computer_id } : null;
}

/** One key per credential, so computers on the relayed saved token and the
 *  saved token's own row count as one. */
export const credentialKey = (credential: PlanCredential) => (credential.kind === 'saved' ? 'saved' : `computer:${credential.computerId}`);

/** The clock a plan's reset is told by: the reader's language, their zone,
 *  and now. */
export interface PlanClock {
  locale: string;
  zone: string;
  now: number;
}

const WINDOWS: ReadonlySet<unknown> = new Set<ClaudeCodePlanWindow>([
  'five_hour', 'seven_day', 'seven_day_opus', 'seven_day_sonnet', 'seven_day_overage_included',
]);

/** A plan window the app has words for, as a frame names it. */
export const isPlanWindow = (value: unknown): value is ClaudeCodePlanWindow => WINDOWS.has(value);
/** A reset time the app can read (ISO 8601). */
export const isPlanTime = (value: unknown): value is string => typeof value === 'string' && !Number.isNaN(Date.parse(value));

const windowName = (t: Translate, window: ClaudeCodePlanWindow) => t(`claudeCodePlan.window.${window}`);

const DAY_MS = 86_400_000;

/** The calendar day an instant falls on in a zone, counted from the epoch. */
const zonedDay = (at: number, zone: string) => Math.floor((at + utcOffsetMinutes(zone, new Date(at)) * 60_000) / DAY_MS);

const zoned = new Map<string, Record<'clock' | 'weekday' | 'dated' | 'day', (d: Date | number, locale: string) => string>>();

function formatsIn(zone: string) {
  let formats = zoned.get(zone);
  if (!formats) {
    formats = {
      clock: createDateFormatter({ hour: 'numeric', minute: '2-digit', timeZone: zone }),
      weekday: createDateFormatter({ weekday: 'short', timeZone: zone }),
      dated: createDateFormatter({ weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: zone }),
      day: createDateFormatter({ month: 'short', day: 'numeric', timeZone: zone }),
    };
    zoned.set(zone, formats);
  }
  return formats;
}

/**
 * The saved token's name as a sentence carries it. Its row label starts
 * uppercase ("Your setup token"); inside an en sentence it starts lowercase
 * ("...on your setup token."). Other locales keep the label as it is.
 */
export function savedTokenInSentence(name: string, locale: string): string {
  return locale.startsWith('en') ? name.charAt(0).toLocaleLowerCase(locale) + name.slice(1) : name;
}

/** When a window resets, in the reader's zone: the time alone today ("3:40
 *  PM", "15:40"), after its weekday later this week ("Thu 9:00 AM", "周四
 *  9:00"), and with its date too from six days out ("Thu, Nov 12, 9:00 AM",
 *  "11月12日周四 9:00"), where the weekday alone would read as one just past,
 *  and for a day already past, which a replayed reply tells. */
export function planTime(iso: string, { locale, zone, now }: PlanClock): string {
  const at = Date.parse(iso);
  const { clock, weekday, dated } = formatsIn(zone);
  const days = zonedDay(at, zone) - zonedDay(now, zone);
  if (days < 0 || days >= 6) return dated(at, locale);
  const time = clock(at, locale);
  return days === 0 ? time : `${weekday(at, locale)} ${time}`;
}

/** A reset as the usage table's Resets column fits it: the time today
 *  ("3:40 PM"), the weekday later this week ("Thu", "周四"), the date from six
 *  days out ("Nov 12"), where the weekday alone would read as one just past. */
export function resetsLabel(iso: string, { locale, zone, now }: PlanClock): string {
  const at = Date.parse(iso);
  const { clock, weekday, day } = formatsIn(zone);
  const days = zonedDay(at, zone) - zonedDay(now, zone);
  if (days === 0) return clock(at, locale);
  return days >= 6 ? day(at, locale) : weekday(at, locale);
}

/** The limit a plan holds a model at now: of the limits that bind its family,
 *  the one that lasts longest. A weekly Opus, Sonnet or Fable limit binds
 *  only that family's models, the 5-hour and weekly ones every model. A limit
 *  whose reset has passed holds none, since the plan read ages in the cache. */
function planLimit(plan: ClaudeCodePlan | null | undefined, model: string, now: number): ClaudeCodePlanHeld | null {
  let binding: ClaudeCodePlanHeld | null = null;
  for (const limit of plan?.limits ?? []) {
    const until = Date.parse(limit.until);
    if (!(until > now) || (limit.family && !model.includes(limit.family))) continue;
    if (!binding || until > Date.parse(binding.until)) binding = limit;
  }
  return binding;
}

/** Whether a model the plan holds still runs, on usage credits: the user
 *  chose them, and the account has not refused them. */
const onCredits = (plan: ClaudeCodePlan, out: PlanOut) => out === 'credits' && plan.credits !== 'off';

/** A limit's window and reset as a sentence names them, or null where
 *  Claude didn't say when it resets, which its sentences say instead. */
function resetValues(t: Translate, limit: ClaudeCodePlanLimit, clock: PlanClock): { window: string; time: string } | null {
  return limit.window && limit.until ? { window: windowName(t, limit.window), time: planTime(limit.until, clock) } : null;
}

/** A held limit as its sentences read it: its `until` only bounds the hold where Claude said no reset. */
const heldReset = (limit: ClaudeCodePlanHeld): ClaudeCodePlanLimit =>
  limit.reset_known ? limit : { window: null, until: null };

/** The windows the meter shows, each a share of the whole plan. */
export type MeterWindow = 'five_hour' | 'seven_day';

/** One window's share used (0 to 1) and when it resets. */
export interface WindowUse {
  window: MeterWindow;
  used: number;
  resetsAt: string;
}

/** At or past this share of a window, the composer says how much is used,
 *  and the meter draws the window in the warning colour. */
export const NEAR_LIMIT = 0.8;

/** A share as the meter prints it: whole percent. */
export const percentOf = (used: number) => Math.round(used * 100);

/** The plan's 5h then 7d windows, those a request reported whose reset is
 *  still ahead. */
export function planWindows(plan: ClaudeCodePlan | null | undefined, now: number): WindowUse[] {
  return (['five_hour', 'seven_day'] as const).flatMap((window) => {
    const use = plan?.[window];
    return use && Date.parse(use.resets_at) > now ? [{ window, used: use.used, resetsAt: use.resets_at }] : [];
  });
}

/** The window with the higher share, whose reset is when the plan frees up
 *  most: the 5h one on a tie. */
export function fullerWindow(plan: ClaudeCodePlan | null | undefined, now: number): WindowUse | null {
  return planWindows(plan, now).reduce<WindowUse | null>((top, w) => (!top || w.used > top.used ? w : top), null);
}

/** The limit a token's row names: of every limit still ahead, one that holds
 *  every model before a family's, then the one lasting longest. Null where
 *  none holds. */
export function rowLimit(plan: ClaudeCodePlan | null | undefined, now: number): ClaudeCodePlanHeld | null {
  const current = (plan?.limits ?? []).filter((l) => Date.parse(l.until) > now);
  const rank = (l: ClaudeCodePlanHeld) => [l.family === null ? 1 : 0, Date.parse(l.until)];
  return current.reduce<ClaudeCodePlanHeld | null>((top, l) => {
    if (!top) return l;
    const [a, b] = [rank(l), rank(top)];
    return a[0] > b[0] || (a[0] === b[0] && a[1] > b[1]) ? l : top;
  }, null);
}

/** A token row's detail at a limit: which window, and when it resets, as
 *  much of that as Claude said. */
export function limitDetail(t: Translate, limit: ClaudeCodePlanHeld, clock: PlanClock): string {
  if (!limit.reset_known) return t('claudeCodePlan.pickerUnknown');
  if (!limit.window) return t('claudeCodePlan.picker', { time: planTime(limit.until, clock) });
  return t('claudeCodePlan.limitDetail', { short: t(`claudeCodePlan.short.${limit.window}`), time: planTime(limit.until, clock) });
}

/** The composer's row on a Claude Code model: at the plan's limit, used up
 *  until the reset or running on usage credits until then; short of it, how
 *  much of the fuller window is used once that is `NEAR_LIMIT` or more. Null
 *  below it. */
export function composerPlanLine(
  t: Translate, plan: ClaudeCodePlan | null | undefined, out: PlanOut, model: string, clock: PlanClock,
): string | null {
  const limit = planLimit(plan, model, clock.now);
  if (plan && limit) {
    const key = onCredits(plan, out) ? 'claudeCodePlan.credits' : 'claudeCodePlan.limit';
    const values = resetValues(t, heldReset(limit), clock);
    return values ? t(key, values) : t(`${key}Unknown`);
  }
  const near = fullerWindow(plan, clock.now);
  if (!near || near.used < NEAR_LIMIT) return null;
  return t('claudeCodePlan.near', {
    percent: percentOf(near.used), window: windowName(t, near.window), time: planTime(near.resetsAt, clock),
  });
}

/** Whether the plan holds `model` though the user chose usage credits,
 *  because the account has them off: a check then costs nothing, and once
 *  they are on again it reads the model free. */
export const creditsRefused = (plan: ClaudeCodePlan | null | undefined, out: PlanOut, model: string, now: number) =>
  out === 'credits' && plan?.credits === 'off' && planLimit(plan, model, now) !== null;

/** Why the picker holds a Claude Code model: the plan's limit for it is used
 *  up and no usage credits carry it. Null for one that runs. */
export function pickerPlanHold(
  t: Translate, plan: ClaudeCodePlan | null | undefined, out: PlanOut, model: string, clock: PlanClock,
): string | null {
  const limit = planLimit(plan, model, clock.now);
  if (!plan || !limit || onCredits(plan, out)) return null;
  if (!limit.reset_known) return t('claudeCodePlan.pickerUnknown');
  return t('claudeCodePlan.picker', { time: planTime(limit.until, clock) });
}

/** A limit's window and reset from a frame: both null where it carries
 *  neither, as Claude said no reset, and null where the app can't word the
 *  ones it carries. */
function frameReset(window: unknown, until: unknown): ClaudeCodePlanLimit | null {
  if (window == null && until == null) return { window: null, until: null };
  return isPlanWindow(window) && isPlanTime(until) ? { window, until } : null;
}

/** A `claude_code_off` refusal's plan limit. Null for another reason, and for
 *  one with a window or a time the app can't word, which keeps the server's
 *  words. */
export function readPlanRefusal(frame: Record<string, unknown>): PlanRefusal | null {
  const { reason, window, until } = frame;
  if (reason !== 'plan_stopped' && reason !== 'plan_limited') return null;
  const reset = frameReset(window, until);
  return reset && { reason, ...reset, creditsBilled: reason === 'plan_stopped' && frame.credits_billed === true };
}

/** A refusal's sentence, under the card's own headline. */
export function planRefusalText(t: Translate, refusal: PlanRefusal, clock: PlanClock): string {
  const key = refusal.reason === 'plan_limited' ? 'claudeCodePlan.refused'
    : refusal.creditsBilled ? 'claudeCodePlan.stoppedBilled' : 'claudeCodePlan.stopped';
  const values = resetValues(t, refusal, clock);
  return values ? t(key, values) : t(`${key}Unknown`);
}

/** An action's refusal where the Claude plan refused it, worded as a refused
 *  send is, since a compaction the plan stopped waits for the reset as a
 *  send does: in the reader's zone and language, where the server's own
 *  words are English and UTC. Null for any other refusal. */
export function planActionText(t: Translate, detail: Record<string, unknown>, clock: PlanClock): string | null {
  const refusal = readPlanRefusal(detail);
  return refusal && planRefusalText(t, { ...refusal, reason: 'plan_limited', creditsBilled: false }, clock);
}

/** A `claude_code_plan` event saying a reply's last request was billed to
 *  usage credits, or null for any other. */
export function readPlanBilled(event: Record<string, unknown>): PlanBilled | null {
  const { reason, window, until } = event;
  return reason === 'billed_credits' ? frameReset(window, until) : null;
}

export function planBilledText(t: Translate, billed: PlanBilled, clock: PlanClock): string {
  const values = resetValues(t, billed, clock);
  return values ? t('claudeCodePlan.billed', values) : t('claudeCodePlan.billedUnknown');
}
