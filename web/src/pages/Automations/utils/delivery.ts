import type { TFunction } from 'i18next';
import { isDmAddress, messagingAppName, platformOf } from '@/pages/ChatAgent/components/messaging/messageDelivery';
import { apiErrorDetail, apiErrorStatus } from '@/pages/ChatAgent/utils/api/errors';
import type { DeliveryAttempt, DeliveryDefault, DeliveryOptions } from '@/types/automation';
import { mutationErrorMessage } from './errors';

const METHOD_KEY: Record<string, string> = {
  slack: 'automation.deliverToSlack',
  discord: 'automation.deliverToDiscord',
};

/** A delivery channel's name as the reader sees it. A DM address reads
 *  "<App> DM". The agent can pick a channel the form does not offer, which
 *  keeps its app's name, or a chat address capitalized. */
export function deliveryMethodName(method: string, t: TFunction): string {
  const key = METHOD_KEY[method];
  if (key) return t(key);
  if (method.includes(':') && isDmAddress(method)) {
    const app = platformOf(method) as string;
    const appKey = METHOD_KEY[app];
    return t('automation.deliverToDm', { app: appKey ? t(appKey) : messagingAppName(app) });
  }
  if (!method.includes(':')) return messagingAppName(method) ?? method;
  return method.charAt(0).toUpperCase() + method.slice(1);
}

/** Whether an entry names one chat (`slack:T1/C2`) rather than an app (`slack`). */
export function namesChat(entry: string): boolean {
  return entry.includes(':');
}

/** Chat names by address, from the chats the apps offer now and the names
 *  past deliveries reported. */
export type DeliveryNames = ReadonlyMap<string, string>;

/**
 * Every chat name known for this automation. The apps' current names win;
 * past runs, newest first, name a chat the apps no longer list. A bare app
 * entry is never named by a run: where it lands moves with the defaults.
 */
export function deliveryNames(
  options: DeliveryOptions | null | undefined,
  attempts: readonly DeliveryAttempt[] = [],
): Map<string, string> {
  const names = new Map<string, string>();
  for (const app of Object.values(options?.apps ?? {})) {
    for (const chat of app?.chats ?? []) if (chat.name) names.set(chat.address, chat.name);
  }
  for (const attempt of attempts) {
    if (!attempt.name) continue;
    for (const key of [attempt.address, attempt.method]) {
      if (key && namesChat(key) && !names.has(key)) names.set(key, attempt.name);
    }
  }
  return names;
}

/** The picked entries after a list's own choices changed to `picked`: an
 *  entry the list cannot show stays where it was, and a new pick goes last. */
export function mergeDeliverySelection(methods: string[], known: ReadonlySet<string>, picked: string[]): string[] {
  const next = new Set(picked);
  const kept = methods.filter((m) => !known.has(m) || next.has(m));
  const added = [...next].filter((k) => !methods.includes(k));
  return [...kept, ...added];
}

/** Where an app's bare entry lands now, or null where nothing says. */
export function deliveryDefaultOf(options: DeliveryOptions | null | undefined, app: string): DeliveryDefault | null {
  return options?.apps?.[app]?.default ?? null;
}

/** An entry as text: a bare app with where it lands now ("Slack (#demo)"),
 *  a chat by its name, else as `deliveryMethodName` spells it. */
export function deliveryEntryName(
  entry: string,
  t: TFunction,
  options?: DeliveryOptions | null,
  names?: DeliveryNames,
): string {
  if (!namesChat(entry)) {
    const app = deliveryMethodName(entry, t);
    const lands = deliveryDefaultOf(options, entry)?.name;
    return lands ? t('automation.deliverAppLandsIn', { app, name: lands }) : app;
  }
  return names?.get(entry) ?? deliveryMethodName(entry, t);
}

/** The chat a run's delivery landed in, by the best name known. */
export function deliveryAttemptName(attempt: DeliveryAttempt, t: TFunction, names?: DeliveryNames): string {
  if (attempt.name) return attempt.name;
  for (const key of [attempt.address, attempt.method]) {
    const name = key && namesChat(key) ? names?.get(key) : undefined;
    if (name) return name;
  }
  return deliveryMethodName(attempt.method, t);
}

export type DeliveryOutcome = 'sent' | 'fallback' | 'notice' | 'failed';

/** How a delivery went: the agent sent it (or, for an older run, it was
 *  posted), its final answer was posted for it, a notice went, or it failed. */
export function deliveryOutcome(attempt: DeliveryAttempt): DeliveryOutcome {
  if (!attempt.success) return 'failed';
  if (attempt.via === 'fallback') return 'fallback';
  if (attempt.via === 'notice') return 'notice';
  return 'sent';
}

const OUTCOME_KEY: Record<DeliveryOutcome, string> = {
  sent: 'automation.deliveredVia',
  fallback: 'automation.deliveredFallbackVia',
  notice: 'automation.deliveredNoticeVia',
  failed: 'automation.deliveryFailedVia',
};

/** One delivery as a phrase: "sent to #demo", "#demo delivery failed". */
export function deliveryAttemptLabel(attempt: DeliveryAttempt, t: TFunction, names?: DeliveryNames): string {
  return t(OUTCOME_KEY[deliveryOutcome(attempt)], { method: deliveryAttemptName(attempt, t, names) });
}

// ── Refusals ───────────────────────────────────────────────

/** A delivery entry a save was refused over, and why. */
export interface DeliveryProblem {
  entry: string;
  message: string;
}

/** The refusal's list of problems, inside `detail` or beside it. */
function problemList(err: unknown): unknown {
  const detail = apiErrorDetail(err);
  if (detail && typeof detail === 'object' && !Array.isArray(detail)) {
    const problems = (detail as { problems?: unknown }).problems;
    if (problems) return problems;
  }
  return (err as { response?: { data?: { problems?: unknown } } })?.response?.data?.problems;
}

/** The entries a refused automation save names, each with why; empty for a
 *  refusal that names none. */
export function deliveryProblems(err: unknown): DeliveryProblem[] {
  const raw = problemList(err);
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((p): DeliveryProblem[] => {
    const { entry, message } = (p ?? {}) as { entry?: unknown; message?: unknown };
    return typeof entry === 'string' && typeof message === 'string' && message ? [{ entry, message }] : [];
  });
}

/** Why a workspace default wasn't set, in a line. */
export function deliveryDefaultError(err: unknown, t: TFunction): string {
  const status = apiErrorStatus(err);
  if (status === 409) return t('automation.deliveryDefaultBusy');
  if (status === 503) return t('automation.deliveryDefaultUnavailable');
  const raw = problemList(err);
  const messages = Array.isArray(raw)
    ? raw.flatMap((p) => {
        const message = (p as { message?: unknown } | null)?.message;
        return typeof message === 'string' && message ? [message] : [];
      })
    : [];
  if (messages.length) return messages.join(' ');
  return mutationErrorMessage(err, t('automation.deliveryDefaultFailed'));
}
