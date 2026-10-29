/**
 * What the user is shown as having answered each turn of the welcome
 * conversation: the picks on its card, put back as the line in their bubble.
 * Built from the same labels the cards print, so an answer reads as what was
 * tapped.
 */
import type { TFunction } from 'i18next';
import { formatList } from '@/lib/format';
import type { ElsewhereAnswers } from './draft';
import type { WelcomeStepId } from './paths';
import { PROFILE_FIELDS, answerLabels, stepFields, type BrokerageKey, type InvestingProfile } from './profile';

/** Every part an answer has, in the order its card asks them. */
const join = (parts: Array<string | null | undefined | false>) => parts.filter(Boolean).join(' · ');

const list = (labels: string[], locale: string) => (labels.length > 0 ? formatList(labels, locale) : null);

/** A question's turn: each answer its card asks, a list of picks read as one
 *  phrase, and nothing for one not answered yet. */
export function questionSummary(t: TFunction, locale: string, step: WelcomeStepId, p: InvestingProfile): string {
  return join(stepFields(step).map((key) => list(answerLabels(t, key, p[key]), locale)));
}

/**
 * The brokerages picked on the elsewhere turn, by name: a listed one by its
 * label and "other" by the name typed for it. `used` keeps only the accounts
 * the agent is told of, so "No account yet" and an "other" left unnamed go;
 * otherwise each reads as the chip tapped, an unnamed "other" as "Other".
 */
export function brokerageNames(
  t: TFunction,
  keys: readonly BrokerageKey[],
  otherName: string,
  { used = false }: { used?: boolean } = {},
): string[] {
  const named = otherName.trim();
  return keys.flatMap((key) => {
    if (key === 'other' && named) return [named];
    if (used && (key === 'other' || key === 'none')) return [];
    return [PROFILE_FIELDS.brokerages.label(t, key)];
  });
}

/** The brokerage turn: what was connected, else that the user went on
 *  without, else that it was passed over. */
export function brokerageSummary(
  t: TFunction,
  locale: string,
  { connected, elsewhere }: { connected: string[]; elsewhere: boolean },
): string {
  if (connected.length > 0) {
    return t('onboarding.welcome.chat.summary.connected', { brokerage: formatList(connected, locale) });
  }
  return elsewhere ? t('onboarding.welcome.brokerage.elsewhereAction') : t('onboarding.welcome.chat.summary.skipped');
}

/** The turn for a user who connected none: where they invest, and how their
 *  holdings come in. */
export function elsewhereSummary(
  t: TFunction,
  locale: string,
  profile: InvestingProfile,
  { otherName, method, shots }: ElsewhereAnswers,
): string {
  const how =
    method === 'screenshot'
      ? shots.length > 0
        ? t('onboarding.welcome.chat.summary.screenshots', { count: shots.length })
        : null
      : method === 'search'
        ? t('onboarding.welcome.chat.summary.search')
        : null;
  return (
    join([list(brokerageNames(t, profile.brokerages, otherName), locale), how]) ||
    t('onboarding.welcome.chat.summary.skipped')
  );
}
