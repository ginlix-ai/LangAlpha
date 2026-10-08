import type { useTranslation } from 'react-i18next';
import { formatList } from '@/lib/format';

type Translate = ReturnType<typeof useTranslation>['t'];

/**
 * The turn that opens onboarding in Home: what the user is shown as having
 * said, plus the instruction riding the `onboarding` skill.
 *
 * The message is the user's and follows their language. The instruction is
 * the agent's and stays English, as every other skill instruction does: the
 * agent answers in the user's language whatever the instruction is written in.
 * Both are kept here, together, so the wording of the opening turn has one home.
 */
export function onboardingKickoff(
  t: Translate,
  locale: string,
  brokerages: readonly string[],
): { message: string; additionalContext: Record<string, unknown>[] } {
  const named = brokerages.length > 0;
  const english = formatList(brokerages, 'en-US');
  return {
    message: named
      ? t('onboarding.kickoff.connected', { brokerage: formatList(brokerages, locale) })
      : t('onboarding.kickoff.noBrokerage'),
    additionalContext: [
      {
        type: 'skills',
        name: 'onboarding',
        instruction: named
          ? `The user just connected ${english}. Import their holdings and watchlists from it first, then continue onboarding.`
          : 'The user has no brokerage connected. Ask which stocks they own or follow, then continue onboarding.',
      },
    ],
  };
}
