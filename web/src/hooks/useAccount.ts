import { useTranslation } from 'react-i18next';
import { useUser } from './useUser';
import {
  allows, NO_STANDING, organizationLabel, orgWords, ruleSetterName, setterOf, standingOf, type AccountStanding, type Rule,
} from '@/lib/accountRules';
import { useAccountSuspendedNotice } from '@/lib/accountSuspension';

export interface Account extends AccountStanding {
  /** The organization's name, when the server gave one. */
  organizationName: string | null;
  /** The organization's name, or "your organization". */
  organizationLabel: string;
  /** `orgWords` for the account's organization. */
  orgT: (key: string, values?: Record<string, unknown>) => string;
  /** Who set `rule`, worded to start a sentence and to sit inside one. */
  setterName: (rule: string) => { start: string; mid: string };
}

/**
 * The account's organization and rules: the one place surfaces read them,
 * from the same `/users/me` the rest of the app shares. A suspension any
 * other answer revealed counts here too, before `/users/me` is read again.
 */
export function useAccount(): Account {
  const { t } = useTranslation();
  const { user } = useUser();
  const notice = useAccountSuspendedNotice();
  const standing = user ? standingOf(user) : NO_STANDING;
  const suspended = standing.suspended || notice !== null;
  const organizationName = standing.organization?.name ?? notice?.organization ?? null;
  return {
    ...standing,
    suspended,
    unknown: standing.unknown && !suspended,
    organizationName,
    organizationLabel: organizationLabel(t, organizationName),
    orgT: (key, values) => orgWords(t, organizationName, key, values),
    setterName: (rule) => ruleSetterName(t, setterOf(standing.policy, rule), organizationName),
  };
}

/** A rule a surface is gated on: whether the account's rules allow it, and
 *  who set it, worded to start a sentence and to sit inside one. */
export function useRule(rule: Exclude<Rule, 'models.allowed'>): { allowed: boolean; who: { start: string; mid: string } } {
  const { policy, setterName } = useAccount();
  return { allowed: allows(policy, rule), who: setterName(rule) };
}
