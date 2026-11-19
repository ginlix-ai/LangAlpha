import { useTranslation } from 'react-i18next';
import { RuleNote } from '@/components/organization/RuleRow';
import { useAccount } from '@/hooks/useAccount';
import { allows, RULE, setterOf } from '@/lib/accountRules';

/**
 * Who keeps the user's own API keys or subscriptions off, said where they
 * would be added. Under the ways to connect (`setup`) it is one sentence of
 * which ways are off. In Settings it takes the place of the Add buttons the
 * rule removed and also says what they saved stays, unused, and with both
 * off that turns run only on what the organization or deployment provides.
 */
export function OwnAccessNote({ variant }: { variant: 'settings' | 'setup' }) {
  const { t } = useTranslation();
  const { policy, setterName } = useAccount();
  const keysOff = !allows(policy, RULE.ownApiKeys);
  const subscriptionsOff = !allows(policy, RULE.ownSubscriptions);
  if (!keysOff && !subscriptionsOff) return null;
  const keysWho = setterName(RULE.ownApiKeys);
  const keysLine = t('organization.modelAccess.keysOff', { who: keysWho.start });
  const subscriptionsLine = t('organization.modelAccess.subscriptionsOff', { who: setterName(RULE.ownSubscriptions).start });
  if (variant === 'setup') {
    return (
      <RuleNote>
        {keysOff && subscriptionsOff
          ? t('organization.setup.nothingToConnect', { who: keysWho.start })
          : keysOff ? keysLine : subscriptionsLine}
      </RuleNote>
    );
  }
  // One sentence for both when one layer set both; otherwise one each.
  const together = keysOff && subscriptionsOff
    && setterOf(policy, RULE.ownApiKeys) === setterOf(policy, RULE.ownSubscriptions);
  const sentences: string[] = [];
  if (together) {
    sentences.push(t('organization.modelAccess.bothOff', { who: keysWho.start }));
  } else {
    if (keysOff) sentences.push(keysLine);
    if (subscriptionsOff) sentences.push(subscriptionsLine);
  }
  if (keysOff && subscriptionsOff) sentences.push(t('organization.modelAccess.provided', { who: keysWho.mid }));
  sentences.push(t('organization.modelAccess.savedStays'));
  return <RuleNote>{sentences.reduce((first, next) => t('organization.twoSentences', { first, next }))}</RuleNote>;
}
