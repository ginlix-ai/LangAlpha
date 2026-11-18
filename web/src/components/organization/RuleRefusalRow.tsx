import { useTranslation } from 'react-i18next';
import { useAccount } from '@/hooks/useAccount';
import { useModelName } from '@/hooks/useModelName';
import { ruleRefusalCopy, type RuleRefusal } from '@/lib/ruleRefusal';
import { requestModelPicker } from '@/lib/modelPickerRequest';
import { RuleRow } from './RuleRow';

/** A refusal by rule, wherever the app shows one: a turn the rule kept from
 *  running, a send it refused, a connection it would not add. */
export function RuleRefusalRow({ refusal, className }: { refusal: RuleRefusal; className?: string }) {
  const { t } = useTranslation();
  const { organizationName } = useAccount();
  const name = useModelName();
  const copy = ruleRefusalCopy(t, refusal, organizationName, name);
  return (
    <RuleRow
      className={className}
      title={copy.title}
      body={copy.body}
      action={copy.action === 'choose_model'
        ? { label: t('organization.refusal.chooseModel'), onPress: requestModelPicker }
        : undefined}
    />
  );
}
