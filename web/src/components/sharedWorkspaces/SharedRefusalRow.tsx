import { useTranslation } from 'react-i18next';
import { Info } from 'lucide-react';
import { RuleRefusalRow } from '@/components/organization/RuleRefusalRow';
import { RuleRow } from '@/components/organization/RuleRow';
import type { SharedRefusal } from '@/lib/sharedWorkspaceRefusal';

/** A shared workspace's refusal as a calm row: a rule through the rule row
 *  every surface uses, anything else as a plain statement of what to do. */
export function SharedRefusalRow({ refusal, className }: { refusal: SharedRefusal; className?: string }) {
  const { t } = useTranslation();
  if (refusal.kind === 'rule') return <RuleRefusalRow refusal={refusal.refusal} className={className} />;
  return <RuleRow icon={Info} title={t(refusal.key, refusal.values)} className={className} />;
}
