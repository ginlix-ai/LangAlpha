import { useTranslation } from 'react-i18next';
import { Building2, Gauge } from 'lucide-react';
import type { RuleStop } from '@/lib/turnRefusal';
import { orgWords } from '@/lib/accountRules';
import { RuleRefusalRow } from './RuleRefusalRow';
import { RuleRow } from './RuleRow';

/** A turn the account's standing kept from running or ended, wherever the
 *  app shows it (the composer's banner, the turn's own card). `message` is
 *  the error's, which a limit is worded by. */
export function RuleStopRow({ stop, message, className }: { stop: RuleStop; message: string; className?: string }) {
  const { t } = useTranslation();
  switch (stop.kind) {
    case 'rule':
      return <RuleRefusalRow refusal={stop.refusal} className={className} />;
    case 'suspended':
      return <RuleRow className={className} title={orgWords(t, stop.organization, 'organization.suspended.turnStopped')} />;
    case 'limit':
      return <RuleRow icon={Gauge} title={message} className={className} />;
    case 'rules_unknown':
      return (
        <RuleRow
          icon={Building2}
          title={t('organization.unknown.notSent')}
          body={t('organization.unknown.notSentBody')}
          className={className}
        />
      );
  }
}
