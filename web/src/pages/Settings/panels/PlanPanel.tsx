import { useTranslation } from 'react-i18next';
import { useUser } from '@/hooks/useUser';
import { ConsoleLink } from '../components/ConsoleLink';
import { SettingRow, SettingsGroup } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS } from '../sections';

/** Plan & usage: where each lives in the account console, one step away. */
export function PlanPanel() {
  const { t } = useTranslation();
  const copy = useSettingCopy();
  const { user } = useUser();
  const plan = user?.plan_display_name;

  return (
    <SettingsGroup>
      <SettingRow
        {...copy(SETTINGS.plan)}
        description={plan ? t('settings.planUsage.current', { plan }) : t(SETTINGS.plan.descKey)}
      >
        <ConsoleLink path="plans">{t('settings.planUsage.change')}</ConsoleLink>
      </SettingRow>
      <SettingRow {...copy(SETTINGS.usage)}>
        <ConsoleLink path="usage">{t('settings.planUsage.viewUsage')}</ConsoleLink>
      </SettingRow>
      <SettingRow {...copy(SETTINGS.billing)}>
        <ConsoleLink path="billing">{t('settings.planUsage.manageBilling')}</ConsoleLink>
      </SettingRow>
    </SettingsGroup>
  );
}
