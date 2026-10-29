import { useTranslation } from 'react-i18next';
import { AUTO_APPROVE, useAutoApprove, type AutoApproveKey } from '@/hooks/useAutoApprove';
import { SettingsGroup, ToggleRow } from '../components/SettingsLayout';
import { useSettingCopy } from '../components/useSettingCopy';
import { SETTINGS, type SettingEntry } from '../sections';
import { useSaveFailureToast } from './usePreferenceWrite';

const ROWS: { key: AutoApproveKey; setting: SettingEntry }[] = [
  { key: AUTO_APPROVE.handoffs, setting: SETTINGS.approveHandoffs },
  { key: AUTO_APPROVE.workspaceCreation, setting: SETTINGS.approveWorkspaces },
];

function AutoApproveRow({ settingKey, setting }: { settingKey: AutoApproveKey; setting: SettingEntry }) {
  const copy = useSettingCopy();
  const reportFailure = useSaveFailureToast();
  const { enabled, set } = useAutoApprove(settingKey);
  return (
    <ToggleRow
      {...copy(setting)}
      checked={enabled}
      onChange={() => { void reportFailure(set(!enabled)); }}
    />
  );
}

/** The agent actions the user approved in advance, each turned back to asking here. */
export function AutoApproveSettings() {
  const { t } = useTranslation();
  return (
    <SettingsGroup title={t('settings.approvals')} description={t('settings.approvalsDesc')}>
      {ROWS.map((row) => <AutoApproveRow key={row.key} settingKey={row.key} setting={row.setting} />)}
    </SettingsGroup>
  );
}
