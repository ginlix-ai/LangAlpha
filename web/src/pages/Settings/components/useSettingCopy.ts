import { useTranslation } from 'react-i18next';
import type { SettingEntry } from '../sections';

/** A setting's anchor, name and line, as a row takes them: `<ToggleRow
 *  {...copy(SETTINGS.voiceInput)} …/>`. A row with a line of its own (an
 *  error, a plan hint) passes `description` after the spread. */
export function useSettingCopy() {
  const { t } = useTranslation();
  return (setting: SettingEntry) => ({
    id: setting.anchor,
    label: t(setting.labelKey),
    description: setting.descKey ? t(setting.descKey) : undefined,
  });
}
