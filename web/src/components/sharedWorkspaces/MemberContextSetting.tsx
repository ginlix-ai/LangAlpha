import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { SwitchRow } from './SwitchRow';

/**
 * Whether a member's own answer preferences and personal memory reach the
 * agent on their messages here, in the People dialog. A manager turns it off
 * for the workspace; a member reads the state. The host leaves it out while
 * the organization's rule does not allow it.
 */
export function MemberContextSetting({ on, canManage, disabled, onChange }: {
  on: boolean;
  canManage: boolean;
  disabled?: boolean;
  onChange: (on: boolean) => void;
}) {
  const { t } = useTranslation();
  const id = useId();

  return (
    <SwitchRow
      divided
      id={id}
      label={t('sharedWorkspace.memberContext.label')}
      description={t('sharedWorkspace.memberContext.body')}
      checked={on}
      disabled={!canManage || disabled}
      onChange={() => onChange(!on)}
    />
  );
}
