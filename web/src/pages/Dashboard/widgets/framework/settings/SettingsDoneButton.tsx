import { useTranslation } from 'react-i18next';

interface Props {
  onClick: () => void;
  /** Override the default "Done" copy when the dialog has a different commit verb. */
  label?: string;
}

/**
 * Shared "Done" button for widget settings dialogs. One change here updates
 * focus-visible / hover / disabled states across every settings panel.
 */
export function SettingsDoneButton({ onClick, label }: Props) {
  const { t } = useTranslation();
  return (
    <div className="flex justify-end pt-1">
      <button
        type="button"
        onClick={onClick}
        className="settings-done-btn px-3 py-1.5 rounded text-sm font-medium transition-opacity hover:opacity-90 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2"
        style={{
          backgroundColor: 'var(--color-accent-primary)',
          color: 'var(--color-text-on-accent)',
          outlineColor: 'var(--color-accent-primary)',
        }}
      >
        {label ?? t('dashboard.widgets.settings.doneButton')}
      </button>
    </div>
  );
}
