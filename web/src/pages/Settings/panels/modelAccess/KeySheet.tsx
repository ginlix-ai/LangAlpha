import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';
import { BackButton, KeyStepFrame } from '@/components/modelAccess/KeyProviderPicker';
import { RegionKeyForm } from '@/components/modelAccess/RegionKeyForm';
import type { KeyOption, ProviderGroup } from '@/components/modelAccess/providerOptions';

interface KeySheetProps {
  /** The region the form starts on, or the only place the key goes. */
  option: KeyOption;
  /** The provider it was picked from in the add-key flow: the form is then
   *  one step of that flow, at its fixed size, with the provider's regions. */
  group?: ProviderGroup;
  configured: ConfiguredProvider[];
  /** `fresh`: the key began a connection, so its models start picked. */
  onSaved: (provider: string, fresh: boolean) => void;
  /** Back to the saved key, where the form edits one. */
  onCancel?: () => void;
  /** Back to the step that picked the provider. */
  onBack?: () => void;
}

/** Adding or updating a key, then on to its models. */
export function KeySheet({ option, group, configured, onSaved, onCancel, onBack }: KeySheetProps) {
  const { t } = useTranslation();
  return (
    <RegionKeyForm start={option} group={group} configured={configured}>
      {({ form, fields }) => {
        const buttons = (
          <>
            {onBack && <BackButton onClick={onBack} disabled={form.saving} className="mr-auto" />}
            {onCancel && (
              <Button type="button" variant="ghost" onClick={onCancel} disabled={form.saving}>
                {t('common.cancel')}
              </Button>
            )}
            <Button
              type="button"
              disabled={!form.canSave || form.saving}
              aria-busy={form.saving}
              onClick={async () => {
                const { fresh } = form;
                const saved = await form.save();
                if (saved) onSaved(saved, fresh);
              }}
            >
              {form.saving ? t('modelAccess.apiKey.saving') : form.saved ? t('modelAccess.apiKey.update') : t('modelAccess.apiKey.save')}
            </Button>
          </>
        );
        return group ? (
          <KeyStepFrame footer={buttons}>
            <div className="-mx-1 flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto px-1">{fields}</div>
          </KeyStepFrame>
        ) : (
          <div className="flex flex-col gap-4">
            {fields}
            <div className="flex items-center justify-end gap-2">{buttons}</div>
          </div>
        );
      }}
    </RegionKeyForm>
  );
}
