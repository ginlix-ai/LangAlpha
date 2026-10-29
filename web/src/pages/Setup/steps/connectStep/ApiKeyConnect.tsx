import { useId, type ReactNode } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { WizardStepHeader } from '@/components/ui/wizard';
import type { ApiKeyFormState } from '@/components/modelAccess/ApiKeyForm';
import { ProviderTitle } from '@/components/modelAccess/KeyProviderPicker';
import { RegionKeyForm } from '@/components/modelAccess/RegionKeyForm';
import { optionsOf, type KeyOption } from '@/components/modelAccess/providerOptions';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { useProviderCatalog, type KeyPlace } from '@/hooks/useProviderCatalog';
import { SetupFooter } from '../../SetupFooter';
import { useProviderConnected, type LocationState } from './shared';

/**
 * A provider of the user's own is not in the catalog. Its key is edited as a
 * bare entry under its own name, with no default base URL, so the form starts
 * from the one saved with the key.
 */
function ownProviderOption(provider: string, displayName: string): KeyOption {
  return optionsOf({
    provider,
    display_name: displayName,
    access_type: 'api_key',
    brand_key: provider,
    byok_eligible: true,
    base_url: null,
  })[0];
}

/** The wizard's key step: the shared API-key form between the wizard's own header and footer. */
export function ApiKeyConnect({ state }: { state: LocationState }) {
  const { t } = useTranslation();
  const { optionFor, placeOf, isLoading } = useProviderCatalog();
  const { providers } = useConfiguredProviders();
  const provider = state.provider ?? '';

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader size={20} className="text-(--color-text-tertiary)" label={t('common.loading')} />
      </div>
    );
  }

  const place = placeOf(provider);
  return (
    <RegionKeyForm
      start={optionFor(provider) ?? ownProviderOption(provider, state.displayName ?? provider)}
      group={place?.group}
      configured={providers}
    >
      {({ form, fields, option }) => <KeyStep state={state} form={form} fields={fields} option={option} place={place} />}
    </RegionKeyForm>
  );
}

function KeyStep({ state, form, fields, option, place }: {
  state: LocationState;
  form: ApiKeyFormState;
  fields: ReactNode;
  option: KeyOption;
  place?: KeyPlace;
}) {
  const navigate = useNavigate();
  const connected = useProviderConnected();
  const { t } = useTranslation();
  const headingId = useId();

  const method = state.method ?? 'api_key';
  const { entry } = option;
  const displayName = place?.group.name ?? entry.display_name;
  const modelsFrom = option.models_from;

  const handleSave = async () => {
    const { fresh } = form;
    const savedAs = await form.save();
    if (!savedAs) return;
    const next = { method, list: state.list, provider: savedAs, displayName, modelsFrom, justConnected: fresh };
    // A local server has no models until some are picked from its list, so it
    // goes on to the model step whatever page sent the user here; the return
    // then happens from the last step.
    if (form.local) navigate('/setup/models', { state: next });
    else connected(next);
  };

  const handleBack = () => navigate('/setup/provider', { state: { list: state.list } });

  return (
    <div>
      <WizardStepHeader
        titleId={headingId}
        title={
          <ProviderTitle
            name={displayName}
            markProvider={place?.group.markProvider ?? entry.provider}
            title={t('setup.connectTitle', { provider: displayName })}
            size="md"
          />
        }
        helper={
          form.local
            ? t('setup.localServerDesc')
            : entry.access_type === 'coding_plan'
              ? t('setup.codingPlanDesc')
              : t('setup.apiKeyInputDesc')
        }
      />
      <div className="flex flex-col gap-4">{fields}</div>
      <SetupFooter
        back={{ label: t('setup.back'), onClick: handleBack }}
        primary={{
          label: form.saving ? t('setup.saving') : t('setup.continue'),
          onClick: handleSave,
          disabled: !form.canSave,
          busy: form.saving,
        }}
      />
    </div>
  );
}
