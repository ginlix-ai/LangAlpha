import { useId } from 'react';
import { useNavigate } from 'react-router';
import { useTranslation } from 'react-i18next';
import { Plus } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { Input } from '@/components/ui/input';
import { WizardStepHeader } from '@/components/ui/wizard';
import { ApiKeyInput } from '@/components/model/ApiKeyInput';
import { CustomModelFields } from '@/components/modelAccess/CustomModelFields';
import { QuietNote, UnverifiedNote } from '@/components/modelAccess/parts';
import { API_FORMATS } from '@/components/modelAccess/apiFormat';
import { useCustomProviderForm } from '@/components/modelAccess/useCustomProviderForm';
import { ModelCheckRow, useProviderConnected, type LocationState } from './shared';
import { SetupFooter } from '../../SetupFooter';

/** The wizard's step for a provider of the user's own: the shared form between the wizard's header and footer. */
export function CustomProviderConnect({ state }: { state: LocationState }) {
  const navigate = useNavigate();
  const connected = useProviderConnected();
  const { t } = useTranslation();
  const headingId = useId();
  const nameId = useId();
  const formatLabelId = useId();
  const formatTipsId = useId();
  const baseUrlId = useId();
  const apiKeyId = useId();
  const knownLabelId = useId();
  const form = useCustomProviderForm();
  const { checking, entered, listing, recognized } = form;
  const method = state.method ?? 'api_key';

  const handleSave = async () => {
    const slug = await form.save();
    if (slug) connected({ method, list: state.list, provider: slug, displayName: form.name.trim(), modelsFrom: slug, justConnected: true });
  };

  return (
    <div>
      <WizardStepHeader
        titleId={headingId}
        title={t('setup.addCustomProvider')}
        helper={t('setup.addCustomProviderDesc')}
      />
      <div className="flex flex-col gap-4 sm:gap-6">
        {form.knownEndpoint === false && <UnverifiedNote>{t('setup.customUnverifiedBody')}</UnverifiedNote>}

        <div className="flex flex-col gap-1.5">
          <label htmlFor={nameId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.providerNameLabel')}
          </label>
          <Input
            id={nameId}
            value={form.name}
            onChange={(e) => form.setName(e.target.value)}
            placeholder={t('setup.providerNamePlaceholder')}
            autoComplete="off"
          />
        </div>

        <div className="flex flex-col gap-1.5">
          <label id={formatLabelId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.apiFormatLabel')}
          </label>
          <div role="group" aria-labelledby={formatLabelId} aria-describedby={formatTipsId} className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {API_FORMATS.map((fmt) => (
              <button
                key={fmt.value}
                type="button"
                aria-pressed={form.apiFormat === fmt.value}
                onClick={() => form.setApiFormat(fmt.value)}
                className="rounded-lg px-3 py-2.5 text-left text-xs font-medium transition-colors"
                style={{
                  border: form.apiFormat === fmt.value
                    ? '1px solid var(--color-text-primary)'
                    : '1px solid var(--color-border-default)',
                  background: form.apiFormat === fmt.value ? 'var(--color-bg-tag)' : undefined,
                  color: 'var(--color-text-primary)',
                  padding: '10px 12px',
                }}
              >
                {t(fmt.labelKey)}
              </button>
            ))}
          </div>
          <ul id={formatTipsId} className="mt-1 flex list-disc flex-col gap-1 pl-4 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            <li>{t('setup.apiFormatTipDocs')}</li>
            <li>{t('setup.apiFormatTipBest')}</li>
          </ul>
        </div>

        <div className="flex flex-col gap-1.5">
          <label htmlFor={baseUrlId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.baseUrlLabel')}
          </label>
          <Input
            id={baseUrlId}
            type="url"
            value={form.baseUrl}
            onChange={(e) => form.setBaseUrl(e.target.value)}
            placeholder={t('setup.customBaseUrlPlaceholder')}
            className="font-mono text-xs"
          />
        </div>

        <div className="flex flex-col gap-1.5">
          <label htmlFor={apiKeyId} className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.apiKeyLabel')}
          </label>
          <ApiKeyInput
            id={apiKeyId}
            provider="custom"
            value={form.apiKey}
            onChange={form.setApiKey}
          />
        </div>

        <section className="flex flex-col gap-3" aria-live="polite">
          <h3 className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.endpointModels.title')}
          </h3>
          {!entered && <QuietNote>{t('setup.endpointModels.enterFirst')}</QuietNote>}
          {checking && (
            <div className="flex items-center gap-2">
              <Loader size={14} className="text-(--color-text-tertiary)" />
              <QuietNote>{t('setup.endpointModels.checking')}</QuietNote>
            </div>
          )}
          {!checking && form.unavailable && <QuietNote>{form.unavailable}</QuietNote>}
          {!checking && listing?.status === 'ok' && recognized.length === 0 && (
            <QuietNote>{t('setup.endpointModels.noneKnown')}</QuietNote>
          )}

          {!checking && recognized.length > 0 && (
            <div className="flex flex-col gap-2">
              <p id={knownLabelId} className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('setup.endpointModels.knownDesc')}
              </p>
              <div role="group" aria-labelledby={knownLabelId} className="flex max-h-[240px] flex-col gap-1 overflow-y-auto">
                {recognized.map((m) => {
                  const holder = form.takenBy.get(m.name);
                  return holder ? (
                    <QuietNote key={m.name} className="px-3 py-2">
                      {m.display_name} · {t('setup.endpointModels.takenBy', { provider: holder })}
                    </QuietNote>
                  ) : (
                    <ModelCheckRow
                      key={m.name}
                      checked={!form.unpicked.has(m.name)}
                      onToggle={() => form.toggle(m.name)}
                      label={m.display_name}
                    />
                  );
                })}
              </div>
            </div>
          )}

          {entered && !checking && (form.showManual ? (
            <div
              className="flex flex-col gap-3 rounded-lg p-4"
              style={{ background: 'var(--color-bg-surface)', border: '1px solid var(--color-border-default)' }}
            >
              <div className="flex flex-col gap-0.5">
                <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {t('setup.endpointModels.yourModel')}
                </span>
                <QuietNote>{t('setup.endpointModels.yourModelDesc')}</QuietNote>
              </div>
              <CustomModelFields state={form.manual} suggestions={form.others} autoFocus={form.manualOpen} />
            </div>
          ) : (
            <button
              type="button"
              onClick={form.openManual}
              className="inline-flex items-center gap-1 self-start text-xs underline underline-offset-2 hover:opacity-80"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              <Plus aria-hidden="true" className="h-3.5 w-3.5" />
              {t('setup.endpointModels.addAnother')}
            </button>
          ))}
        </section>

        {form.error && (
          <p role="alert" className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {form.error}
          </p>
        )}
      </div>

      <SetupFooter
        back={{ label: t('setup.back'), onClick: () => navigate('/setup/provider', { state: { list: state.list } }) }}
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
