import { useId, useMemo, useState } from 'react';
import { useNavigate } from 'react-router';
import { CheckCircle2 } from 'lucide-react';
import { WizardStepHeader } from '@/components/ui/wizard';
import { SetupFooter } from '../SetupFooter';
import { usePreferences } from '@/hooks/usePreferences';
import { useApiKeys } from '@/hooks/useApiKeys';
import type { ByokProvider } from '@/components/model/types';
import { useTranslation } from 'react-i18next';
import { useModelName } from '@/hooks/useModelName';
import { modelPrefs } from '@/lib/modelPreferences';
import { useModelSlotCopy } from '@/components/model/useModelSlotCopy';
import { useWelcomeOffer } from '@/pages/Onboarding/welcome/useWelcomeOffer';
import { readSetupReturn, returnFromSetup } from '@/lib/setupReturn';

// ---------------------------------------------------------------------------
// DoneStep — confirmation screen
// ---------------------------------------------------------------------------

export default function DoneStep() {
  const navigate = useNavigate();
  const { preferences } = usePreferences();
  const { apiKeys } = useApiKeys();
  const { t } = useTranslation();
  const copy = useModelSlotCopy();
  const modelName = useModelName();
  const welcomeOffer = useWelcomeOffer();
  const headingId = useId();
  // A connection sends the user back the moment it works, so a return that
  // reaches this screen came another way (an invitation code, or skipping
  // ahead past the providers already connected). It still wins over the
  // dashboard. This screen doesn't know which provider, so it names none.
  const [hasReturn] = useState(() => readSetupReturn() !== null);

  const handlePrimary = () => {
    if (hasReturn && returnFromSetup()) return;
    navigate('/dashboard', { replace: true });
  };

  // ---------------------------------------------------------------------------
  // Derive summary data
  // ---------------------------------------------------------------------------

  const configuredProviderNames = useMemo<string[]>(() => {
    if (!apiKeys) return [];
    const keys = apiKeys as Record<string, unknown>;
    if (Array.isArray(keys.providers)) {
      return (keys.providers as ByokProvider[])
        .filter((p) => p.has_key !== false)
        .map((p) => p.display_name || p.provider);
    }
    return Object.entries(keys)
      .filter(([, v]) => v && typeof v === 'object')
      .map(([k, v]) => (v as Record<string, unknown>).display_name as string ?? k);
  }, [apiKeys]);

  const modelPref = modelPrefs(preferences);
  const primaryModel = modelPref.preferred_model
    ? modelName(modelPref.preferred_model)
    : t('setup.notSet');
  const backgroundModel = modelPref.preferred_background_model
    ? modelName(modelPref.preferred_background_model)
    : t('setup.notSet');
  const providerLabel = configuredProviderNames.length > 0
    ? configuredProviderNames.join(', ')
    : t('setup.notConfigured');

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div>
      {/* Success icon */}
      <CheckCircle2
        aria-hidden="true"
        className="mb-4 h-12 w-12"
        style={{ color: 'var(--color-success)' }}
      />
      <WizardStepHeader titleId={headingId} title={t('setup.doneTitle')} helper={t('setup.doneSummary')} />

      {/* Summary card */}
      <div
        className="w-full rounded-lg overflow-hidden"
        style={{
          background: 'var(--color-bg-surface)',
          border: '1px solid var(--color-border-default)',
        }}
      >
        <div className="flex flex-col divide-y" style={{ borderColor: 'var(--color-border-default)' }}>
          <SummaryRow label={t('setup.providerLabel')} value={providerLabel} />
          <SummaryRow label={copy.default.summary} value={primaryModel} />
          <SummaryRow label={copy.background.summary} value={backgroundModel} />
        </div>
      </div>

      <SetupFooter
        // Not offered on the way back to a return: the welcome flow ends in
        // the app, so the setup that sent the user here would never resume.
        skip={
          welcomeOffer && !hasReturn
            ? { label: t('setup.personalizeLink'), onClick: () => welcomeOffer.open({ replace: true }) }
            : null
        }
        primary={{
          label: hasReturn ? t('setup.continue') : t('setup.goToDashboard'),
          onClick: handlePrimary,
        }}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// SummaryRow
// ---------------------------------------------------------------------------

function SummaryRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between px-4 py-3">
      <span
        className="text-sm"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {label}
      </span>
      <span
        className="text-sm font-medium text-right"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {value}
      </span>
    </div>
  );
}
