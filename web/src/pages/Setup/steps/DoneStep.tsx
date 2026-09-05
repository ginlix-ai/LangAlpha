import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { CheckCircle2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { usePreferences } from '@/hooks/usePreferences';
import { useApiKeys } from '@/hooks/useApiKeys';
import type { ByokProvider } from '@/components/model/types';
import { useTranslation } from 'react-i18next';
import { modelPrefs } from '@/lib/modelPreferences';

// ---------------------------------------------------------------------------
// DoneStep — confirmation screen
// ---------------------------------------------------------------------------

export default function DoneStep() {
  const navigate = useNavigate();
  const { preferences } = usePreferences();
  const { apiKeys } = useApiKeys();
  const { t } = useTranslation();

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
  const primaryModel = modelPref.preferred_model ?? t('setup.notSet');
  const flashModel = modelPref.preferred_flash_model ?? t('setup.notSet');
  const providerLabel = configuredProviderNames.length > 0
    ? configuredProviderNames.join(', ')
    : t('setup.notConfigured');

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  return (
    <div className="flex flex-col items-center gap-8 py-4">
      {/* Success icon */}
      <div className="flex flex-col items-center gap-3">
        <CheckCircle2
          className="h-12 w-12"
          style={{ color: 'var(--color-success)' }}
        />
        <h2
          className="font-semibold text-center"
          style={{ fontSize: '1.25rem', color: 'var(--color-text-primary)' }}
        >
          {t('setup.doneTitle')}
        </h2>
        <p
          className="text-sm text-center max-w-md"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('setup.doneSummary')}
        </p>
      </div>

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
          <SummaryRow label={t('setup.primaryModelLabel')} value={primaryModel} />
          <SummaryRow label={t('setup.flashModelLabel')} value={flashModel} />
        </div>
      </div>

      {/* Actions */}
      <div className="flex flex-col items-center gap-3 w-full">
        <Button
          variant="default"
          className="w-full"
          onClick={() => navigate('/dashboard', { replace: true })}
        >
          {t('setup.goToDashboard')}
        </Button>
        <button
          type="button"
          className="text-sm font-medium transition-colors"
          style={{ color: 'var(--color-accent-primary)' }}
          onClick={() =>
            navigate('/chat/t/__default__', {
              replace: true,
              state: { isPersonalizing: true },
            })
          }
        >
          {t('setup.personalizeLink')}
        </button>
      </div>
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
