import { useState, useCallback, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { ProviderCard } from '@/components/model/ProviderCard';
import { useModels } from '@/hooks/useModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import type { AccessType, ProviderCatalogEntry } from '@/components/model/types';
import { useTranslation } from 'react-i18next';

const CUSTOM_PROVIDER_KEY = '__custom__';

// ---------------------------------------------------------------------------
// ProviderStep — Step 2: Choose provider filtered by method
// ---------------------------------------------------------------------------

export default function ProviderStep() {
  const navigate = useNavigate();
  const location = useLocation();

  // Method passed from MethodStep via location state
  const method = (location.state as { method?: AccessType } | null)?.method ?? 'api_key';

  const { models: modelsData, isLoading } = useModels();
  const { preferences } = usePreferences();
  const { configuredSet } = useConfiguredProviders();
  const { t } = useTranslation();
  const [selected, setSelected] = useState<string | null>(null);

  // Extract provider_catalog from models response
  const catalog = useMemo<ProviderCatalogEntry[]>(() => {
    if (!modelsData) return [];
    const raw = modelsData as Record<string, unknown>;
    return (raw.provider_catalog as ProviderCatalogEntry[] | undefined) ?? [];
  }, [modelsData]);

  // Filter catalog by selected method
  const filteredProviders = useMemo(
    () => catalog.filter((p) => p.access_type === method),
    [catalog, method],
  );

  // User's existing custom providers (from preferences)
  const userCustomProviders = useMemo(() => {
    if (!preferences) return [];
    const prefs = preferences as Record<string, unknown>;
    const other = (prefs.other_preference ?? {}) as Record<string, unknown>;
    const cp = other.custom_providers;
    if (!Array.isArray(cp)) return [];
    return cp as Array<{ name: string; parent_provider: string; use_response_api?: boolean }>;
  }, [preferences]);

  // Set of custom provider names for lookup
  const customProviderSet = useMemo(
    () => new Set(userCustomProviders.map((p) => p.name)),
    [userCustomProviders],
  );

  const handleBack = useCallback(() => {
    navigate('/setup/method');
  }, [navigate]);

  const handleNext = useCallback(() => {
    if (!selected) return;

    if (selected === CUSTOM_PROVIDER_KEY) {
      navigate('/setup/connect', {
        state: { method, isCustom: true },
      });
      return;
    }

    // Existing custom provider — already has a key, go straight to model management
    if (customProviderSet.has(selected)) {
      navigate('/setup/models', {
        state: {
          method,
          provider: selected,
          displayName: selected,
          brandKey: selected,
        },
      });
      return;
    }

    const provider = filteredProviders.find((p) => p.provider === selected);
    const providerState = {
      method,
      provider: selected,
      displayName: provider?.display_name ?? selected,
      brandKey: provider?.brand_key ?? selected,
      sdk: provider?.sdk ?? null,
      defaultBaseUrl: provider?.base_url ?? null,
      useResponseApi: provider?.use_response_api ?? false,
      regionVariants: provider?.region_variants ?? null,
      defaultRegion: provider?.region ?? null,
      dynamicModels: provider?.dynamic_models ?? false,
    };

    // Already connected? Skip key input, go straight to model management
    if (configuredSet.has(selected)) {
      navigate('/setup/models', { state: providerState });
      return;
    }

    navigate('/setup/connect', { state: providerState });
  }, [selected, method, filteredProviders, customProviderSet, configuredSet, navigate]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-20">
        <Loader size={20} className="text-[color:var(--color-text-tertiary)]" />
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-4 sm:gap-6">
      {/* Section heading */}
      <div className="flex flex-col gap-1">
        <h2
          className="font-semibold"
          style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
        >
          {t('setup.providerTitle')}
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {method === 'oauth' && t('setup.providerSubtitleOAuth')}
          {method === 'coding_plan' && t('setup.providerSubtitleCodingPlan')}
          {method === 'api_key' && t('setup.providerSubtitleApiKey')}
          {method === 'local' && t('setup.providerSubtitleLocal')}
        </p>
      </div>

      {/* Provider grid */}
      {filteredProviders.length === 0 ? (
        <p
          className="text-sm py-8 text-center"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('setup.noProviders')}
        </p>
      ) : (
        <div
          role="radiogroup"
          aria-label="Choose your AI provider"
          className="grid grid-cols-2 sm:grid-cols-3 gap-3"
        >
          {filteredProviders.map((p) => (
            <ProviderCard
              key={p.provider}
              provider={p.provider}
              displayName={p.display_name}
              selected={selected === p.provider}
              // brand_key fallback only for api_key — OAuth/coding_plan providers have unique keys
              configured={configuredSet.has(p.provider) || (method === 'api_key' && configuredSet.has(p.brand_key))}
              onSelect={setSelected}
            />
          ))}
          {/* User's existing custom providers (api_key method only) */}
          {method === 'api_key' && userCustomProviders.map((cp) => (
            <ProviderCard
              key={cp.name}
              provider={cp.name}
              displayName={cp.name}
              selected={selected === cp.name}
              configured={configuredSet.has(cp.name)}
              onSelect={setSelected}
            />
          ))}
        </div>
      )}

      {/* Custom provider option (api_key and coding_plan only) */}
      {method !== 'oauth' && (
        <button
          type="button"
          role="radio"
          aria-checked={selected === CUSTOM_PROVIDER_KEY}
          onClick={() => setSelected(CUSTOM_PROVIDER_KEY)}
          className="flex items-center gap-3 rounded-lg p-3 text-left transition-colors cursor-pointer w-full"
          style={{
            border: selected === CUSTOM_PROVIDER_KEY
              ? '2px solid var(--color-accent-primary)'
              : '1px dashed var(--color-border-default)',
            background: selected === CUSTOM_PROVIDER_KEY ? 'var(--color-accent-soft)' : undefined,
            padding: selected === CUSTOM_PROVIDER_KEY ? 11 : 12,
          }}
        >
          <div
            className="flex items-center justify-center w-8 h-8 rounded-full shrink-0"
            style={{
              background: selected === CUSTOM_PROVIDER_KEY
                ? 'var(--color-accent-primary)'
                : 'var(--color-bg-surface)',
              color: selected === CUSTOM_PROVIDER_KEY ? '#fff' : 'var(--color-text-tertiary)',
            }}
          >
            <Plus className="h-4 w-4" />
          </div>
          <div className="flex flex-col">
            <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('setup.customProvider')}
            </span>
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('setup.customProviderDesc')}
            </span>
          </div>
        </button>
      )}

      {/* Navigation buttons */}
      <div className="flex items-center justify-between pt-2">
        <Button variant="outline" onClick={handleBack}>
          {t('setup.back')}
        </Button>
        <Button
          variant="default"
          disabled={!selected}
          onClick={handleNext}
          className="min-w-[120px]"
        >
          {t('setup.next')}
        </Button>
      </div>
    </div>
  );
}
