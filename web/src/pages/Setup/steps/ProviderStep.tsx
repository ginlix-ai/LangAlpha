import { useState, useCallback, useMemo } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Loader2, Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ProviderCard } from '@/components/model/ProviderCard';
import { useModels } from '@/hooks/useModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import type { AccessType, ProviderCatalogEntry } from '@/components/model/types';

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
  const [selected, setSelected] = useState<string | null>(null);

  // Extract provider_catalog from models response
  const catalog = useMemo<ProviderCatalogEntry[]>(() => {
    if (!modelsData) return [];
    const raw = modelsData as Record<string, unknown>;
    return (raw.provider_catalog as ProviderCatalogEntry[] | undefined) ?? [];
  }, [modelsData]);

  const isLocalDev = !import.meta.env.VITE_SUPABASE_URL;

  // Filter catalog by selected method, split cloud vs local
  const filteredProviders = useMemo(
    () => catalog.filter((p) => p.access_type === method && !p.local_only),
    [catalog, method],
  );

  const localProviders = useMemo(
    () => isLocalDev ? catalog.filter((p) => p.access_type === method && p.local_only) : [],
    [catalog, method, isLocalDev],
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
        <Loader2 className="h-5 w-5 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
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
          Choose your provider
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {method === 'oauth' && 'Select the subscription you want to connect.'}
          {method === 'coding_plan' && 'Select your coding plan provider.'}
          {method === 'api_key' && 'Select the provider you have an API key for.'}
        </p>
      </div>

      {/* Provider grid */}
      {filteredProviders.length === 0 ? (
        <p
          className="text-sm py-8 text-center"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          No providers available for this method.
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
              configured={configuredSet.has(p.provider) || configuredSet.has(p.brand_key)}
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

      {/* Local model providers (dev only) */}
      {localProviders.length > 0 && method === 'api_key' && (
        <>
          <div className="flex items-center gap-2">
            <div className="h-px flex-1" style={{ background: 'var(--color-border-default)' }} />
            <span className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
              Local models
            </span>
            <div className="h-px flex-1" style={{ background: 'var(--color-border-default)' }} />
          </div>
          <div
            role="radiogroup"
            aria-label="Local model providers"
            className="grid grid-cols-2 sm:grid-cols-3 gap-3"
          >
            {localProviders.map((p) => (
              <ProviderCard
                key={p.provider}
                provider={p.provider}
                displayName={p.display_name}
                selected={selected === p.provider}
                configured={configuredSet.has(p.provider)}
                onSelect={setSelected}
              />
            ))}
          </div>
        </>
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
              Custom provider
            </span>
            <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              Any OpenAI, Anthropic, or Gemini compatible endpoint
            </span>
          </div>
        </button>
      )}

      {/* Navigation buttons */}
      <div className="flex items-center justify-between pt-2">
        <Button variant="outline" onClick={handleBack}>
          Back
        </Button>
        <Button
          variant="default"
          disabled={!selected}
          onClick={handleNext}
          className="min-w-[120px]"
        >
          Next
        </Button>
      </div>
    </div>
  );
}
