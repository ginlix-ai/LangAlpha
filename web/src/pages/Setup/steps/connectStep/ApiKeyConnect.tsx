import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ApiKeyInput, type TestResult } from '@/components/model/ApiKeyInput';
import { useUpdateApiKeys } from '@/hooks/useApiKeys';
import { queryKeys } from '@/lib/queryKeys';
import { useTranslation } from 'react-i18next';
import { getApiFormatKey, testApiKey, type LocationState } from './shared';

export function ApiKeyConnect({ state }: { state: LocationState }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const updateApiKeys = useUpdateApiKeys();
  const { t } = useTranslation();

  const method = state.method ?? 'api_key';
  const provider = state.provider ?? '';
  const displayName = state.displayName ?? provider;
  const brandKey = state.brandKey ?? provider;
  const sdk = state.sdk ?? null;
  const defaultBaseUrl = state.defaultBaseUrl ?? null;
  const useResponseApi = state.useResponseApi ?? false;
  const regionVariants = state.regionVariants ?? null;
  const defaultRegion = state.defaultRegion ?? null;
  const dynamicModels = state.dynamicModels ?? false;
  const apiFormatKey = getApiFormatKey(sdk, useResponseApi);

  // Region selection state — when variants exist, user can switch
  const [selectedRegion, setSelectedRegion] = useState<string | null>(null);

  // Compute effective provider/base_url/sdk based on region selection
  const activeVariant = selectedRegion && regionVariants
    ? regionVariants.find((v) => v.region === selectedRegion)
    : null;
  const effectiveProvider = activeVariant?.provider ?? provider;
  const effectiveBaseUrl = activeVariant?.base_url ?? defaultBaseUrl ?? '';
  const effectiveSdk = activeVariant?.sdk ?? sdk;
  const effectiveUseResponseApi = activeVariant?.use_response_api ?? useResponseApi;
  const effectiveApiFormatKey = activeVariant
    ? getApiFormatKey(effectiveSdk, effectiveUseResponseApi)
    : apiFormatKey;

  const handleRegionChange = useCallback((region: string | null) => {
    setSelectedRegion(region);
    if (region && regionVariants) {
      const v = regionVariants.find((rv) => rv.region === region);
      if (v?.base_url) setBaseUrl(v.base_url);
    } else {
      setBaseUrl(defaultBaseUrl ?? '');
    }
  }, [regionVariants, defaultBaseUrl]);

  // API key / coding plan state
  const [apiKey, setApiKey] = useState('');
  const [baseUrl, setBaseUrl] = useState(defaultBaseUrl ?? '');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // ---------------------------------------------------------------------------
  // API key / coding plan handlers
  // ---------------------------------------------------------------------------

  const handleTestKey = useCallback(
    async (_provider: string, key: string): Promise<TestResult> => {
      return testApiKey(effectiveProvider || brandKey, key, baseUrl || undefined);
    },
    [effectiveProvider, brandKey, baseUrl],
  );

  const handleSaveAndNext = useCallback(async () => {
    if (!dynamicModels && !apiKey.trim()) {
      setError(t('setup.errorNoApiKey'));
      return;
    }
    if (dynamicModels && !baseUrl.trim()) {
      setError(t('setup.errorNoBaseUrl'));
      return;
    }

    setSaving(true);
    setError(null);

    try {
      const saveProvider = effectiveProvider || provider;
      const payload: Record<string, unknown> = {
        byok_enabled: true,
        api_keys: { [saveProvider]: apiKey },
      };
      if (baseUrl.trim()) {
        payload.base_urls = { [saveProvider]: baseUrl };
      }
      await updateApiKeys.mutateAsync(payload);

      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.user.me() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() }),
      ]);

      if (dynamicModels) {
        // Dynamic providers (LM Studio, vLLM, Ollama) — go to model discovery
        navigate('/setup/connect', {
          state: {
            method,
            provider: saveProvider,
            displayName,
            brandKey,
            isExistingCustom: true,
            dynamicModels: true,
          },
        });
      } else {
        navigate('/setup/models', {
          state: { method, provider: saveProvider, displayName, brandKey },
        });
      }
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : err?.message ?? t('setup.errorSaveKey'));
    } finally {
      setSaving(false);
    }
  }, [apiKey, baseUrl, dynamicModels, effectiveProvider, provider, updateApiKeys, queryClient, navigate, method, displayName, brandKey, t]);

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { method } });
  }, [navigate, method]);

  return (
    <div className="flex flex-col gap-4 sm:gap-6">
      <div className="flex flex-col gap-1">
        <h2
          className="font-semibold"
          style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
        >
          {t('setup.connectTitle', { provider: displayName })}
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {dynamicModels
            ? t('setup.localServerDesc')
            : method === 'coding_plan'
              ? t('setup.codingPlanDesc')
              : t('setup.apiKeyInputDesc')}
        </p>
      </div>

      {/* Region toggle — shown when provider has region variants */}
      {regionVariants && regionVariants.length > 0 && (
        <div className="flex items-center gap-2">
          <span
            className="text-xs font-medium"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('setup.regionLabel')}
          </span>
          <div
            className="inline-flex rounded-md overflow-hidden"
            style={{ border: '1px solid var(--color-border-default)' }}
          >
            {/* Default region option */}
            <button
              type="button"
              onClick={() => handleRegionChange(null)}
              className="px-3 py-1 text-xs font-medium transition-colors"
              style={{
                background: !selectedRegion ? 'var(--color-accent-primary)' : 'var(--color-bg-surface)',
                color: !selectedRegion ? '#fff' : 'var(--color-text-secondary)',
              }}
            >
              {(defaultRegion === 'cn' ? t('setup.regionChina') : defaultRegion === 'sg' ? t('setup.regionSingapore') : t('setup.regionInternational'))}
            </button>
            {regionVariants.map((rv) => (
              <button
                key={rv.provider}
                type="button"
                onClick={() => handleRegionChange(rv.region)}
                className="px-3 py-1 text-xs font-medium transition-colors"
                style={{
                  background: selectedRegion === rv.region ? 'var(--color-accent-primary)' : 'var(--color-bg-surface)',
                  color: selectedRegion === rv.region ? '#fff' : 'var(--color-text-secondary)',
                  borderLeft: '1px solid var(--color-border-default)',
                }}
              >
                {rv.region === 'cn' ? t('setup.regionChina') : rv.region === 'sg' ? t('setup.regionSingapore') : rv.region === 'intl' ? t('setup.regionInternational') : rv.region}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* Provider info: SDK format + base URL */}
      <div
        className="rounded-lg p-4 flex flex-col gap-3"
        style={{
          background: 'var(--color-bg-surface)',
          border: '1px solid var(--color-border-default)',
        }}
      >
        {/* SDK format badge */}
        <div className="flex items-center gap-2">
          <span
            className="text-xs font-medium"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('setup.apiFormatLabel')}
          </span>
          <span
            className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
            style={{
              background: 'var(--color-accent-soft)',
              color: 'var(--color-accent-primary)',
            }}
          >
            {t(effectiveApiFormatKey, { sdk: effectiveSdk })}
          </span>
        </div>

        {/* Base URL — always shown, always editable */}
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <label
              className="text-xs font-medium"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('setup.baseUrlLabel')}
            </label>
            {baseUrl !== effectiveBaseUrl && effectiveBaseUrl && (
              <button
                type="button"
                onClick={() => setBaseUrl(effectiveBaseUrl)}
                className="text-[11px]"
                style={{ color: 'var(--color-accent-primary)' }}
              >
                {t('setup.resetToDefault')}
              </button>
            )}
          </div>
          <Input
            type="url"
            value={baseUrl}
            onChange={(e) => setBaseUrl(e.target.value)}
            placeholder={effectiveBaseUrl || 'https://...'}
            className="font-mono text-xs"
          />
          {effectiveBaseUrl && baseUrl !== effectiveBaseUrl && baseUrl.trim() !== '' && (
            <p className="text-[11px]" style={{ color: 'var(--color-warning, #f59e0b)' }}>
              {t('setup.customUrlWarning', { url: effectiveBaseUrl })}
            </p>
          )}
        </div>
      </div>

      {/* API key input */}
      <div className="flex flex-col gap-3">
        <label
          className="block text-sm font-medium"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('setup.providerApiKey', { provider: displayName })}
          {dynamicModels && (
            <span className="text-xs font-normal ml-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('setup.optional')}
            </span>
          )}
        </label>
        <ApiKeyInput
          provider={provider}
          value={apiKey}
          onChange={setApiKey}
          onTest={dynamicModels ? undefined : handleTestKey}
        />
      </div>

      {error && (
        <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}

      <div className="flex items-center justify-between pt-2">
        <Button variant="outline" onClick={handleBack}>
          {t('setup.back')}
        </Button>
        <Button
          variant="default"
          disabled={saving || (!dynamicModels && !apiKey.trim()) || (dynamicModels && !baseUrl.trim())}
          onClick={handleSaveAndNext}
          className="min-w-[120px]"
        >
          {saving ? (
            <>
              <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
              {t('setup.saving')}
            </>
          ) : (
            t('setup.continue')
          )}
        </Button>
      </div>
    </div>
  );
}
