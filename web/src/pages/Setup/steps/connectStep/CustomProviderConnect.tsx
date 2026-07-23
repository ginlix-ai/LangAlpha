import { useState, useCallback, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Loader2, Check } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ApiKeyInput } from '@/components/model/ApiKeyInput';
import { useUpdateApiKeys } from '@/hooks/useApiKeys';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import { api } from '@/api/client';
import { useTranslation } from 'react-i18next';
import { mergeCustomModelsForSlug, type CustomModelEntry } from '../mergeCustomModelsForSlug';
import { API_FORMATS, useModalityState, type LocationState, type ParentModel } from './shared';

export function CustomProviderConnect({ state }: { state: LocationState }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const updateApiKeys = useUpdateApiKeys();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { t } = useTranslation();

  const method = state.method ?? 'api_key';
  const isCustom = state.isCustom ?? false;

  // Custom provider state
  const [customName, setCustomName] = useState('');
  const [customFormat, setCustomFormat] = useState<string>('openai-completions');
  const [customBaseUrl, setCustomBaseUrl] = useState('');
  const [customApiKey, setCustomApiKey] = useState('');
  const [customModelName, setCustomModelName] = useState('');
  const [customModelId, setCustomModelId] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { manualModalities, toggleManualModality, buildModalitiesArray } = useModalityState();

  // Parent-model import state (for custom providers) — fetched from
  // /api/v1/providers/{parent}/visible-models once the user picks an API format.
  const [parentModels, setParentModels] = useState<ParentModel[]>([]);
  const [parentModelsLoading, setParentModelsLoading] = useState(false);
  const [selectedParentModelNames, setSelectedParentModelNames] = useState<Set<string>>(new Set());

  // Fetch parent's visible models whenever the API format changes in the
  // custom-provider flow. Lets the user pre-select a catalog with one click
  // instead of typing each model name.
  useEffect(() => {
    if (!isCustom) return;
    const format = API_FORMATS.find((f) => f.value === customFormat);
    const parent = format?.parent ?? 'openai';
    let cancelled = false;
    setParentModelsLoading(true);
    setParentModels([]);
    setSelectedParentModelNames(new Set());
    api.get(`/api/v1/providers/${parent}/visible-models`)
      .then(({ data }) => {
        if (cancelled) return;
        const models = (data.models ?? []) as ParentModel[];
        setParentModels(models);
        // Default: all checked — one click imports the full catalog.
        setSelectedParentModelNames(new Set(models.map((m) => m.name)));
      })
      .catch(() => {
        if (cancelled) return;
        // Soft failure: the section just stays empty.
        setParentModels([]);
      })
      .finally(() => { if (!cancelled) setParentModelsLoading(false); });
    return () => { cancelled = true; };
  }, [isCustom, customFormat]);

  const toggleParentModel = useCallback((name: string) => {
    setSelectedParentModelNames((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name); else next.add(name);
      return next;
    });
  }, []);

  const selectAllParentModels = useCallback(() => {
    setSelectedParentModelNames(new Set(parentModels.map((m) => m.name)));
  }, [parentModels]);

  const selectNoParentModels = useCallback(() => {
    setSelectedParentModelNames(new Set());
  }, []);

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { method } });
  }, [navigate, method]);

  // ---------------------------------------------------------------------------
  // Custom provider save
  // ---------------------------------------------------------------------------

  const handleCustomSave = useCallback(async () => {
    const slug = customName.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '-');
    const hasManual = customModelName.trim().length > 0;
    const importCount = selectedParentModelNames.size;
    if (!slug || !customBaseUrl.trim() || !customApiKey.trim() || (!hasManual && importCount === 0)) {
      setError(t('setup.errorFillFields'));
      return;
    }

    // Shadow semantics: a manual name is free to match a built-in. The
    // resolver checks the user's custom entry first, so the variant key
    // wins when names collide.

    setSaving(true);
    setError(null);

    try {
      const format = API_FORMATS.find((f) => f.value === customFormat);
      const parentProvider = format?.parent ?? 'openai';
      const useRespApi = format?.useResponseApi ?? false;

      // 1. Read existing custom_providers/custom_models from current preferences
      const prefs = (preferences ?? {}) as Record<string, unknown>;
      const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
      const existingProviders = (Array.isArray(otherPref.custom_providers) ? otherPref.custom_providers : []) as Array<Record<string, unknown>>;
      const existingModels = (Array.isArray(otherPref.custom_models) ? otherPref.custom_models : []) as Array<Record<string, unknown>>;

      const newProvider: Record<string, unknown> = {
        name: slug,
        parent_provider: parentProvider,
      };
      if (useRespApi) newProvider.use_response_api = true;

      // Build the list of new models: imported parent models (routed through
      // the variant) plus the user's manually-typed model, if any. Models
      // imported from the parent keep their original name/model_id; only the
      // ``provider`` is rewritten to the new variant so BYOK routes through
      // the user's key/base_url.
      const newModels: Array<Record<string, unknown>> = [];
      for (const pm of parentModels) {
        if (!selectedParentModelNames.has(pm.name)) continue;
        const entry: Record<string, unknown> = {
          name: pm.name,
          model_id: pm.model_id,
          provider: slug,
        };
        if (pm.input_modalities && pm.input_modalities.length > 0) {
          entry.input_modalities = pm.input_modalities;
        }
        newModels.push(entry);
      }
      if (hasManual) {
        const entry: Record<string, unknown> = {
          name: customModelName.trim(),
          model_id: customModelId.trim() || customModelName.trim(),
          provider: slug,
        };
        const mods = buildModalitiesArray(manualModalities);
        if (mods) entry.input_modalities = mods;
        // Manual entry wins if it collides with an imported one.
        const idx = newModels.findIndex((m) => m.name === entry.name);
        if (idx >= 0) newModels.splice(idx, 1);
        newModels.push(entry);
      }

      // Preserve existing ``custom_models`` entries for this slug whose name
      // isn't in the new set — user-added entries (via ModelPickStep) must
      // survive the "Add new custom provider" flow even when the user re-uses
      // an existing slug. Imported/manual entries from this wizard pass win on
      // name collision; anything the user added elsewhere under this slug
      // keeps its current config.
      const mergedModels = mergeCustomModelsForSlug({
        existing: existingModels as unknown as CustomModelEntry[],
        slug,
        newForSlug: newModels as unknown as CustomModelEntry[],
      });

      // Only send custom_providers and custom_models — backend merges into existing JSONB
      await updatePreferences.mutateAsync({
        other_preference: {
          custom_providers: [...existingProviders.filter((p) => p.name !== slug), newProvider],
          custom_models: mergedModels,
        },
      });

      // 2. Enable BYOK first (separate call to ensure flag is set)
      await updateApiKeys.mutateAsync({ byok_enabled: true });

      // 3. Save API key + base URL (provider is now in allowed list after prefs save)
      await updateApiKeys.mutateAsync({
        api_keys: { [slug]: customApiKey },
        base_urls: { [slug]: customBaseUrl },
      });

      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.user.me() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.user.apiKeys() }),
        queryClient.invalidateQueries({ queryKey: queryKeys.models.all }),
      ]);

      navigate('/setup/models', {
        state: { method, provider: slug, displayName: customName.trim(), brandKey: slug },
      });
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string | Array<{ msg?: string }> } }; message?: string };
      const detail = err?.response?.data?.detail;
      const msg = typeof detail === 'string'
        ? detail
        : Array.isArray(detail) ? detail.map((d) => d.msg).filter(Boolean).join('; ') : null;
      setError(msg || err?.message || t('setup.errorSaveProvider'));
    } finally {
      setSaving(false);
    }
  }, [customName, customFormat, customBaseUrl, customApiKey, customModelName, customModelId, preferences, updatePreferences, updateApiKeys, queryClient, navigate, method, t, manualModalities, buildModalitiesArray, parentModels, selectedParentModelNames]);

    return (
      <div className="flex flex-col gap-4 sm:gap-6">
        <div className="flex flex-col gap-1">
          <h2
            className="font-semibold"
            style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
          >
            {t('setup.addCustomProvider')}
          </h2>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('setup.addCustomProviderDesc')}
          </p>
        </div>

        {/* Provider name */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.providerNameLabel')}
          </label>
          <Input
            value={customName}
            onChange={(e) => setCustomName(e.target.value)}
            placeholder={t('setup.providerNamePlaceholder')}
            autoComplete="off"
          />
        </div>

        {/* API format */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.apiFormatLabel')}
          </label>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {API_FORMATS.map((fmt) => (
              <button
                key={fmt.value}
                type="button"
                onClick={() => setCustomFormat(fmt.value)}
                className="rounded-lg px-3 py-2.5 text-left text-xs font-medium transition-colors"
                style={{
                  border: customFormat === fmt.value
                    ? '2px solid var(--color-accent-primary)'
                    : '1px solid var(--color-border-default)',
                  background: customFormat === fmt.value ? 'var(--color-accent-soft)' : undefined,
                  color: 'var(--color-text-primary)',
                  padding: customFormat === fmt.value ? '9px 11px' : '10px 12px',
                }}
              >
                {t(fmt.labelKey)}
              </button>
            ))}
          </div>
        </div>

        {/* Base URL */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.baseUrlLabel')}
          </label>
          <Input
            type="url"
            value={customBaseUrl}
            onChange={(e) => setCustomBaseUrl(e.target.value)}
            placeholder={t('setup.customBaseUrlPlaceholder')}
            className="font-mono text-xs"
          />
        </div>

        {/* API key */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.apiKeyLabel')}
          </label>
          <ApiKeyInput
            provider="custom"
            value={customApiKey}
            onChange={setCustomApiKey}
          />
        </div>

        {/* Model */}
        <div
          className="rounded-lg p-4 flex flex-col gap-3"
          style={{
            background: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-default)',
          }}
        >
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('setup.modelLabel')}
          </label>
          <div className="flex flex-col gap-2">
            <Input
              value={customModelName}
              onChange={(e) => {
                setCustomModelName(e.target.value);
                if (!customModelId) setCustomModelId(e.target.value);
              }}
              placeholder={t('setup.modelDisplayNamePlaceholder')}
              autoComplete="off"
            />
            <Input
              value={customModelId}
              onChange={(e) => setCustomModelId(e.target.value)}
              placeholder={t('setup.modelIdPlaceholder')}
              className="font-mono text-xs"
              autoComplete="off"
            />
          </div>
          <p className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('setup.modelIdHint')}
          </p>
          <div className="flex items-center gap-1.5 pt-1">
            <span className="text-[10px] uppercase tracking-wider" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('setup.capabilities', { defaultValue: 'Capabilities' })}:
            </span>
            <span
              className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium"
              style={{ background: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)', opacity: 0.6 }}
            >
              Text
            </span>
            {(['image', 'pdf'] as const).map((mod) => {
              const active = manualModalities.has(mod);
              return (
                <button
                  key={mod}
                  type="button"
                  onClick={() => toggleManualModality(mod)}
                  className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium transition-colors"
                  style={{
                    background: active ? 'var(--color-accent-soft)' : 'transparent',
                    color: active ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)',
                    border: `1px solid ${active ? 'var(--color-accent-primary)' : 'var(--color-border-default)'}`,
                  }}
                >
                  {mod === 'image' ? 'Image' : 'PDF'}
                </button>
              );
            })}
          </div>
        </div>

        {/* Import parent models — auto-selected; users uncheck what they don't want. */}
        {(parentModelsLoading || parentModels.length > 0) && (
          <div
            className="rounded-lg p-4 flex flex-col gap-3"
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
            }}
          >
            <div className="flex items-center justify-between gap-2">
              <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('setup.importParentModels', { defaultValue: 'Also add these models' })}
              </label>
              {parentModels.length > 0 && (
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={selectAllParentModels}
                    className="text-[11px] font-medium transition-colors"
                    style={{ color: 'var(--color-accent-primary)' }}
                  >
                    {t('setup.selectAll', { defaultValue: 'All' })}
                  </button>
                  <span className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>·</span>
                  <button
                    type="button"
                    onClick={selectNoParentModels}
                    className="text-[11px] font-medium transition-colors"
                    style={{ color: 'var(--color-text-secondary)' }}
                  >
                    {t('setup.selectNone', { defaultValue: 'None' })}
                  </button>
                </div>
              )}
            </div>
            <p className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('setup.importParentModelsDesc', {
                defaultValue: 'Models from the parent catalog, routed through your endpoint and key. Uncheck any you don\'t need.',
              })}
            </p>
            {parentModelsLoading && (
              <div className="flex items-center gap-2 py-2">
                <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('setup.fetchingModels', { defaultValue: 'Fetching models...' })}
                </span>
              </div>
            )}
            {!parentModelsLoading && parentModels.length > 0 && (
              <div className="flex flex-col gap-1 max-h-[240px] overflow-y-auto">
                {parentModels.map((pm) => {
                  const checked = selectedParentModelNames.has(pm.name);
                  return (
                    <button
                      key={pm.name}
                      type="button"
                      onClick={() => toggleParentModel(pm.name)}
                      className="flex items-center gap-3 rounded-md px-3 py-2 text-left transition-colors"
                      style={{
                        background: checked ? 'var(--color-accent-soft)' : 'transparent',
                        border: checked
                          ? '1px solid var(--color-accent-primary)'
                          : '1px solid var(--color-border-default)',
                      }}
                    >
                      <div
                        className="flex-shrink-0 h-4 w-4 rounded border flex items-center justify-center"
                        style={{
                          borderColor: checked ? 'var(--color-accent-primary)' : 'var(--color-border-default)',
                          background: checked ? 'var(--color-accent-primary)' : 'transparent',
                        }}
                      >
                        {checked && <Check className="h-3 w-3" style={{ color: '#fff' }} />}
                      </div>
                      <span className="text-sm font-mono" style={{ color: 'var(--color-text-primary)' }}>
                        {pm.display_name || pm.name}
                      </span>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        )}

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
            disabled={
              saving
              || !customName.trim()
              || !customBaseUrl.trim()
              || !customApiKey.trim()
              || (!customModelName.trim() && selectedParentModelNames.size === 0)
            }
            onClick={handleCustomSave}
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
