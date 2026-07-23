import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Loader2, Check, AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import { api } from '@/api/client';
import { useTranslation } from 'react-i18next';
import { useCallback } from 'react';
import { useModalityState, type LocationState } from './shared';

export function ExistingCustomConnect({ state }: { state: LocationState }) {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { t } = useTranslation();

  const method = state.method ?? 'api_key';
  const isExistingCustom = state.isExistingCustom ?? false;
  const provider = state.provider ?? '';
  const displayName = state.displayName ?? provider;
  const brandKey = state.brandKey ?? provider;
  const dynamicModels = state.dynamicModels ?? false;

  const [customModelName, setCustomModelName] = useState('');
  const [customModelId, setCustomModelId] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const { modelModalities, manualModalities, toggleDiscoveredModality, toggleManualModality, buildModalitiesArray } = useModalityState();

  // Dynamic model discovery state (for local providers)
  const [discoveredModels, setDiscoveredModels] = useState<Array<{ id: string; name: string }>>([]);
  const [selectedModelIds, setSelectedModelIds] = useState<Set<string>>(new Set());
  const [loadingModels, setLoadingModels] = useState(false);
  const [modelsError, setModelsError] = useState<string | null>(null);

  // Fetch models from provider when entering isExistingCustom (dynamic providers)
  useEffect(() => {
    if (!isExistingCustom || !dynamicModels || !provider) return;
    let cancelled = false;
    setLoadingModels(true);
    setModelsError(null);
    api.get(`/api/v1/providers/${provider}/models`)
      .then(({ data }) => {
        if (cancelled) return;
        const models = (data.models ?? []) as Array<{ id: string; name: string }>;
        setDiscoveredModels(models);
      })
      .catch((err) => {
        if (cancelled) return;
        const detail = err?.response?.data?.detail;
        setModelsError(typeof detail === 'string' ? detail : 'Could not fetch models from provider');
      })
      .finally(() => { if (!cancelled) setLoadingModels(false); });
    return () => { cancelled = true; };
  }, [isExistingCustom, dynamicModels, provider]);

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { method } });
  }, [navigate, method]);

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  // Add model to existing custom provider
  // ---------------------------------------------------------------------------

  const handleAddModelToExisting = useCallback(async () => {
    // Support both: selected from discovered list, or manual entry
    const hasSelected = selectedModelIds.size > 0;
    const hasManual = customModelName.trim();
    if (!hasSelected && !hasManual) {
      setError(t('setup.errorNoModelName'));
      return;
    }

    // Shadow semantics: collisions with built-in names are allowed. The
    // resolver picks the user's custom entry first when the name matches.

    setSaving(true);
    setError(null);

    try {
      const prefs = (preferences ?? {}) as Record<string, unknown>;
      const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
      const existingModels = (Array.isArray(otherPref.custom_models) ? otherPref.custom_models : []) as Array<Record<string, unknown>>;

      const newModels: Array<Record<string, unknown>> = [];

      if (hasSelected) {
        for (const id of selectedModelIds) {
          const entry: Record<string, unknown> = { name: id, model_id: id, provider };
          const mods = buildModalitiesArray(modelModalities.get(id) ?? new Set());
          if (mods) entry.input_modalities = mods;
          newModels.push(entry);
        }
      } else {
        const entry: Record<string, unknown> = {
          name: customModelName.trim(),
          model_id: customModelId.trim() || customModelName.trim(),
          provider,
        };
        const mods = buildModalitiesArray(manualModalities);
        if (mods) entry.input_modalities = mods;
        newModels.push(entry);
      }

      // Deduplicate: replace existing entries with same name, append truly new ones
      const newNames = new Set(newModels.map((m) => m.name as string));
      const deduped = existingModels.filter((m) => !newNames.has(m.name as string));

      await updatePreferences.mutateAsync({
        other_preference: {
          custom_models: [...deduped, ...newModels],
        },
      });

      await queryClient.invalidateQueries({ queryKey: queryKeys.models.all });

      navigate('/setup/models', {
        state: { method, provider, displayName, brandKey },
      });
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : err?.message ?? t('setup.errorAddModel'));
    } finally {
      setSaving(false);
    }
  }, [selectedModelIds, customModelName, customModelId, provider, preferences, updatePreferences, queryClient, navigate, method, displayName, brandKey, t, modelModalities, manualModalities, buildModalitiesArray]);

    const toggleModel = (id: string) => {
      setSelectedModelIds((prev) => {
        const next = new Set(prev);
        if (next.has(id)) next.delete(id); else next.add(id);
        return next;
      });
    };

    const hasSelection = selectedModelIds.size > 0 || customModelName.trim();

    return (
      <div className="flex flex-col gap-4 sm:gap-6">
        <div className="flex flex-col gap-1">
          <h2
            className="font-semibold"
            style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
          >
            {t('setup.addModelTo', { provider: displayName })}
          </h2>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {dynamicModels
              ? t('setup.selectModelsDesc', { defaultValue: 'Select models available on your server.' })
              : t('setup.addModelToDesc')}
          </p>
        </div>

        {/* Discovered models list (dynamic providers) */}
        {dynamicModels && (
          <div
            className="rounded-lg p-4 flex flex-col gap-3"
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
            }}
          >
            <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('setup.availableModels', { defaultValue: 'Available models' })}
            </label>
            {loadingModels && (
              <div className="flex items-center gap-2 py-4 justify-center">
                <Loader2 className="h-4 w-4 animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
                <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('setup.fetchingModels', { defaultValue: 'Fetching models...' })}
                </span>
              </div>
            )}
            {modelsError && (
              <div className="flex items-center gap-2 py-2">
                <AlertTriangle className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-warning, #f59e0b)' }} />
                <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
                  {modelsError}
                </span>
              </div>
            )}
            {!loadingModels && !modelsError && discoveredModels.length === 0 && (
              <p className="text-sm py-2" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('setup.noModelsFound', { defaultValue: 'No models found. Make sure your server is running and has models loaded.' })}
              </p>
            )}
            {discoveredModels.length > 0 && (
              <div className="flex flex-col gap-1.5 max-h-[280px] overflow-y-auto">
                {discoveredModels.map((m) => (
                  <div key={m.id} className="flex flex-col">
                    <button
                      type="button"
                      onClick={() => toggleModel(m.id)}
                      className="flex items-center gap-3 rounded-md px-3 py-2 text-left transition-colors"
                      style={{
                        background: selectedModelIds.has(m.id)
                          ? 'var(--color-accent-soft)'
                          : 'transparent',
                        border: selectedModelIds.has(m.id)
                          ? '1px solid var(--color-accent-primary)'
                          : '1px solid var(--color-border-default)',
                      }}
                    >
                      <div
                        className="flex-shrink-0 h-4 w-4 rounded border flex items-center justify-center"
                        style={{
                          borderColor: selectedModelIds.has(m.id)
                            ? 'var(--color-accent-primary)'
                            : 'var(--color-border-default)',
                          background: selectedModelIds.has(m.id)
                            ? 'var(--color-accent-primary)'
                            : 'transparent',
                        }}
                      >
                        {selectedModelIds.has(m.id) && (
                          <Check className="h-3 w-3" style={{ color: '#fff' }} />
                        )}
                      </div>
                      <span className="text-sm font-mono" style={{ color: 'var(--color-text-primary)' }}>
                        {m.id}
                      </span>
                    </button>
                    {selectedModelIds.has(m.id) && (
                      <div className="flex items-center gap-1.5 pl-10 py-1">
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
                          const active = modelModalities.get(m.id)?.has(mod);
                          return (
                            <button
                              key={mod}
                              type="button"
                              onClick={(e) => { e.stopPropagation(); toggleDiscoveredModality(m.id, mod); }}
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
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {/* Manual model entry — fallback or for non-dynamic providers */}
        {(!dynamicModels || (dynamicModels && !loadingModels && discoveredModels.length === 0)) && (
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
            disabled={saving || !hasSelection}
            onClick={handleAddModelToExisting}
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
