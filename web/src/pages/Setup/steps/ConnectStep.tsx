import { useState, useCallback, useRef, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { Loader2, ExternalLink, Shield, Copy, Check, AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { ApiKeyInput, type TestResult } from '@/components/model/ApiKeyInput';
import { useUpdateApiKeys } from '@/hooks/useApiKeys';
import { usePreferences } from '@/hooks/usePreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { queryKeys } from '@/lib/queryKeys';
import { api } from '@/api/client';
import type { AccessType, RegionVariant } from '@/components/model/types';
import {
  initiateCodexDevice,
  pollCodexDevice,
  initiateClaudeOAuth,
  submitClaudeCallback,
} from '@/pages/Dashboard/utils/api';
import { useTranslation } from 'react-i18next';
import { mergeCustomModelsForSlug, type CustomModelEntry } from './mergeCustomModelsForSlug';

// ---------------------------------------------------------------------------
// ConnectStep — Step 3: OAuth redirect or API key input
// ---------------------------------------------------------------------------

interface LocationState {
  method?: AccessType;
  provider?: string;
  displayName?: string;
  brandKey?: string;
  sdk?: string | null;
  defaultBaseUrl?: string | null;
  useResponseApi?: boolean;
  isCustom?: boolean;
  isExistingCustom?: boolean;
  regionVariants?: RegionVariant[] | null;
  defaultRegion?: string | null;
  dynamicModels?: boolean;
}

/** API format options for custom provider setup */
const API_FORMATS = [
  { value: 'openai-responses', labelKey: 'setup.apiFormatOpenaiResponses', parent: 'openai', useResponseApi: true },
  { value: 'openai-completions', labelKey: 'setup.apiFormatOpenaiCompletions', parent: 'openai', useResponseApi: false },
  { value: 'anthropic', labelKey: 'setup.apiFormatAnthropic', parent: 'anthropic', useResponseApi: false },
  { value: 'gemini', labelKey: 'setup.apiFormatGemini', parent: 'gemini', useResponseApi: false },
] as const;

/** Translation key for API format from sdk + use_response_api. */
function getApiFormatKey(sdk?: string | null, useResponseApi?: boolean): string {
  switch (sdk) {
    case 'anthropic':
      return 'setup.apiFormatAnthropic';
    case 'gemini':
      return 'setup.apiFormatGemini';
    case 'openai':
      return useResponseApi ? 'setup.apiFormatOpenaiResponses' : 'setup.apiFormatOpenaiCompletions';
    case 'codex':
      return 'setup.apiFormatCodex';
    case 'deepseek':
    case 'qwq':
      return 'setup.apiFormatCompatible';
    default:
      return sdk ? 'setup.apiFormatGeneric' : 'setup.apiFormatDefault';
  }
}

async function testApiKey(
  provider: string,
  apiKey: string,
  baseUrl?: string,
): Promise<TestResult> {
  try {
    const { data } = await api.post('/api/v1/keys/test', {
      provider,
      api_key: apiKey,
      base_url: baseUrl || undefined,
    });
    return data as TestResult;
  } catch {
    return { success: false, error: 'Test request failed' };
  }
}

// ---------------------------------------------------------------------------
// Process step UI
// ---------------------------------------------------------------------------

function ProcessStep({ number, title, description }: { number: number; title: string; description: string }) {
  return (
    <div className="flex gap-3 items-start">
      <div
        className="flex-shrink-0 h-6 w-6 rounded-full flex items-center justify-center text-xs font-bold"
        style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
      >
        {number}
      </div>
      <div>
        <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{title}</p>
        <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>{description}</p>
      </div>
    </div>
  );
}

function DisclaimerBox({ provider }: { provider: string }) {
  const { t } = useTranslation();
  const isClaude = provider === 'claude-oauth';
  return (
    <div
      className="rounded-lg p-3"
      style={{
        backgroundColor: 'var(--color-bg-sunken, var(--color-bg-card))',
        border: '1px solid var(--color-border-muted)',
      }}
    >
      <div className="flex gap-2 items-start">
        <Shield className="h-4 w-4 flex-shrink-0 mt-0.5" style={{ color: 'var(--color-text-tertiary)' }} />
        <div>
          <p className="text-xs font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
            {t('setup.securityPrivacy')}
          </p>
          <p className="text-[11px] leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('setup.tokensEncrypted')}
          </p>
          <p className="text-[11px] leading-relaxed mt-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
            {isClaude
              ? t('setup.usageCountsClaude')
              : t('setup.usageCountsOpenai')}
          </p>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Copy button
// ---------------------------------------------------------------------------

function CopyButton({ text }: { text: string }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback
    }
  }, [text]);

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium transition-colors"
      style={{
        background: copied ? 'var(--color-success)' : 'var(--color-bg-surface)',
        color: copied ? '#fff' : 'var(--color-text-secondary)',
        border: copied ? 'none' : '1px solid var(--color-border-default)',
      }}
    >
      {copied ? (
        <>
          <Check className="h-3 w-3" />
          {t('setup.copied')}
        </>
      ) : (
        <>
          <Copy className="h-3 w-3" />
          {t('setup.copy')}
        </>
      )}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function ConnectStep() {
  const navigate = useNavigate();
  const location = useLocation();
  const queryClient = useQueryClient();
  const updateApiKeys = useUpdateApiKeys();
  const { preferences } = usePreferences();
  const updatePreferences = useUpdatePreferences();
  const { t } = useTranslation();

  const state = (location.state as LocationState | null) ?? {};

  // Redirect to method step if essential state is missing (e.g. browser refresh)
  // Custom provider flows don't have a provider yet — skip the guard for those.
  useEffect(() => {
    if (!state.provider && !state.isCustom && !state.isExistingCustom) {
      navigate('/setup/method', { replace: true });
    }
  }, [state.provider, state.isCustom, state.isExistingCustom, navigate]);

  const method = state.method ?? 'api_key';
  const isCustom = state.isCustom ?? false;
  const isExistingCustom = state.isExistingCustom ?? false;
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

  // Custom provider state
  const [customName, setCustomName] = useState('');
  const [customFormat, setCustomFormat] = useState<string>('openai-completions');
  const [customBaseUrl, setCustomBaseUrl] = useState('');
  const [customApiKey, setCustomApiKey] = useState('');
  const [customModelName, setCustomModelName] = useState('');
  const [customModelId, setCustomModelId] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Input modality state — per-model for discovered, single for manual entry
  const [modelModalities, setModelModalities] = useState<Map<string, Set<string>>>(new Map());
  const [manualModalities, setManualModalities] = useState<Set<string>>(new Set());

  const toggleDiscoveredModality = (modelId: string, modality: string) => {
    setModelModalities(prev => {
      const next = new Map(prev);
      const current = next.get(modelId) ?? new Set<string>();
      const updated = new Set(current);
      if (updated.has(modality)) updated.delete(modality);
      else updated.add(modality);
      next.set(modelId, updated);
      return next;
    });
  };

  const toggleManualModality = (modality: string) => {
    setManualModalities(prev => {
      const next = new Set(prev);
      if (next.has(modality)) next.delete(modality);
      else next.add(modality);
      return next;
    });
  };

  const buildModalitiesArray = useCallback((modSet: Set<string>): string[] | undefined => {
    if (modSet.size === 0) return undefined;
    const arr = ['text', ...Array.from(modSet).filter(m => m !== 'text')];
    return arr;
  }, []);

  // Dynamic model discovery state (for local providers)
  const [discoveredModels, setDiscoveredModels] = useState<Array<{ id: string; name: string }>>([]);
  const [selectedModelIds, setSelectedModelIds] = useState<Set<string>>(new Set());
  const [loadingModels, setLoadingModels] = useState(false);
  const [modelsError, setModelsError] = useState<string | null>(null);

  // Parent-model import state (for custom providers) — fetched from
  // /api/v1/providers/{parent}/visible-models once the user picks an API format.
  interface ParentModel {
    name: string;
    model_id: string;
    display_name: string;
    input_modalities: string[];
  }
  const [parentModels, setParentModels] = useState<ParentModel[]>([]);
  const [parentModelsLoading, setParentModelsLoading] = useState(false);
  const [selectedParentModelNames, setSelectedParentModelNames] = useState<Set<string>>(new Set());

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

  // OAuth shared state
  const [oauthPhase, setOauthPhase] = useState<'disclaimer' | 'connecting' | 'active'>('disclaimer');
  const [oauthError, setOauthError] = useState<string | null>(null);
  const [agreed, setAgreed] = useState(false);

  // Codex device flow state
  const [codexUserCode, setCodexUserCode] = useState<string | null>(null);
  const [codexVerifyUrl, setCodexVerifyUrl] = useState<string | null>(null);
  const [codexPolling, setCodexPolling] = useState(false);

  // Claude OAuth state
  const [claudeAuthorizeUrl, setClaudeAuthorizeUrl] = useState<string | null>(null);
  const [claudeCallbackInput, setClaudeCallbackInput] = useState('');
  const [claudeSubmitting, setClaudeSubmitting] = useState(false);

  const isCodex = provider === 'codex-oauth';
  const isClaude = provider === 'claude-oauth';

  // Abort polling on unmount
  const cancelledRef = useRef(false);
  useEffect(() => {
    return () => { cancelledRef.current = true; };
  }, []);

  // ---------------------------------------------------------------------------
  // Codex OAuth handler
  // ---------------------------------------------------------------------------

  const handleCodexStart = useCallback(async () => {
    setOauthPhase('connecting');
    setOauthError(null);
    try {
      const result = await initiateCodexDevice();
      const userCode = result.user_code as string;
      const verifyUrl = result.verification_url as string;
      const interval = (result.interval as number) || 5;

      setCodexUserCode(userCode);
      setCodexVerifyUrl(verifyUrl);
      setOauthPhase('active');

      // Open verification URL in new tab
      window.open(verifyUrl, '_blank', 'noopener');

      // Start polling
      cancelledRef.current = false;
      setCodexPolling(true);
      const maxAttempts = 60;
      for (let i = 0; i < maxAttempts; i++) {
        await new Promise((r) => setTimeout(r, interval * 1000));
        if (cancelledRef.current) return;
        try {
          const pollResult = await pollCodexDevice();
          if ('success' in pollResult && pollResult.success) {
            await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
            await queryClient.invalidateQueries({ queryKey: queryKeys.oauth.codex() });
            navigate('/setup/models', {
              state: { method, provider, displayName, brandKey },
            });
            return;
          }
        } catch {
          // Continue polling
        }
      }
      setOauthError(t('setup.errorAuthTimeout'));
      setCodexPolling(false);
    } catch {
      setOauthError(t('setup.errorDeviceAuth'));
      setOauthPhase('disclaimer');
    }
  }, [queryClient, navigate, method, provider, displayName, brandKey, t]);

  // ---------------------------------------------------------------------------
  // Claude OAuth handlers
  // ---------------------------------------------------------------------------

  const handleClaudeStart = useCallback(async () => {
    setOauthPhase('connecting');
    setOauthError(null);
    try {
      const result = await initiateClaudeOAuth();
      const authorizeUrl = result.authorize_url as string;
      setClaudeAuthorizeUrl(authorizeUrl);
      setOauthPhase('active');
      // Open in new tab so user can paste code back here
      window.open(authorizeUrl, '_blank', 'noopener');
    } catch {
      setOauthError(t('setup.errorClaudeOAuth'));
      setOauthPhase('disclaimer');
    }
  }, [t]);

  const handleClaudeSubmit = useCallback(async () => {
    if (!claudeCallbackInput.trim()) return;
    setClaudeSubmitting(true);
    setOauthError(null);
    try {
      const result = await submitClaudeCallback(claudeCallbackInput.trim());
      if (result.success) {
        await queryClient.invalidateQueries({ queryKey: queryKeys.user.me() });
        await queryClient.invalidateQueries({ queryKey: queryKeys.oauth.claude() });
        navigate('/setup/models', {
          state: { method, provider, displayName, brandKey },
        });
      } else {
        setOauthError((result as Record<string, unknown>).error as string ?? 'Authorization failed. Please try again.');
      }
    } catch {
      setOauthError(t('setup.errorInvalidCode'));
    } finally {
      setClaudeSubmitting(false);
    }
  }, [claudeCallbackInput, queryClient, navigate, method, provider, displayName, brandKey, t]);

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

  // ---------------------------------------------------------------------------
  // Render — Add model to existing custom provider
  // ---------------------------------------------------------------------------

  if (isExistingCustom) {
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

  // ---------------------------------------------------------------------------
  // Render — Custom provider flow
  // ---------------------------------------------------------------------------

  if (isCustom) {
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

  // ---------------------------------------------------------------------------
  // Render — OAuth flow
  // ---------------------------------------------------------------------------

  if (method === 'oauth') {
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
            {isCodex
              ? t('setup.oauthCodexDesc')
              : t('setup.oauthClaudeDesc')}
          </p>
        </div>

        {/* Phase 1: Disclaimer + process flow */}
        {oauthPhase === 'disclaimer' && (
          <>
            {/* Process flow */}
            <div
              className="rounded-lg p-4"
              style={{
                background: 'var(--color-bg-surface)',
                border: '1px solid var(--color-border-default)',
              }}
            >
              <p
                className="text-xs font-medium uppercase tracking-wide mb-3"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('setup.howItWorks')}
              </p>
              <div className="space-y-3">
                {isCodex ? (
                  <>
                    <ProcessStep
                      number={1}
                      title={t('setup.codexStep1Title')}
                      description={t('setup.codexStep1Desc')}
                    />
                    <ProcessStep
                      number={2}
                      title={t('setup.codexStep2Title')}
                      description={t('setup.codexStep2Desc')}
                    />
                    <ProcessStep
                      number={3}
                      title={t('setup.codexStep3Title')}
                      description={t('setup.codexStep3Desc')}
                    />
                  </>
                ) : (
                  <>
                    <ProcessStep
                      number={1}
                      title={t('setup.claudeStep1Title')}
                      description={t('setup.claudeStep1Desc')}
                    />
                    <ProcessStep
                      number={2}
                      title={t('setup.claudeStep2Title')}
                      description={t('setup.claudeStep2Desc')}
                    />
                    <ProcessStep
                      number={3}
                      title={t('setup.claudeStep3Title')}
                      description={t('setup.claudeStep3Desc')}
                    />
                  </>
                )}
              </div>
            </div>

            {/* Disclaimer */}
            <DisclaimerBox provider={provider} />

            {/* Agreement checkbox */}
            <label className="flex items-start gap-3 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={agreed}
                onChange={(e) => setAgreed(e.target.checked)}
                className="mt-0.5 h-4 w-4 rounded border accent-[var(--color-accent-primary)]"
              />
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t(isClaude ? 'setup.oauthAgreeAnthropic' : 'setup.oauthAgreeOpenai')}
              </span>
            </label>

            {/* Action button */}
            <Button
              variant="default"
              disabled={!agreed}
              onClick={isCodex ? handleCodexStart : handleClaudeStart}
              className="w-full h-11"
            >
              <ExternalLink className="h-4 w-4 mr-1.5" />
              {isCodex ? t('setup.openVerification') : t('setup.openClaude')}
            </Button>
          </>
        )}

        {/* Phase 2: Connecting spinner */}
        {oauthPhase === 'connecting' && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
            <span className="ml-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              {t('setup.connecting')}
            </span>
          </div>
        )}

        {/* Phase 3: Active — Codex device code display */}
        {oauthPhase === 'active' && isCodex && codexUserCode && (
          <div
            className="flex flex-col items-center gap-4 rounded-lg p-6"
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
            }}
          >
            <div className="flex items-center gap-2">
              <AlertTriangle className="h-4 w-4" style={{ color: 'var(--color-warning, #f59e0b)' }} />
              <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('setup.enterCodeOnOpenai')}
              </p>
            </div>

            <div className="flex items-center gap-3">
              <code
                className="text-2xl font-mono font-bold tracking-widest px-4 py-2 rounded"
                style={{
                  background: 'var(--color-bg-page)',
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-default)',
                }}
              >
                {codexUserCode}
              </code>
              <CopyButton text={codexUserCode} />
            </div>

            {codexPolling && (
              <p
                className="text-xs flex items-center gap-1.5"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                <Loader2 className="h-3 w-3 animate-spin" />
                {t('setup.waitingApproval')}
              </p>
            )}

            <a
              href={codexVerifyUrl ?? '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-sm font-medium"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              {t('setup.openVerificationAgain')}
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          </div>
        )}

        {/* Phase 3: Active — Claude paste-back input */}
        {oauthPhase === 'active' && isClaude && (
          <div
            className="flex flex-col gap-4 rounded-lg p-5"
            style={{
              background: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-default)',
            }}
          >
            <div className="flex flex-col gap-1">
              <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {t('setup.pasteAuthCode')}
              </p>
              <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('setup.pasteAuthCodeHint')}
              </p>
            </div>

            <div className="flex gap-2">
              <Input
                value={claudeCallbackInput}
                onChange={(e) => {
                  setClaudeCallbackInput(e.target.value);
                  setOauthError(null);
                }}
                placeholder={t('setup.pasteCodePlaceholder')}
                className="flex-1 font-mono text-sm"
                autoComplete="off"
                spellCheck={false}
              />
              <Button
                variant="default"
                disabled={claudeSubmitting || !claudeCallbackInput.trim()}
                onClick={handleClaudeSubmit}
                className="shrink-0"
              >
                {claudeSubmitting ? (
                  <>
                    <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
                    {t('setup.submitting')}
                  </>
                ) : (
                  t('setup.submit')
                )}
              </Button>
            </div>

            <a
              href={claudeAuthorizeUrl ?? '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-xs self-start"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              {t('setup.openClaudeAgain')}
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
        )}

        {/* Error */}
        {oauthError && (
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {oauthError}
          </p>
        )}

        {/* Back button */}
        <div className="flex items-center justify-between pt-2">
          <Button variant="outline" onClick={handleBack}>
            {t('setup.back')}
          </Button>
        </div>
      </div>
    );
  }

  // ---------------------------------------------------------------------------
  // Render — API key / Coding plan flow
  // ---------------------------------------------------------------------------

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
