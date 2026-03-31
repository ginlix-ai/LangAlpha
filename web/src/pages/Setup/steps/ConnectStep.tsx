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
  { value: 'openai-responses', label: 'OpenAI Responses API', parent: 'openai', useResponseApi: true },
  { value: 'openai-completions', label: 'OpenAI Chat Completions API', parent: 'openai', useResponseApi: false },
  { value: 'anthropic', label: 'Anthropic Messages API', parent: 'anthropic', useResponseApi: false },
  { value: 'gemini', label: 'Google Gemini API', parent: 'gemini', useResponseApi: false },
] as const;

/** Human-readable API format label from sdk + use_response_api. */
function getApiFormatLabel(sdk?: string | null, useResponseApi?: boolean): string {
  switch (sdk) {
    case 'anthropic':
      return 'Anthropic Messages API';
    case 'gemini':
      return 'Google Gemini API';
    case 'openai':
      return useResponseApi ? 'OpenAI Responses API' : 'OpenAI Chat Completions API';
    case 'codex':
      return 'OpenAI Codex API';
    case 'deepseek':
    case 'qwq':
      return 'OpenAI-compatible API';
    default:
      return sdk ? `${sdk} API` : 'API';
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
            Security &amp; Privacy
          </p>
          <p className="text-[11px] leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
            Your tokens are encrypted at rest. We use them only to make API calls on your behalf.
          </p>
          <p className="text-[11px] leading-relaxed mt-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
            {isClaude
              ? 'Usage will count against your Anthropic subscription. You can disconnect at any time.'
              : 'Usage will count against your OpenAI subscription. You can disconnect at any time.'}
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
          Copied
        </>
      ) : (
        <>
          <Copy className="h-3 w-3" />
          Copy
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
  const apiFormatLabel = getApiFormatLabel(sdk, useResponseApi);

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
  const effectiveApiFormatLabel = activeVariant
    ? getApiFormatLabel(effectiveSdk, effectiveUseResponseApi)
    : apiFormatLabel;

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
      setOauthError('Authorization timed out. Please try again.');
      setCodexPolling(false);
    } catch {
      setOauthError('Failed to start device authorization. Please try again.');
      setOauthPhase('disclaimer');
    }
  }, [queryClient, navigate, method, provider, displayName, brandKey]);

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
      setOauthError('Failed to initiate Claude OAuth. Please try again.');
      setOauthPhase('disclaimer');
    }
  }, []);

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
      setOauthError('Invalid authorization code. Please try again.');
    } finally {
      setClaudeSubmitting(false);
    }
  }, [claudeCallbackInput, queryClient, navigate, method, provider, displayName, brandKey]);

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
      setError('Please enter an API key.');
      return;
    }
    if (dynamicModels && !baseUrl.trim()) {
      setError('Please enter the base URL for your local server.');
      return;
    }

    setSaving(true);
    setError(null);

    try {
      const saveProvider = effectiveProvider || provider;
      const payload: Record<string, unknown> = {
        byok_enabled: true,
        api_keys: { [saveProvider]: apiKey || null },
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
        // Dynamic providers (LM Studio, vLLM, Ollama) — go to custom model add
        navigate('/setup/connect', {
          state: {
            method,
            provider: saveProvider,
            displayName,
            brandKey,
            isExistingCustom: true,
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
      setError(typeof detail === 'string' ? detail : err?.message ?? 'Failed to save API key.');
    } finally {
      setSaving(false);
    }
  }, [apiKey, baseUrl, dynamicModels, effectiveProvider, provider, updateApiKeys, queryClient, navigate, method, displayName, brandKey]);

  const handleBack = useCallback(() => {
    navigate('/setup/provider', { state: { method } });
  }, [navigate, method]);

  // ---------------------------------------------------------------------------
  // Custom provider save
  // ---------------------------------------------------------------------------

  const handleCustomSave = useCallback(async () => {
    const slug = customName.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '-');
    if (!slug || !customBaseUrl.trim() || !customApiKey.trim() || !customModelName.trim()) {
      setError('Please fill in all fields.');
      return;
    }

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

      const newModel = {
        name: customModelName.trim(),
        model_id: customModelId.trim() || customModelName.trim(),
        provider: slug,
      };

      // Only send custom_providers and custom_models — backend merges into existing JSONB
      await updatePreferences.mutateAsync({
        other_preference: {
          custom_providers: [...existingProviders.filter((p) => p.name !== slug), newProvider],
          custom_models: [...existingModels.filter((m) => m.provider !== slug), newModel],
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
      setError(msg || err?.message || 'Failed to save custom provider.');
    } finally {
      setSaving(false);
    }
  }, [customName, customFormat, customBaseUrl, customApiKey, customModelName, customModelId, preferences, updatePreferences, updateApiKeys, queryClient, navigate, method]);

  // ---------------------------------------------------------------------------
  // ---------------------------------------------------------------------------
  // Add model to existing custom provider
  // ---------------------------------------------------------------------------

  const handleAddModelToExisting = useCallback(async () => {
    if (!customModelName.trim()) {
      setError('Please enter a model name.');
      return;
    }

    setSaving(true);
    setError(null);

    try {
      const prefs = (preferences ?? {}) as Record<string, unknown>;
      const otherPref = (prefs.other_preference ?? {}) as Record<string, unknown>;
      const existingModels = (Array.isArray(otherPref.custom_models) ? otherPref.custom_models : []) as Array<Record<string, unknown>>;

      const newModel = {
        name: customModelName.trim(),
        model_id: customModelId.trim() || customModelName.trim(),
        provider,
      };

      await updatePreferences.mutateAsync({
        other_preference: {
          custom_models: [...existingModels, newModel],
        },
      });

      await queryClient.invalidateQueries({ queryKey: queryKeys.models.all });

      navigate('/setup/models', {
        state: { method, provider, displayName, brandKey },
      });
    } catch (e: unknown) {
      const err = e as { response?: { data?: { detail?: string } }; message?: string };
      const detail = err?.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : err?.message ?? 'Failed to add model.');
    } finally {
      setSaving(false);
    }
  }, [customModelName, customModelId, provider, preferences, updatePreferences, queryClient, navigate, method, displayName, brandKey]);

  // ---------------------------------------------------------------------------
  // Render — Add model to existing custom provider
  // ---------------------------------------------------------------------------

  if (isExistingCustom) {
    return (
      <div className="flex flex-col gap-4 sm:gap-6">
        <div className="flex flex-col gap-1">
          <h2
            className="font-semibold"
            style={{ fontSize: '1.125rem', color: 'var(--color-text-primary)' }}
          >
            Add model to {displayName}
          </h2>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            Add another model to your existing custom provider.
          </p>
        </div>

        <div
          className="rounded-lg p-4 flex flex-col gap-3"
          style={{
            background: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-default)',
          }}
        >
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            Model
          </label>
          <Input
            value={customModelName}
            onChange={(e) => {
              setCustomModelName(e.target.value);
              if (!customModelId) setCustomModelId(e.target.value);
            }}
            placeholder="Display name (e.g. Llama 3.3 70B)"
            autoComplete="off"
          />
          <Input
            value={customModelId}
            onChange={(e) => setCustomModelId(e.target.value)}
            placeholder="Model ID (e.g. meta-llama/Llama-3.3-70B)"
            className="font-mono text-xs"
            autoComplete="off"
          />
          <p className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
            Model ID is sent to the API. Leave same as name if unsure.
          </p>
        </div>

        {error && (
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {error}
          </p>
        )}

        <div className="flex items-center justify-between pt-2">
          <Button variant="outline" onClick={handleBack}>
            Back
          </Button>
          <Button
            variant="default"
            disabled={saving || !customModelName.trim()}
            onClick={handleAddModelToExisting}
            className="min-w-[120px]"
          >
            {saving ? (
              <>
                <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
                Saving...
              </>
            ) : (
              'Continue'
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
            Add custom provider
          </h2>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            Connect any OpenAI, Anthropic, or Gemini compatible endpoint.
          </p>
        </div>

        {/* Provider name */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            Provider name
          </label>
          <Input
            value={customName}
            onChange={(e) => setCustomName(e.target.value)}
            placeholder="e.g. My vLLM Server"
            autoComplete="off"
          />
        </div>

        {/* API format */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            API format
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
                {fmt.label}
              </button>
            ))}
          </div>
        </div>

        {/* Base URL */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            Base URL
          </label>
          <Input
            type="url"
            value={customBaseUrl}
            onChange={(e) => setCustomBaseUrl(e.target.value)}
            placeholder="https://your-endpoint.com/v1"
            className="font-mono text-xs"
          />
        </div>

        {/* API key */}
        <div className="flex flex-col gap-1.5">
          <label className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            API key
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
            Model
          </label>
          <div className="flex flex-col gap-2">
            <Input
              value={customModelName}
              onChange={(e) => {
                setCustomModelName(e.target.value);
                if (!customModelId) setCustomModelId(e.target.value);
              }}
              placeholder="Display name (e.g. Llama 3.3 70B)"
              autoComplete="off"
            />
            <Input
              value={customModelId}
              onChange={(e) => setCustomModelId(e.target.value)}
              placeholder="Model ID (e.g. meta-llama/Llama-3.3-70B)"
              className="font-mono text-xs"
              autoComplete="off"
            />
          </div>
          <p className="text-[11px]" style={{ color: 'var(--color-text-tertiary)' }}>
            Model ID is sent to the API. Leave same as name if unsure.
          </p>
        </div>

        {error && (
          <p className="text-sm" style={{ color: 'var(--color-loss)' }}>
            {error}
          </p>
        )}

        <div className="flex items-center justify-between pt-2">
          <Button variant="outline" onClick={handleBack}>
            Back
          </Button>
          <Button
            variant="default"
            disabled={saving || !customName.trim() || !customBaseUrl.trim() || !customApiKey.trim() || !customModelName.trim()}
            onClick={handleCustomSave}
            className="min-w-[120px]"
          >
            {saving ? (
              <>
                <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
                Saving...
              </>
            ) : (
              'Continue'
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
            Connect {displayName}
          </h2>
          <p
            className="text-sm"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {isCodex
              ? 'Authorize with your ChatGPT account using a device code.'
              : 'Connect your Claude subscription via OAuth.'}
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
                How it works
              </p>
              <div className="space-y-3">
                {isCodex ? (
                  <>
                    <ProcessStep
                      number={1}
                      title="Open OpenAI verification page"
                      description="A new tab will open to OpenAI's device authorization page."
                    />
                    <ProcessStep
                      number={2}
                      title="Enter the device code"
                      description="We'll generate a code for you. Copy it and paste it on OpenAI's page, then approve access."
                    />
                    <ProcessStep
                      number={3}
                      title="Return here"
                      description="Once approved, we'll detect it automatically and complete the connection."
                    />
                  </>
                ) : (
                  <>
                    <ProcessStep
                      number={1}
                      title="Authorize on claude.ai"
                      description="A new tab will open to claude.ai where you sign in and authorize access."
                    />
                    <ProcessStep
                      number={2}
                      title="Copy the authorization code"
                      description="After approval, you'll see a code on the page. Copy the entire value."
                    />
                    <ProcessStep
                      number={3}
                      title="Paste it back here"
                      description="Return to this tab and paste the code into the input field to complete the connection."
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
                I understand that usage will count against my {isClaude ? 'Anthropic' : 'OpenAI'} subscription
                and that my tokens will be stored encrypted on this platform.
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
              {isCodex ? 'Open OpenAI verification page' : 'Open claude.ai'}
            </Button>
          </>
        )}

        {/* Phase 2: Connecting spinner */}
        {oauthPhase === 'connecting' && (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="h-6 w-6 animate-spin" style={{ color: 'var(--color-accent-primary)' }} />
            <span className="ml-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
              Connecting...
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
                Enter this code on the OpenAI page
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
                Waiting for approval on OpenAI...
              </p>
            )}

            <a
              href={codexVerifyUrl ?? '#'}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-sm font-medium"
              style={{ color: 'var(--color-accent-primary)' }}
            >
              Open verification page again
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
                Paste the authorization code from claude.ai
              </p>
              <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                After authorizing, you&apos;ll see a code on the Claude page. Copy and paste it below.
              </p>
            </div>

            <div className="flex gap-2">
              <Input
                value={claudeCallbackInput}
                onChange={(e) => {
                  setClaudeCallbackInput(e.target.value);
                  setOauthError(null);
                }}
                placeholder="Paste code here (e.g. abc123#state456)"
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
                    Submitting...
                  </>
                ) : (
                  'Submit'
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
              Open claude.ai again
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
            Back
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
          Connect {displayName}
        </h2>
        <p
          className="text-sm"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {dynamicModels
            ? 'Configure your local server URL. API key is optional.'
            : method === 'coding_plan'
              ? 'Enter the API key from your coding plan subscription.'
              : 'Enter your API key. You can find this in your provider dashboard.'}
        </p>
      </div>

      {/* Region toggle — shown when provider has region variants */}
      {regionVariants && regionVariants.length > 0 && (
        <div className="flex items-center gap-2">
          <span
            className="text-xs font-medium"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            Region
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
              {(defaultRegion === 'cn' ? 'China' : defaultRegion === 'sg' ? 'Singapore' : 'International')}
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
                {rv.region === 'cn' ? 'China' : rv.region === 'sg' ? 'Singapore' : rv.region === 'intl' ? 'International' : rv.region}
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
            API format
          </span>
          <span
            className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium"
            style={{
              background: 'var(--color-accent-soft)',
              color: 'var(--color-accent-primary)',
            }}
          >
            {effectiveApiFormatLabel}
          </span>
        </div>

        {/* Base URL — always shown, always editable */}
        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <label
              className="text-xs font-medium"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              Base URL
            </label>
            {baseUrl !== effectiveBaseUrl && effectiveBaseUrl && (
              <button
                type="button"
                onClick={() => setBaseUrl(effectiveBaseUrl)}
                className="text-[11px]"
                style={{ color: 'var(--color-accent-primary)' }}
              >
                Reset to default
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
              Custom URL. Default: {effectiveBaseUrl}
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
          {displayName} API Key
          {dynamicModels && (
            <span className="text-xs font-normal ml-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
              (optional)
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
          Back
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
              Saving...
            </>
          ) : (
            'Continue'
          )}
        </Button>
      </div>
    </div>
  );
}
