/* eslint-disable react-refresh/only-export-components -- mixed helper/component module shared by the ConnectStep branch components */
import { useState, useCallback } from 'react';
import { Shield, Copy, Check } from 'lucide-react';
import { type TestResult } from '@/components/model/ApiKeyInput';
import { api } from '@/api/client';
import type { AccessType, RegionVariant } from '@/components/model/types';
import { useTranslation } from 'react-i18next';

export interface LocationState {
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
export const API_FORMATS = [
  { value: 'openai-responses', labelKey: 'setup.apiFormatOpenaiResponses', parent: 'openai', useResponseApi: true },
  { value: 'openai-completions', labelKey: 'setup.apiFormatOpenaiCompletions', parent: 'openai', useResponseApi: false },
  { value: 'anthropic', labelKey: 'setup.apiFormatAnthropic', parent: 'anthropic', useResponseApi: false },
  { value: 'gemini', labelKey: 'setup.apiFormatGemini', parent: 'gemini', useResponseApi: false },
] as const;

/** Shape returned by `/api/v1/providers/{parent}/visible-models` — the wizard
 *  renders these as checkboxes so the user can import a subset into their
 *  variant. */
export interface ParentModel {
  name: string;
  model_id: string;
  display_name: string;
  input_modalities: string[];
}

/** Translation key for API format from sdk + use_response_api. */
export function getApiFormatKey(sdk?: string | null, useResponseApi?: boolean): string {
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

export async function testApiKey(
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

export function ProcessStep({ number, title, description }: { number: number; title: string; description: string }) {
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

export function DisclaimerBox({ provider }: { provider: string }) {
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

export function CopyButton({ text }: { text: string }) {
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

/** Input modality selection shared by the custom-provider and
 *  existing-custom flows — per-model for discovered models, a single
 *  set for manual entry. */
export function useModalityState() {
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

  return { modelModalities, manualModalities, toggleDiscoveredModality, toggleManualModality, buildModalitiesArray };
}
