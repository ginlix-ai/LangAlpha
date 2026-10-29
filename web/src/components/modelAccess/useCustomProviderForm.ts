import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useUpdateApiKeys } from '@/hooks/useApiKeys';
import { usePreferences } from '@/hooks/usePreferences';
import { useReadStoredPreferences } from '@/hooks/useStoredPreferences';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useEndpointModels } from '@/hooks/useProviderModels';
import { useRefreshAfterAccessChange } from '@/hooks/useModelAccessActions';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { modelPrefs, splitPreferenceWrite, type CustomProviderEntry } from '@/lib/modelPreferences';
import { mergeProviderModels, replaceProviderModels } from '@/lib/customModels';
import { apiErrorDetail, apiErrorStatus, apiValidationErrors } from '@/api/errors';
import type { CustomModelEntry } from '@/components/model/types';
import { API_FORMATS } from './apiFormat';
import { useCustomModelDraft } from './CustomModelFields';
import { useEndpointVerdict } from './endpointTrust';
import { slugifyProviderName } from './slugifyModelName';

/** Why the endpoint's list is not shown, as one line under the key. */
const UNAVAILABLE_NOTE: Record<string, string> = {
  unauthorized: 'setup.endpointModels.unauthorized',
  unreachable: 'setup.endpointModels.unreachable',
};

const ENTERED_URL = /^https?:\/\/\S+$/i;

/**
 * A provider of the user's own: its name, format, endpoint and key, the
 * models the endpoint lists that the manifest has tuned (offered picked), and
 * one model described by hand where it lists none of those. `save` stores the
 * provider and its models, then the key; it resolves to the provider's name,
 * or null when refused.
 */
export function useCustomProviderForm() {
  const { t } = useTranslation();
  const { preferences } = usePreferences();
  const readStored = useReadStoredPreferences();
  const updatePreferences = useUpdatePreferences();
  const { connectionOf } = useProviderCatalog();
  const updateApiKeys = useUpdateApiKeys();
  const refresh = useRefreshAfterAccessChange();

  const [name, setName] = useState('');
  const [apiFormat, setApiFormat] = useState<string>('openai-completions');
  const [baseUrl, setBaseUrl] = useState('');
  const [apiKey, setApiKey] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Unchecked rather than checked, so a model the next list adds starts picked.
  const [unpicked, setUnpicked] = useState<ReadonlySet<string>>(new Set());
  const [manualOpen, setManualOpen] = useState(false);
  const manual = useCustomModelDraft();

  const slug = slugifyProviderName(name.trim());

  // Only the models the manifest has tuned are offered from the list: one it
  // does not know needs its window described, which the manual form asks for.
  const endpoint = useEndpointModels(apiFormat, baseUrl, apiKey);
  const entered = ENTERED_URL.test(baseUrl.trim()) && !!apiKey.trim();
  const checking = entered && (!endpoint.settled || endpoint.isFetching);
  const listing = entered && endpoint.settled && !endpoint.isFetching ? endpoint.data : undefined;
  const recognized = listing?.recognized ?? [];
  const others = listing?.others ?? [];

  // A name another provider's model already holds would be refused as a duplicate.
  const takenBy = useMemo(() => {
    const out = new Map<string, string>();
    for (const cm of modelPrefs(preferences).custom_models ?? []) {
      if (cm.provider !== slug) out.set(cm.name, cm.provider);
    }
    return out;
  }, [preferences, slug]);
  const picked = recognized.filter((m) => !unpicked.has(m.name) && !takenBy.has(m.name));
  const showManual = manualOpen || (!!listing && recognized.length === 0) || (!checking && !listing && entered);

  const toggle = (model: string) => {
    setUnpicked((prev) => {
      const next = new Set(prev);
      if (next.has(model)) next.delete(model); else next.add(model);
      return next;
    });
  };

  // Warned once there is an address that is neither a provider's own API nor a cloud we recognize.
  const knownEndpoint = useEndpointVerdict(baseUrl);
  // A base URL the server refuses to call is a 400 saying why.
  const refused = apiErrorStatus(endpoint.error) === 400 ? apiErrorDetail(endpoint.error) : null;
  const unavailable = endpoint.isError
    ? typeof refused === 'string' ? refused : t('setup.endpointModels.noList')
    : listing?.status === 'unavailable'
      ? t(UNAVAILABLE_NOTE[listing.reason ?? ''] ?? 'setup.endpointModels.noList')
      : null;

  const manualEntry = showManual ? manual.toEntry(slug) : null;
  const canSave = !!slug && !!baseUrl.trim() && !!apiKey.trim()
    && (picked.length > 0 || !!manualEntry)
    && (!showManual || !manual.started || manual.ready);

  const save = async (): Promise<string | null> => {
    if (!canSave) {
      setError(t('setup.errorFillFields'));
      return null;
    }
    setSaving(true);
    setError(null);

    const format = API_FORMATS.find((f) => f.value === apiFormat);
    const provider: CustomProviderEntry = { name: slug, parent_provider: format?.parent ?? 'openai' };
    if (format?.useResponseApi) provider.use_response_api = true;
    // A tuned model keeps its manifest name, so it inherits what the manifest
    // declares and only the route changes; the window and inputs are set only
    // where this endpoint serves less of them.
    const batch: CustomModelEntry[] = picked
      .filter((m) => m.name !== manualEntry?.name)
      .map((m) => ({
        name: m.name,
        model_id: m.model_id,
        provider: slug,
        ...(m.context ? { context: m.context } : {}),
        ...(m.input_modalities ? { input_modalities: m.input_modalities } : {}),
        ...(m.reasoning ? { reasoning: m.reasoning } : {}),
      }));
    if (manualEntry) batch.push(manualEntry);
    let bag = modelPrefs(preferences);
    let unsaved = false;

    try {
      bag = modelPrefs(await readStored());
      await updatePreferences.mutateAsync(splitPreferenceWrite({
        custom_providers: [...(bag.custom_providers ?? []).filter((p) => p.name !== slug), provider],
        // Models added since under a name re-used here survive this pass.
        custom_models: mergeProviderModels(bag.custom_models ?? [], slug, batch, connectionOf),
      }));
      unsaved = true;
      // After the preferences: the server takes a key only for a provider it knows.
      await updateApiKeys.mutateAsync({
        byok_enabled: true,
        api_keys: { [slug]: apiKey.trim() },
        base_urls: { [slug]: baseUrl.trim() },
      });
      unsaved = false;
      await refresh(slug);
    } catch (e) {
      // A refused key leaves no definition behind: its models would be listed
      // with nothing to run them on. Only this provider goes back, onto what
      // is stored now, so a change another tab saved in between stays.
      if (unsaved) {
        await readStored().then((stored) => {
          const now = modelPrefs(stored);
          const providers = [
            ...(now.custom_providers ?? []).filter((p) => p.name !== slug),
            ...(bag.custom_providers ?? []).filter((p) => p.name === slug),
          ];
          const models = replaceProviderModels(
            now.custom_models ?? [], slug, (bag.custom_models ?? []).filter((m) => m.provider === slug), connectionOf,
          );
          return updatePreferences.mutateAsync(splitPreferenceWrite({
            custom_providers: providers.length > 0 ? providers : null,
            custom_models: models.length > 0 ? models : null,
          }));
        }).catch(() => undefined);
      }
      const detail = apiErrorDetail(e);
      const invalid = apiValidationErrors(e).map((v) => v.message).join('; ');
      setError((typeof detail === 'string' && detail) || invalid || (e instanceof Error && e.message) || t('setup.errorSaveProvider'));
      setSaving(false);
      return null;
    }
    // Kept busy on success: the step is left next, so the button can't send twice.
    return slug;
  };

  // A refusal is about what was sent, so it goes once the user changes it.
  const edited = (set: (value: string) => void) => (value: string) => {
    setError(null);
    set(value);
  };

  return {
    name,
    setName: edited(setName),
    apiFormat,
    setApiFormat: edited(setApiFormat),
    baseUrl,
    setBaseUrl: edited(setBaseUrl),
    apiKey,
    setApiKey: edited(setApiKey),
    knownEndpoint,
    entered,
    checking,
    listing,
    recognized,
    others,
    takenBy,
    unpicked,
    toggle,
    unavailable,
    showManual,
    manualOpen,
    openManual: () => setManualOpen(true),
    manual,
    canSave,
    save,
    saving,
    error,
  };
}

export type CustomProviderFormState = ReturnType<typeof useCustomProviderForm>;
