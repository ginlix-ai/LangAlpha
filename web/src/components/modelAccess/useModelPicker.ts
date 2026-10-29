import { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useAllModels } from '@/hooks/useAllModels';
import { useModels } from '@/hooks/useModels';
import { usePreferences } from '@/hooks/usePreferences';
import { useReadStoredPreferences } from '@/hooks/useStoredPreferences';
import { useConfiguredProviders } from '@/hooks/useConfiguredProviders';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { useUpdatePreferences } from '@/hooks/useUpdatePreferences';
import { useAvailableModels, useLocalProviderModels, useRefreshAvailableModels } from '@/hooks/useProviderModels';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import { modelPrefs, splitPreferenceWrite, starredModels } from '@/lib/modelPreferences';
import { connectionBuiltIns, runsNatively, seedPicks, takenElsewhere } from '@/lib/connectionModels';
import { apiErrorDetail } from '@/api/errors';
import type { CustomModelEntry } from '@/components/model/types';
import type { ListedModel } from '@/api/model';
import { planIncludes, type ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { entryForListed } from './listedEntries';
import { pickerWrite } from './pickerWrite';

const NO_MODELS: Record<string, ModelMetadataEntry> = {};

/** A row of Ready to use: a manifest model, or one a gateway's catalog
 *  describes (`listed`), shown with what the catalog says of it. */
export interface ReadyRow {
  name: string;
  listed?: ListedModel;
}

/**
 * Which models a provider's connection offers, and which of them the user has
 * picked: the provider's own list (Ready to use, then the untuned rest), the
 * manifest's when the list could not be read, a local server's own list, and
 * the user's own models for it. `save` writes the picks as the configured list
 * (`starred_models`) and any new custom models, and takes a model just
 * unpicked out of everything that names it, unless another connection still
 * serves it.
 *
 * `justConnected`: the connection was just made, so with nothing of its own on
 * the list yet the picker starts on every model it offers. Anywhere else it
 * opens on what is saved, which may be nothing.
 */
export function useModelPicker({
  provider,
  modelsFrom,
  justConnected = false,
}: {
  provider: string;
  modelsFrom?: string;
  justConnected?: boolean;
}) {
  const { t } = useTranslation();
  const source = modelsFrom ?? provider;
  const { metadata, isLoading: modelsLoading } = useAllModels();
  const { models: modelsData } = useModels();
  const { providers: configured, configuredSet } = useConfiguredProviders();
  const { entryFor, connectionOf } = useProviderCatalog();
  const { preferences, isLoaded: prefsLoaded } = usePreferences();
  const readStored = useReadStoredPreferences();
  const updatePreferences = useUpdatePreferences();

  const entry = entryFor(provider);
  const isCustomProvider = (modelPrefs(preferences).custom_providers ?? []).some((cp) => cp.name === provider);
  const isLocal = entry?.dynamic_models === true;
  const searchFirst = entry?.sdk === 'openrouter' || provider === 'openrouter';
  const listable = !!entry && !isLocal && !isCustomProvider;

  const [query, setQuery] = useState('');
  const debounced = useDebouncedValue(query.trim(), 300);
  const base = useAvailableModels(listable ? provider : null);
  const searched = useAvailableModels(listable && searchFirst && debounced ? provider : null, { q: debounced });
  const local = useLocalProviderModels(isLocal ? provider : null);
  const refreshList = useRefreshAvailableModels();
  const [refreshing, setRefreshing] = useState(false);

  const allCustom = useMemo(() => modelPrefs(preferences).custom_models ?? [], [preferences]);
  // The manifest's own rows, before the user's entries are merged over them:
  // a saved entry named after one of these reaches that model through this
  // provider and keeps its tuning, so it is ready to use, not the user's own.
  const manifestMeta = modelsData?.model_metadata ?? NO_MODELS;
  const keyCandidates = modelsData?.key_candidates;
  /** A manifest model this connection runs as it is, needing no entry of its own. */
  const native = useCallback(
    (m: string) => runsNatively(m, provider, manifestMeta, keyCandidates),
    [provider, manifestMeta, keyCandidates],
  );
  // The manifest's models for this provider: a variant without entries of
  // its own (a coding plan, or a region of one) shares `modelsFrom`'s.
  const builtIn = useMemo<string[]>(
    () => connectionBuiltIns(provider, source, manifestMeta, allCustom, connectionOf, keyCandidates),
    [provider, source, manifestMeta, allCustom, connectionOf, keyCandidates],
  );

  // The user's own models under this exact provider, by name (what every
  // preference stores).
  const savedCustom = useMemo(
    () => allCustom.filter((cm) => connectionOf(cm.provider) === provider),
    [allCustom, provider, connectionOf],
  );
  const savedCustomNames = useMemo(() => savedCustom.map((cm) => cm.name || cm.model_id), [savedCustom]);
  const savedTuned = useMemo(
    () => savedCustomNames.filter((m) => !!manifestMeta[m] && !builtIn.includes(m)),
    [savedCustomNames, manifestMeta, builtIn],
  );

  const list = base.data && base.data.provider === provider ? base.data : null;
  const listOk = list?.status === 'ok';
  const listFallback = list?.status === 'fallback';
  const listSettled = !listable || !base.isLoading;

  // A subscription's plan, which some of its models are limited to.
  const plan = configured.find((p) => p.provider === provider && p.access_type === 'oauth')?.plan_type;

  // Ready to use: the manifest models the list confirms, or every one when
  // there is no list to confirm them, less those the plan does not include.
  const ready = useMemo(() => {
    // A name a custom entry under another provider took routes there, so the
    // list's word for it is not this connection's to act on.
    const confirmed = (listOk ? list!.ready.filter((m) => metadata[m] && !takenElsewhere(m, provider, allCustom, connectionOf)) : builtIn)
      .filter((m) => planIncludes(metadata[m], plan));
    return [...confirmed, ...savedTuned.filter((m) => !confirmed.includes(m))];
  }, [listOk, list, builtIn, metadata, provider, allCustom, savedTuned, plan, connectionOf]);
  const missing = listOk ? list!.missing : [];

  const listed = useMemo<ListedModel[]>(() => {
    if (isLocal) return (local.data ?? []).map((m) => ({ id: m.id, display_name: m.name }));
    return list?.others ?? [];
  }, [isLocal, local.data, list]);
  // A gateway's catalog tunes the models it describes (OpenCode Go), so they
  // are listed as ready to use; the untuned rest stay behind Show all.
  const described = useMemo(() => listed.filter((m) => m.described), [listed]);
  const others = useMemo(() => listed.filter((m) => !m.described), [listed]);
  const results = searchFirst && debounced && searched.data?.provider === provider ? searched.data.others : others;

  // Models put on the user's own list this visit and not saved yet: added by
  // hand, or ticked from the untuned rest or a search. They stay under the
  // user's own until removed there, ticked or not.
  const [added, setAdded] = useState<ReadonlyMap<string, CustomModelEntry>>(new Map());

  // Every listed model as the entry a pick saves it as, so each row is one
  // name: a saved or added entry for the model where there is one, else a
  // name no other model, built-in or the user's, has taken.
  const listedEntries = useMemo(() => {
    const byId = new Map<string, CustomModelEntry>();
    for (const cm of [...savedCustom, ...added.values()]) if (!byId.has(cm.model_id)) byId.set(cm.model_id, cm);
    const taken = new Set([...Object.keys(metadata), ...allCustom.map((cm) => cm.name || cm.model_id), ...added.keys()]);
    for (const m of [...listed, ...results]) {
      if (byId.has(m.id)) continue;
      const e = entryForListed(m, provider, taken);
      taken.add(e.name);
      byId.set(m.id, e);
    }
    return byId;
  }, [savedCustom, added, metadata, allCustom, listed, results, provider]);
  /** The name a listed model is picked under. */
  const nameOf = (m: ListedModel) => listedEntries.get(m.id)?.name ?? m.id;
  /** Entries not saved yet, each saved if its name is picked. */
  const unsaved = useMemo(() => {
    const out = new Map(added);
    for (const e of listedEntries.values()) if (!savedCustom.includes(e) && !out.has(e.name)) out.set(e.name, e);
    return [...out.values()];
  }, [added, listedEntries, savedCustom]);

  // Ready to use, one list: the manifest's models, then those the gateway's
  // catalog describes.
  const readyRows = useMemo<ReadyRow[]>(
    () => [
      ...ready.map((name) => ({ name })),
      ...described.map((m) => ({ name: listedEntries.get(m.id)?.name ?? m.id, listed: m })),
    ],
    [ready, described, listedEntries],
  );

  // A connection just made whose models all come from its gateway's catalog
  // (OpenCode Go) has no ready list to start on, so it starts on the models
  // tuned elsewhere in the manifest; the rest are a Select all away.
  const seededListed = useMemo(
    () => (!justConnected || savedCustom.length > 0 || builtIn.length > 0
      ? []
      : readyRows.filter((r) => r.listed?.known).map((r) => r.name)),
    [justConnected, savedCustom, builtIn, readyRows],
  );

  // A described model shows checked in its own list, not again as the user's.
  const yours = useMemo(() => {
    const listedReady = new Set(readyRows.map((r) => r.name));
    return [...new Set([...savedCustomNames, ...added.keys()])].filter((m) => !listedReady.has(m));
  }, [savedCustomNames, added, readyRows]);
  /** Every name this view decides about: what is saved under this provider,
   *  and its own manifest models. Picks outside it belong to other providers,
   *  a coding plan's brand models among them until picked here. */
  const owned = useMemo(
    () => [...new Set([...[...builtIn, ...ready].filter(native), ...savedCustomNames, ...yours])],
    [builtIn, ready, native, savedCustomNames, yours],
  );

  const existingStarred = useMemo(() => starredModels(preferences), [preferences]);

  // Seeded once everything it reads has arrived: the picks already saved for
  // this provider, or for a connection just made every model it offers. Null
  // until the user changes something, so the seed follows the data until then.
  const [picked, setPicked] = useState<Set<string> | null>(null);
  // Loaded, not present: a new user has no preferences row yet (a 404 read as null).
  const inputsReady = !modelsLoading && prefsLoaded && listSettled && (!isLocal || !local.isLoading);
  const seed = useMemo(() => seedPicks({
    candidates: [...ready, ...savedCustomNames, ...seededListed],
    savedNames: savedCustomNames,
    starred: existingStarred,
    native,
    justConnected,
  }), [ready, savedCustomNames, seededListed, existingStarred, native, justConnected]);
  const selected = picked ?? seed;
  const initialized = picked !== null || inputsReady;
  const setSelected = (update: (prev: Set<string>) => Set<string>) => setPicked((prev) => update(prev ?? seed));

  /** Picks or unpicks a model by name. An untuned listed model picked joins
   *  the user's own. */
  const toggle = (name: string) => {
    const e = unsaved.find((u) => u.name === name);
    if (e && !selected.has(name) && !added.has(name) && !readyRows.some((r) => r.name === name)) {
      setAdded((prev) => new Map(prev).set(name, e));
    }
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  /** Pick every model in Ready to use. */
  const selectAllReady = () => setSelected((prev) => new Set([...prev, ...readyRows.map((r) => r.name)]));

  const addManual = (e: CustomModelEntry) => {
    setAdded((prev) => new Map(prev).set(e.name, e));
    setSelected((prev) => new Set(prev).add(e.name));
  };

  /** Drops an unsaved model, or unpicks a saved one (saving then removes it). */
  const removeYours = (name: string) => {
    setAdded((prev) => {
      if (!prev.has(name)) return prev;
      const next = new Map(prev);
      next.delete(name);
      return next;
    });
    setSelected((prev) => {
      const next = new Set(prev);
      next.delete(name);
      return next;
    });
  };

  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setRefreshing(true);
    try {
      await refreshList(provider);
    } catch {
      // The list on screen stays; the server allows one refresh per 10s.
    }
    // Not a `finally`, which keeps the React Compiler off this hook.
    setRefreshing(false);
  };

  /** Writes the picks. Resolves true when saved. */
  const save = async (): Promise<boolean> => {
    setSaving(true);
    setError(null);
    try {
      const stored = await readStored();
      await updatePreferences.mutateAsync(splitPreferenceWrite(pickerWrite(stored, {
        provider, selected, owned, saved: savedCustom, unsaved, listed, builtIn,
        manifest: manifestMeta, native, connected: configuredSet, keyCandidates, connectionOf,
      })));
    } catch (e) {
      const detail = apiErrorDetail(e);
      setError(typeof detail === 'string' ? detail : t('modelAccess.models.saveFailed'));
      setSaving(false);
      return false;
    }
    setAdded(new Map());
    setSaving(false);
    return true;
  };

  return {
    provider,
    entry,
    metadata,
    manifestMeta,
    loading: !initialized,
    listing: listable && (base.isFetching || refreshing),
    listOk,
    listFallback,
    fallbackReason: listFallback ? list!.reason ?? null : null,
    listable,
    isLocal,
    localError: isLocal && local.isError,
    searchFirst,
    query,
    setQuery,
    searching: searchFirst && !!debounced && searched.isFetching,
    searchedFor: debounced,
    ready: readyRows,
    missing,
    others,
    results,
    yours,
    nameOf,
    selected,
    toggle,
    selectAllReady,
    addManual,
    removeYours,
    refresh,
    refreshing,
    save,
    saving,
    error,
  };
}

export type ModelPickerState = ReturnType<typeof useModelPicker>;
