import type { CustomModelEntry, KeyCandidates } from '@/components/model/types';
import type { ListedModel } from '@/api/model';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import type { ConnectionOf } from '@/lib/providerCatalog';
import { modelPrefs, pruneModelReferences, starredModels, type PreferencePatch } from '@/lib/modelPreferences';
import { replaceProviderModels } from '@/lib/customModels';
import { servedElsewhere } from '@/lib/connectionModels';
import { withListedSpecs } from './listedEntries';

/** The fields an entry that only routes a model to a provider has. */
const ROUTING_ONLY = new Set(['name', 'model_id', 'provider']);

/** What the model picker holds when it saves; see `useModelPicker`. */
export interface PickerSnapshot {
  provider: string;
  /** The picked names. */
  selected: ReadonlySet<string>;
  /** Every name the picker decides about; a name outside it is another connection's. */
  owned: readonly string[];
  /** The user's entries under this connection, as rendered. */
  saved: readonly CustomModelEntry[];
  /** Entries not saved yet, each saved if its name is picked. */
  unsaved: readonly CustomModelEntry[];
  /** The provider's list, which refreshes the saved entries it names. */
  listed: readonly ListedModel[];
  /** The manifest's models this connection offers. */
  builtIn: readonly string[];
  /** The manifest's own rows, before the user's entries are merged over them. */
  manifest: Readonly<Record<string, ModelMetadataEntry>>;
  /** A manifest model this connection runs as it is, needing no entry of its own. */
  native: (name: string) => boolean;
  connected: ReadonlySet<string>;
  keyCandidates?: Readonly<Record<string, KeyCandidates>>;
  connectionOf: ConnectionOf;
}

/**
 * The preferences write that saves a connection's picks onto what is stored
 * now (`stored`), so another provider's models and stars saved from another
 * tab are kept: the picks become this connection's entries and its part of
 * the list, and a model just unpicked leaves everything that names it, unless
 * another connection still serves it.
 */
export function pickerWrite(stored: unknown, s: PickerSnapshot): PreferencePatch {
  const storedCustom = modelPrefs(stored).custom_models ?? [];
  const ctx = {
    customModels: storedCustom,
    manifest: s.manifest,
    connected: s.connected,
    keyCandidates: s.keyCandidates,
    connectionOf: s.connectionOf,
  };
  // Unpicked here, and run by no other connection: a name another one still
  // serves keeps its place on the list and its settings.
  const unpicked = (m: string) => s.owned.includes(m) && !s.selected.has(m) && !servedElsewhere(m, s.provider, ctx);
  const fromOthers = starredModels(stored).filter((m) => !s.selected.has(m) && !unpicked(m));
  // A saved model stays while picked; unpicking one of the user's own removes it.
  // An entry that only named this provider for a model that now runs on it
  // as it is (a plan's model picked under a region before regions routed)
  // goes, so the model is the plan's own again. One that says anything more
  // about the model (a window, parameters) is the user's, and stays.
  const redundant = (cm: CustomModelEntry) =>
    s.native(cm.name)
    && cm.model_id === (s.manifest[cm.name]?.model_id ?? cm.name)
    && Object.keys(cm).every((k) => ROUTING_ONLY.has(k));
  const keptSaved = withListedSpecs(
    s.saved.filter((cm) => s.selected.has(cm.name || cm.model_id) && !redundant(cm)),
    s.listed,
  );
  const keptNames = new Set(keptSaved.map((cm) => cm.name || cm.model_id));
  const added = s.unsaved.filter((e) => s.selected.has(e.name) && !keptNames.has(e.name));
  // A parent-catalog model picked under a variant (a coding plan) needs an
  // entry naming the variant, or its key would not be the one used.
  const variantPicks = [...s.selected]
    .filter((m) => s.builtIn.includes(m) && !keptNames.has(m) && !s.native(m))
    .map((m): CustomModelEntry => ({ name: m, model_id: s.manifest[m]?.model_id ?? m, provider: s.provider }));
  const custom = replaceProviderModels(storedCustom, s.provider, [...keptSaved, ...added, ...variantPicks], s.connectionOf);
  return {
    ...pruneModelReferences(stored, unpicked),
    starred_models: [...fromOthers, ...s.selected],
    custom_models: custom.length > 0 ? custom : null,
  };
}
