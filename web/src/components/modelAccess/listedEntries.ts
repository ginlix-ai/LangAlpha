/**
 * A provider's listed models as the custom-models entries a pick saves them
 * as, and saved entries refreshed from what the list says now.
 */
import type { CustomModelEntry } from '@/components/model/types';
import type { ListedModel } from '@/api/model';
import { slugifyModelName } from './slugifyModelName';

/** The name a custom model may take (`CUSTOM_MODEL_NAME_RE` on the server). */
const NAME_RE = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,62}$/;
const MODALITIES = new Set(['text', 'image', 'pdf']);

/** What a listed model takes beyond text, as an entry saves it; null for text
 *  alone, which an entry says by naming none. */
function listedModalities(listed: ListedModel): string[] | null {
  // OpenRouter's `file` is a PDF, as the server reads it.
  const modalities = [...new Set((listed.input_modalities ?? []).map((m) => (m === 'file' ? 'pdf' : m)))]
    .filter((m) => MODALITIES.has(m));
  return modalities.some((m) => m !== 'text') ? ['text', ...modalities.filter((m) => m !== 'text')] : null;
}

/** The custom-models entry an untuned listed model is saved as: under the
 *  connection `provider`, or the API of it a gateway's catalog serves the
 *  model on, with what that API needs sent. Its prompt guidance is not
 *  copied: a turn on it inherits the matching manifest model's. */
export function entryForListed(
  listed: ListedModel,
  provider: string,
  taken: ReadonlySet<string>,
): CustomModelEntry {
  // The provider's own id where the server accepts it as a name and it names
  // no other model; prefixed with the provider otherwise, so it never shadows
  // a built-in of the same name, numbered on where that is taken too. A
  // gateway's ids (a `route` says it is one) are its vendors' own, which a
  // later release may give a built-in, so a pick from one is always prefixed.
  const own = !listed.route && NAME_RE.test(listed.id) && !taken.has(listed.id);
  let name = own ? listed.id : slugifyModelName(`${provider}/${listed.id}`);
  if (!name) name = slugifyModelName(listed.id) || 'model';
  for (let n = 2, stem = name; taken.has(name); n++) name = `${stem.slice(0, 62 - String(n).length)}-${n}`;
  const entry: CustomModelEntry = { name, model_id: listed.id, provider: listed.route ?? provider };
  const modalities = listedModalities(listed);
  if (modalities) entry.input_modalities = modalities;
  if (typeof listed.context_window === 'number' && listed.context_window > 0) entry.context = listed.context_window;
  if (listed.reasoning) entry.reasoning = listed.reasoning;
  if (listed.parameters) entry.parameters = listed.parameters;
  // A catalog's name is the model's; a plain list's may be anything, so the
  // entry's own name stands there.
  if (listed.described && listed.display_name) entry.display_name = listed.display_name;
  return entry;
}

/**
 * Saved entries with the levels, window and inputs the provider's list now
 * gives each, so a pick follows the provider: one saved before lists carried
 * levels gets them on the next save, and a window lowered or an input dropped
 * stops being sent as the model's. A field the list leaves out, and an entry
 * it does not name, stay as saved, except on a model a gateway's catalog
 * describes, whose route and what it sends there are the catalog's.
 */
export function withListedSpecs(entries: readonly CustomModelEntry[], listed: readonly ListedModel[]): CustomModelEntry[] {
  const byId = new Map(listed.map((m) => [m.id, m]));
  return entries.map((cm) => {
    const now = byId.get(cm.model_id);
    if (!now) return cm;
    const context = typeof now.context_window === 'number' && now.context_window > 0 ? now.context_window : null;
    const next: CustomModelEntry = {
      ...cm,
      ...(context ? { context } : {}),
      ...(now.reasoning ? { reasoning: now.reasoning } : {}),
      ...(now.described && now.display_name ? { display_name: now.display_name } : {}),
    };
    // A gateway's catalog says which API the model is served on and what that
    // API needs, together: a model it moves to another API moves whole.
    if (now.described) {
      if (now.route) next.provider = now.route;
      if (now.parameters) next.parameters = now.parameters;
      else delete next.parameters;
      if (!now.reasoning) delete next.reasoning;
    }
    if (Array.isArray(now.input_modalities)) {
      const modalities = listedModalities(now);
      if (modalities) next.input_modalities = modalities;
      else delete next.input_modalities;
    }
    return next;
  });
}
