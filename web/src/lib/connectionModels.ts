/**
 * Which models belong to one connection (a key, a subscription, a provider of
 * the user's own), for the writers that act on a connection alone: removing
 * it, and saving its model picks. Both take a model out of every preference
 * that names it, and preferences store models by name only, so a name another
 * connection still serves must be left alone.
 */
import type { CustomModelEntry, KeyCandidates } from '@/components/model/types';
import type { ConnectionOf } from '@/lib/providerCatalog';

/** The manifest's rows (`model_metadata`), before the user's entries are merged over them. */
export type ManifestModels = Readonly<Record<string, { provider?: string; access_type?: string } | undefined>>;

type Candidates = Readonly<Record<string, KeyCandidates>> | undefined;

/**
 * Whether the manifest model `name` runs on `provider`'s key as it is, with no
 * entry of the user's naming the provider: one of its own rows, or a coding
 * plan's row the plan's key reaches under `provider` (a region of the plan,
 * or another protocol of it), which the server's walk finds there. A plan keeps
 * one region, so the walk never finds another region's key first.
 */
export function runsNatively(name: string, provider: string, manifest: ManifestModels, keyCandidates: Candidates): boolean {
  const meta = manifest[name];
  if (!meta?.provider) return false;
  if (meta.provider === provider) return true;
  return meta.access_type === 'coding_plan' && !!keyCandidates?.[meta.provider]?.shared.includes(provider);
}

export interface ConnectionContext {
  customModels: readonly CustomModelEntry[];
  manifest: ManifestModels;
  /** Every provider the user has connected. */
  connected: ReadonlySet<string>;
  /** Where a turn looks for each provider's key (`key_candidates`). */
  keyCandidates?: Readonly<Record<string, KeyCandidates>>;
  /** A model saved under another API of a connection is the connection's. */
  connectionOf: ConnectionOf;
}

const nameOf = (cm: CustomModelEntry) => cm.name || cm.model_id;

/** A saved custom entry under another provider took this name, so the name routes there. */
export function takenElsewhere(
  name: string,
  provider: string,
  customModels: readonly CustomModelEntry[],
  connectionOf: ConnectionOf,
): boolean {
  return customModels.some((cm) => connectionOf(cm.provider) !== provider && nameOf(cm) === name);
}

/**
 * Whether `name` still runs once `provider` gives it up: a custom entry under
 * another provider took the name (a provider of the user's own serving a
 * built-in, a coding plan's pick of its brand's model), or a key the server
 * would find for it is still connected: its own provider's (a coding plan's
 * pick that the brand's key runs too), or one the vendor issues as one with it
 * (a region's key, the server's `shares_api_key`).
 */
export function servedElsewhere(name: string, provider: string, ctx: ConnectionContext): boolean {
  if (takenElsewhere(name, provider, ctx.customModels, ctx.connectionOf)) return true;
  const own = ctx.manifest[name]?.provider;
  if (!own) return false;
  const holders = ctx.keyCandidates?.[own]?.shared ?? [own];
  return holders.some((s) => s !== provider && ctx.connected.has(s));
}

/** Every model name `provider` serves that leaves the user's lists with it. */
export function modelsLeavingWith(provider: string, ctx: ConnectionContext): Set<string> {
  const names = [
    ...ctx.customModels.filter((cm) => ctx.connectionOf(cm.provider) === provider).map(nameOf),
    ...Object.keys(ctx.manifest).filter((name) => runsNatively(name, provider, ctx.manifest, ctx.keyCandidates)),
  ];
  return new Set(names.filter((m) => !servedElsewhere(m, provider, ctx)));
}

/**
 * The manifest's models a connection offers: those that run on it as they are
 * (`runsNatively`), or for a variant with none (a coding plan of a brand, or a
 * region of one) those of `modelsFrom`, the provider the catalog names for it,
 * each picked as an entry naming the variant. Read off each model's own
 * provider rather than the catalog's grouping, which files a region's models
 * under the top-level brand (`z-ai-cn`'s under `z-ai`), so a variant of the
 * region would find none. A name a custom entry under another provider took
 * routes there, so it is not this connection's to offer.
 */
export function connectionBuiltIns(
  provider: string,
  modelsFrom: string,
  manifest: ManifestModels,
  customModels: readonly CustomModelEntry[],
  connectionOf: ConnectionOf,
  keyCandidates?: Candidates,
): string[] {
  const offered = (runs: (name: string) => boolean) =>
    Object.keys(manifest).filter((name) => runs(name) && !takenElsewhere(name, provider, customModels, connectionOf));
  const native = offered((name) => runsNatively(name, provider, manifest, keyCandidates));
  if (native.length > 0 || provider === modelsFrom) return native;
  return offered((name) => manifest[name]?.provider === modelsFrom);
}

/**
 * The picks a connection's model picker opens with: what is on the user's
 * list and is this connection's (saved under it, or its own manifest model).
 * Only a connection just made, with nothing of its own on the list yet, starts
 * on every candidate; one the user already manages opens as they left it, even
 * with nothing picked.
 */
export function seedPicks({
  candidates,
  savedNames,
  starred,
  native,
  justConnected,
}: {
  candidates: readonly string[];
  /** The names saved as custom entries under this provider. */
  savedNames: readonly string[];
  starred: readonly string[];
  /** Whether a manifest model runs on this connection as it is (`runsNatively`). */
  native: (name: string) => boolean;
  justConnected: boolean;
}): Set<string> {
  const mine = (m: string) => savedNames.includes(m) || native(m);
  const kept = starred.filter((m) => candidates.includes(m) && mine(m));
  return new Set(kept.length === 0 && justConnected ? candidates : kept);
}
