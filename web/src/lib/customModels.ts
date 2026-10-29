/**
 * Writing `custom_models`, which every provider's models share: a write
 * replaces one provider's entries and passes every other provider's through
 * untouched, so no writer can drop models it does not own.
 */
import type { CustomModelEntry } from '@/components/model/types';
import type { ConnectionOf } from '@/lib/providerCatalog';

/** Every entry but `provider`'s, those saved under another API of it included. */
export function withoutProvider(
  all: readonly CustomModelEntry[],
  provider: string,
  connectionOf: ConnectionOf,
): CustomModelEntry[] {
  return all.filter((m) => connectionOf(m.provider) !== provider);
}

/** `provider`'s entries become `next`; the rest keep their order. */
export function replaceProviderModels(
  all: readonly CustomModelEntry[],
  provider: string,
  next: readonly CustomModelEntry[],
  connectionOf: ConnectionOf,
): CustomModelEntry[] {
  return [...withoutProvider(all, provider, connectionOf), ...next];
}

/**
 * Adds `batch` to `provider`'s entries, the batch winning on a name both
 * hold. Setting up a provider again under a name it already has keeps the
 * models added to it since (the picker's Add model) rather than wiping them.
 */
export function mergeProviderModels(
  all: readonly CustomModelEntry[],
  provider: string,
  batch: readonly CustomModelEntry[],
  connectionOf: ConnectionOf,
): CustomModelEntry[] {
  const names = new Set(batch.map((m) => m.name));
  const kept = all.filter((m) => connectionOf(m.provider) === provider && !names.has(m.name));
  return replaceProviderModels(all, provider, [...kept, ...batch], connectionOf);
}
