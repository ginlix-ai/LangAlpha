import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';

/**
 * What a picker prints for a model key: the manifest's `display_name` when it
 * authors one, else the key. Only the printed text changes; the key stays the
 * value every control stores and sends.
 */
export function modelLabel(key: string, metadata?: Record<string, ModelMetadataEntry>): string {
  return metadata?.[key]?.display_name || key;
}

/** Search matches either what is printed or the key a user may already know. */
export function modelMatches(key: string, query: string, metadata?: Record<string, ModelMetadataEntry>): boolean {
  if (!query) return true;
  return key.toLowerCase().includes(query) || modelLabel(key, metadata).toLowerCase().includes(query);
}
