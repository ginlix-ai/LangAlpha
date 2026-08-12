/**
 * Feature-flag endpoints.
 * `getFeatures` returns the effective per-user flag list; `setFeatureOverride`
 * writes (or clears with null) a user override and returns the full refreshed
 * list. Both unwrap the `{ features }` envelope.
 */
import { api } from '@/api/client';
import type { FeatureState } from '@/types/api';

interface FeaturesResponse {
  features: FeatureState[];
}

export async function getFeatures(): Promise<FeatureState[]> {
  const { data } = await api.get<FeaturesResponse>('/api/v1/features');
  return data.features;
}

export async function setFeatureOverride(key: string, enabled: boolean | null): Promise<FeatureState[]> {
  const { data } = await api.put<FeaturesResponse>(`/api/v1/features/${encodeURIComponent(key)}`, { enabled });
  return data.features;
}
