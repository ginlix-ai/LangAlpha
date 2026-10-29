import type { KeyPlace } from '@/hooks/useProviderCatalog';

type T = (key: string) => string;

export function regionName(t: T, region: string | null): string {
  if (region === 'cn') return t('setup.regionChina');
  if (region === 'sg') return t('setup.regionSingapore');
  if (region === 'intl' || !region) return t('setup.regionInternational');
  return region;
}

/** Which of its provider's regions a saved key is for, where there is more than one. */
export function regionTag(t: T, place: KeyPlace | undefined): string | null {
  return place && place.group.options.length > 1 ? regionName(t, place.option.region) : null;
}
