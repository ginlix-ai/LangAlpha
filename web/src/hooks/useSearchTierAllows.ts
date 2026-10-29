import { isPlatformMode } from '@/config/hostMode';
import { useUser } from './useUser';

/**
 * Whether the user's plan reaches a search provider's or depth's `min_tier`,
 * which comes pre-resolved from the API. Gated in platform mode only; the
 * server enforces it at resolve time, so a disabled option is UX only.
 */
export function useSearchTierAllows(): (minTier: number) => boolean {
  const { user } = useUser();
  return (minTier) => !isPlatformMode || (user?.access_tier ?? -1) >= minTier;
}
