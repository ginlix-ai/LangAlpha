import { SERVER_KEY, type ConfiguredProvider } from '@/hooks/useConfiguredProviders';

/** What a saved key or local server shows under its name: the server's
 *  address, or the key as the server masks it (a deployment's own key has no
 *  mask to show, so it is named). */
export function keyDetail(p: ConfiguredProvider, t: (key: string) => string): string | null | undefined {
  if (p.access_type === 'local') return p.base_url;
  return p.masked_key === SERVER_KEY ? t('modelAccess.apiKey.serverKey') : p.masked_key;
}
