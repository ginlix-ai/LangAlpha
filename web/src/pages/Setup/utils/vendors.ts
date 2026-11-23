import type { useTranslation } from 'react-i18next';
import type { SetupKey } from '@/api/setup';
import { providerLabel } from '@/pages/ChatAgent/components/providerUi';

type Translate = ReturnType<typeof useTranslation>['t'];

/** The key vendors no other table names: the sandbox vendors are the computer
 *  providers', and the search vendors the web search catalog's. Not translated. */
const VENDOR_NAMES: Readonly<Record<string, string>> = {
  fmp: 'Financial Modeling Prep',
  firecrawl: 'Firecrawl',
};

/** One added to the server's manifest before any table names it reads as its
 *  id, capitalized. */
export function vendorName(provider: string, t: Translate): string {
  if (Object.hasOwn(VENDOR_NAMES, provider)) return VENDOR_NAMES[provider];
  const label = providerLabel(provider, t);
  return label !== provider ? label : provider.charAt(0).toUpperCase() + provider.slice(1);
}

export const KEY_SIGNUP_URLS: Readonly<Record<string, string>> = {
  fmp: 'https://site.financialmodelingprep.com/',
  tavily: 'https://tavily.com/',
  serper: 'https://serper.dev/',
  bocha: 'https://bochaai.com/',
  exa: 'https://exa.ai/',
  parallel: 'https://parallel.ai/',
  firecrawl: 'https://www.firecrawl.dev/',
  daytona: 'https://www.daytona.io/',
  e2b: 'https://e2b.dev/dashboard?tab=keys',
};

export const DOCKER_INSTALL_URL = 'https://docs.docker.com/get-docker/';

/** The React key that remounts a key's field once what the server holds for it
 *  has changed, which closes a replace left open by the save. */
export const keyFieldKey = (key: SetupKey) => `${key.name}:${key.state}:${key.masked ?? ''}`;
