import { useMemo } from 'react';
import { useProviderCatalog } from '@/hooks/useProviderCatalog';
import { useDebouncedValue } from '@/lib/useDebouncedValue';
import type { ProviderCatalogEntry } from '@/components/model/types';

/**
 * Whether a base URL is an API we know, or something in front of one. A proxy
 * for Claude or OpenAI models is where most broken setups come from: it drops
 * prompt caching, routes to another model, or strips thinking settings, and
 * the user sees it as our fault. A provider's own API and the clouds that host
 * these models first-hand are not proxies, so they carry no warning.
 */

/** Where the SDK goes when a provider names no base URL (the catalog leaves it out). */
const SDK_DEFAULT_HOSTS = ['api.anthropic.com', 'generativelanguage.googleapis.com'];

/** AWS Bedrock (runtime and Mantle), Azure OpenAI and AI Foundry, Google Vertex AI. */
const CLOUD_HOSTS: readonly RegExp[] = [
  /^bedrock-(runtime|mantle)(-fips)?\.[a-z0-9-]+\.(amazonaws\.com(\.cn)?|api\.aws)$/,
  /\.openai\.azure\.com$/,
  /\.services\.ai\.azure\.com$/,
  /\.cognitiveservices\.azure\.com$/,
  /^([a-z0-9-]+-)?aiplatform\.googleapis\.com$/,
];

function hostOf(url: string): string | null {
  try {
    return new URL(url.trim()).hostname.toLowerCase() || null;
  } catch {
    return null;
  }
}

/** The hosts of every catalog provider reached with a key, in every region. */
export function knownHosts(catalog: readonly ProviderCatalogEntry[]): ReadonlySet<string> {
  const hosts = new Set(SDK_DEFAULT_HOSTS);
  for (const entry of catalog) {
    if (entry.access_type !== 'api_key' && entry.access_type !== 'coding_plan') continue;
    for (const url of [entry.base_url, ...(entry.region_variants ?? []).map((v) => v.base_url)]) {
      const host = url ? hostOf(url) : null;
      if (host) hosts.add(host);
    }
  }
  return hosts;
}

/** Null while the URL does not parse yet; otherwise whether it is a known API or cloud. */
export function isKnownEndpoint(url: string, known: ReadonlySet<string>): boolean | null {
  const host = hostOf(url);
  if (!host) return null;
  return known.has(host) || CLOUD_HOSTS.some((re) => re.test(host));
}

export function useKnownHosts(): ReadonlySet<string> {
  const { catalog } = useProviderCatalog();
  return useMemo(() => knownHosts(catalog), [catalog]);
}

/**
 * `isKnownEndpoint` for a URL being typed: null while the field is empty, and
 * judged on the address once typing rests, so the warning neither shows before
 * there is an address nor flickers through the half-typed hosts on the way.
 */
export function useEndpointVerdict(url: string): boolean | null {
  const known = useKnownHosts();
  const live = url.trim();
  const rested = useDebouncedValue(live, 600);
  return live ? isKnownEndpoint(rested, known) : null;
}
