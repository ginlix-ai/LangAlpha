import { describe, expect, it } from 'vitest';
import type { ProviderCatalogEntry } from '@/components/model/types';
import { isKnownEndpoint, knownHosts } from '../endpointTrust';

const entry = (provider: string, access_type: string, base_url: string | null, regions: string[] = []) => ({
  provider, display_name: provider, access_type, brand_key: provider, byok_eligible: true, base_url,
  region_variants: regions.map((url, i) => ({ provider: `${provider}-${i}`, region: 'cn', base_url: url })),
}) as unknown as ProviderCatalogEntry;

const known = knownHosts([
  entry('openai', 'api_key', 'https://api.openai.com/v1'),
  entry('anthropic', 'api_key', null),
  entry('z-ai', 'api_key', 'https://api.z.ai/api/paas/v4', ['https://open.bigmodel.cn/api/paas/v4']),
  entry('ollama', 'local', 'http://localhost:11434/v1'),
  entry('codex-oauth', 'oauth', 'https://chatgpt.com/backend-api/codex'),
]);

describe('isKnownEndpoint', () => {
  it.each([
    'https://api.openai.com/v1',
    'https://api.anthropic.com',
    'https://open.bigmodel.cn/api/anthropic',
    'https://bedrock-runtime.us-east-1.amazonaws.com/anthropic',
    'https://bedrock-mantle.eu-west-1.api.aws/v1',
    'https://my-res.openai.azure.com/openai/v1/',
    'https://my-res.services.ai.azure.com/anthropic',
    'https://us-east5-aiplatform.googleapis.com/v1/projects/p/locations/us-east5/endpoints/openapi',
  ])('trusts a provider API or a recognized cloud: %s', (url) => {
    expect(isKnownEndpoint(url, known)).toBe(true);
  });

  it.each([
    'http://127.0.0.1:8317/v1',
    'http://localhost:11434/v1',
    'https://chatgpt.com/backend-api/codex',
    'https://api.some-relay.com/v1',
    'https://openai.azure.com.evil.example/v1',
    'https://bedrock-runtime.us-east-1.amazonaws.com.evil.example',
  ])('treats anything else as unverified: %s', (url) => {
    expect(isKnownEndpoint(url, known)).toBe(false);
  });

  it('has no answer until the URL parses', () => {
    expect(isKnownEndpoint('', known)).toBeNull();
    expect(isKnownEndpoint('api.openai', known)).toBeNull();
  });
});
