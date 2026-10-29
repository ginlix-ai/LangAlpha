// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ProviderCatalogEntry } from '@/components/model/types';
import { indexCatalog, ownProviderNames } from '../providerCatalog';

const entry = (over: Partial<ProviderCatalogEntry>): ProviderCatalogEntry => ({
  provider: 'p',
  display_name: 'P',
  access_type: 'api_key',
  brand_key: 'p',
  byok_eligible: true,
  ...over,
});

const gateway = entry({
  provider: 'gateway',
  display_name: 'Gateway',
  sdk: 'anthropic',
  routes: ['gateway-chat', 'gateway-responses'],
});
const plan = entry({
  provider: 'plan',
  display_name: 'Plan',
  sdk: 'anthropic',
  region_variants: [{ provider: 'plan-cn', display_name: 'Plan (China)', region: 'cn', sdk: 'openai' }],
});

describe('indexCatalog', () => {
  const { rows, entryFor, connectionOf } = indexCatalog([gateway, plan]);

  it('files a route under its entry, as one API of its connection', () => {
    expect(entryFor('gateway-chat')).toBe(gateway);
    expect(connectionOf('gateway-chat')).toBe('gateway');
    expect(connectionOf('gateway')).toBe('gateway');
  });

  it('keeps a region its own connection, with its own SDK', () => {
    expect(entryFor('plan-cn')).toBe(plan);
    expect(connectionOf('plan-cn')).toBe('plan-cn');
    expect(rows.get('plan-cn')?.sdk).toBe('openai');
    expect(rows.get('plan')?.sdk).toBe('anthropic');
  });

  it('leaves a provider outside the catalog its own connection', () => {
    expect(entryFor('my-server')).toBeNull();
    expect(connectionOf('my-server')).toBe('my-server');
  });

  it("leaves a user's own provider saved under a route's name the user's", () => {
    const own = indexCatalog([gateway], ownProviderNames([{ name: 'gateway-chat' }, { name: 7 }]));
    expect(own.entryFor('gateway-chat')).toBeNull();
    expect(own.connectionOf('gateway-chat')).toBe('gateway-chat');
    expect(own.connectionOf('gateway-responses')).toBe('gateway');
  });
});
