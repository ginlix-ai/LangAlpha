/**
 * The providers a setup pick goes on from without its form. A key while BYOK
 * is off runs no turn, so its pick opens the form, whose save turns BYOK
 * back on, as Settings does.
 */
import { describe, it, expect, vi } from 'vitest';
import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';

vi.mock('@/config/hostMode', () => ({ isPlatformMode: true, HOST_MODE: 'platform', APP_ENTRY_PATH: '/app' }));

import { connectedProviders } from '../shared';

const KEY: ConfiguredProvider = { provider: 'openai', display_name: 'OpenAI', access_type: 'api_key', state: 'connected', masked_key: 'sk-...abcd' };
const SIGN_IN: ConfiguredProvider = { provider: 'codex-oauth', display_name: 'ChatGPT', access_type: 'oauth', state: 'connected' };

describe('connectedProviders', () => {
  it('leaves out a key while BYOK is off, not a subscription', () => {
    expect([...connectedProviders([KEY, SIGN_IN], { answering: true, byokEnabled: false })]).toEqual(['codex-oauth']);
    expect([...connectedProviders([KEY, SIGN_IN], { answering: false, byokEnabled: false })]).toEqual(['codex-oauth']);
  });

  it('counts the key once BYOK is on', () => {
    expect([...connectedProviders([KEY, SIGN_IN], { answering: true, byokEnabled: true })]).toEqual(['openai', 'codex-oauth']);
  });
});
