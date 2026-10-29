// @vitest-environment node
/** A model the catalog has not named yet takes a name from its key, and a
 *  subscription's twin of a model takes the twin's: the badge, not the name,
 *  says it runs on the sign-in. */
import { describe, it, expect } from 'vitest';
import { getModelDisplayName } from '../chat-input.helpers';

describe('getModelDisplayName', () => {
  it("prints the catalog's name, else one derived from the key", () => {
    expect(getModelDisplayName('claude-opus-5-5', { 'claude-opus-5-5': { display_name: 'Claude Opus 5.5' } }))
      .toBe('Claude Opus 5.5');
    expect(getModelDisplayName('claude-opus-5-5')).toBe('Opus 5.5');
  });

  it("names a subscription's twin as the model itself", () => {
    expect(getModelDisplayName('claude-opus-5-5-oauth')).toBe(getModelDisplayName('claude-opus-5-5'));
  });

  it('keeps the suffix of a name the user gave a model of their own', () => {
    const metadata = { 'my-model-oauth': { provider: 'my-gateway', is_custom_model: true } };
    expect(getModelDisplayName('my-model-oauth', metadata)).not.toBe(getModelDisplayName('my-model', metadata));
  });
});
