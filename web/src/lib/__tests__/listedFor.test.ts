/**
 * The models a connection serves: a name the user's own entry took under
 * another connection runs there, whoever's manifest model it shares the name
 * with, so it is not offered on the manifest's connection too.
 */
import { describe, expect, it } from 'vitest';
import { listedFor } from '../connectionModels';

const metadata = { 'gpt-5': { provider: 'openai' }, 'gpt-4o': { provider: 'openai' } };
const custom = [{ name: 'gpt-5', model_id: 'gpt-5', provider: 'my-proxy' }];
// As the lists file them: the custom entry took gpt-5 out of openai's group.
const listed = { openai: { models: ['gpt-4o'] }, 'my-proxy': { models: ['gpt-5'] } };
const connectionOf = (p: string) => p;

describe('listedFor', () => {
  it('offers a name a custom entry took only on the connection it runs on', () => {
    expect(listedFor('openai', listed, metadata, custom, connectionOf)).toEqual(['gpt-4o']);
    expect(listedFor('my-proxy', listed, metadata, custom, connectionOf)).toEqual(['gpt-5']);
  });

  it('offers a manifest model filed under another group on its own connection', () => {
    expect(listedFor('openai', { brand: { models: ['gpt-5'] } }, metadata, [], connectionOf)).toEqual(['gpt-5']);
  });
});
