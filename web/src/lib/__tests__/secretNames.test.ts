import { describe, expect, it } from 'vitest';

import { MAX_SECRET_NAME_LENGTH, normalizeSecretName } from '../secretNames';

describe('normalizeSecretName', () => {
  it('spells a suggestion the way the vault accepts it', () => {
    expect(normalizeSecretName('acme-api-key')).toBe('ACME_API_KEY');
    expect(normalizeSecretName('9lives')).toBe('LIVES');
  });

  it('cuts a generated name at the vault limit so Save can succeed', () => {
    const server = 'a'.repeat(64);
    const name = normalizeSecretName(`${server}_X-api-key`);
    expect(name).toHaveLength(MAX_SECRET_NAME_LENGTH);
    expect(name).toBe('A'.repeat(64));
  });
});
