import { describe, it, expect } from 'vitest';
import { HEADER_CHOICES, choiceLabel, joinHeader, splitHeader } from '../mcpHeaderNames';

describe('joinHeader', () => {
  it('writes the scheme word in front of the value and keeps the vault ref bare', () => {
    expect(joinHeader('bearer', '', '${vault:TOKEN}')).toEqual({ key: 'Authorization', value: 'Bearer ${vault:TOKEN}' });
    expect(joinHeader('basic', '', 'dXNlcjpwdw==')).toEqual({ key: 'Authorization', value: 'Basic dXNlcjpwdw==' });
    expect(joinHeader('x-api-key', '', 'k')).toEqual({ key: 'X-API-Key', value: 'k' });
    expect(joinHeader('custom', 'X-Vendor-Token', 'k')).toEqual({ key: 'X-Vendor-Token', value: 'k' });
  });

  it('leaves an empty value empty rather than sending a bare scheme word', () => {
    expect(joinHeader('bearer', '', '')).toEqual({ key: 'Authorization', value: '' });
  });
});

describe('splitHeader', () => {
  it('reads a stored header back into its choice and bare value', () => {
    expect(splitHeader('Authorization', 'Bearer ${vault:TOKEN}')).toEqual({
      choice: 'bearer',
      name: 'Authorization',
      scheme: 'Bearer',
      inner: '${vault:TOKEN}',
    });
    expect(splitHeader('authorization', 'basic abc')).toMatchObject({ choice: 'basic', scheme: 'Basic', inner: 'abc' });
    expect(splitHeader('X-API-Key', 'k')).toMatchObject({ choice: 'x-api-key', inner: 'k' });
  });

  it("keeps a vendor's own spelling as a custom header instead of normalizing it", () => {
    expect(splitHeader('X-api-key', '${vault:FUYAO}')).toEqual({
      choice: 'custom',
      name: 'X-api-key',
      scheme: null,
      inner: '${vault:FUYAO}',
    });
  });

  it('shows an Authorization value with no scheme as what it is', () => {
    expect(splitHeader('Authorization', 'raw-token')).toMatchObject({ choice: 'custom', inner: 'raw-token' });
    expect(splitHeader('Authorization', '')).toMatchObject({ choice: 'bearer', inner: '' });
  });

  it('round-trips every listed choice', () => {
    for (const c of HEADER_CHOICES) {
      if (c.id === 'custom') continue;
      const stored = joinHeader(c.id, '', 'v');
      expect(splitHeader(stored.key, stored.value).choice).toBe(c.id);
      expect(choiceLabel(c)).toBe(c.scheme ? `${c.name}: ${c.scheme}` : c.name);
    }
  });
});
