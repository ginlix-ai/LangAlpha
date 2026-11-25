// @vitest-environment node
import { createHash, randomBytes } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { md5, toBase64, toHex } from '../md5';

const node = (b: Uint8Array) => createHash('md5').update(b).digest();

describe('md5', () => {
  it('matches the reference for every tail length and a part-sized buffer', () => {
    for (const n of [0, 1, 55, 56, 63, 64, 65, 119, 120, 127, 128, 1000]) {
      const b = randomBytes(n);
      expect(toHex(md5(b))).toBe(node(b).toString('hex'));
    }
    const part = randomBytes(16 * 1024 * 1024 + 3);
    expect(toBase64(md5(part))).toBe(node(part).toString('base64'));
  });

  it('reads a view that does not start on a word', () => {
    const b = randomBytes(1001);
    const view = b.subarray(1);
    expect(toHex(md5(view))).toBe(node(view).toString('hex'));
  });
});
