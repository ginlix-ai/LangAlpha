// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { versionOfComment } from '../commentVersions';

const sha = (c: string) => c.repeat(64);
// v1 on Nov 1, v2 on Nov 3, v3 restores v1's bytes on Nov 5.
const versions = [
  { seq: 3, sha256: sha('a'), created_at: '2026-11-05T00:00:00Z' },
  { seq: 2, sha256: sha('b'), created_at: '2026-11-03T00:00:00Z' },
  { seq: 1, sha256: sha('a'), created_at: '2026-11-01T00:00:00Z' },
];
const made = (file_sha256: string | null, created_at: string) => ({ file_sha256, created_at });

describe('the version a Drive comment was made on', () => {
  it('goes by the bytes it was made on, whatever the time says', () => {
    // Made after v3 was saved, on v2's bytes still on screen.
    expect(versionOfComment(made(sha('b'), '2026-11-06T00:00:00Z'), versions)).toBe(2);
  });

  it('tells apart two versions with the same bytes by which was current', () => {
    expect(versionOfComment(made(sha('a'), '2026-11-02T00:00:00Z'), versions)).toBe(1);
    expect(versionOfComment(made(sha('a'), '2026-11-06T00:00:00Z'), versions)).toBe(3);
    // Neither current then: the latest before it.
    expect(versionOfComment(made(sha('a'), '2026-11-04T00:00:00Z'), versions)).toBe(1);
  });

  it('does not guess a version whose bytes are no longer kept', () => {
    expect(versionOfComment(made(sha('c'), '2026-11-04T00:00:00Z'), versions)).toBeNull();
  });

  it('places a comment with no hash by the version current when it was made', () => {
    expect(versionOfComment(made(null, '2026-11-04T00:00:00Z'), versions)).toBe(2);
    expect(versionOfComment(made(null, '2026-10-01T00:00:00Z'), versions)).toBeNull();
  });

  it('does not place a comment with no hash where a version between two kept ones is gone', () => {
    // v2 was cleared: a comment made between v1 and v3 may have been made on it.
    const kept = versions.filter((v) => v.seq !== 2);
    expect(versionOfComment(made(null, '2026-11-02T00:00:00Z'), kept)).toBeNull();
    expect(versionOfComment(made(null, '2026-11-04T00:00:00Z'), kept)).toBeNull();
    // On the head, nothing later can be missing.
    expect(versionOfComment(made(null, '2026-11-06T00:00:00Z'), kept)).toBe(3);
  });
});
