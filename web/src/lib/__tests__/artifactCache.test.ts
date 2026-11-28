import { describe, expect, it } from 'vitest';
import { QueryClient } from '@tanstack/react-query';
import { ALL, refreshArtifactComments, refreshDriveComments } from '@/lib/artifactCache';
import { queryKeys } from '@/lib/queryKeys';

function client() {
  const qc = new QueryClient();
  const seed = (key: readonly unknown[]) => qc.setQueryData(key, { seeded: true });
  return { qc, seed, stale: (key: readonly unknown[]) => qc.getQueryState(key)?.isInvalidated ?? false };
}

describe('what a comment change makes stale', () => {
  it("reads the file's destinations again, and no other file's, so the next comment goes where the last one went", () => {
    const { qc, seed, stale } = client();
    const mine = queryKeys.artifacts.destinations('a-1', null);
    const search = queryKeys.artifacts.destinationThreads('a-1', '');
    const other = queryKeys.artifacts.destinations('a-2', null);
    [mine, search, other].forEach(seed);

    refreshArtifactComments(qc, [{ artifactId: 'a-1', workspaceId: 'home', kind: 'drive' }], ALL, { listed: true });

    expect(stale(mine)).toBe(true);
    expect(stale(search)).toBe(true);
    expect(stale(other)).toBe(false);
  });

  it("reads every file's destinations again when which files changed is not known", () => {
    const { qc, seed, stale } = client();
    const key = queryKeys.artifacts.destinations('a-2', 't-1');
    seed(key);

    refreshArtifactComments(qc, ALL, ALL);

    expect(stale(key)).toBe(true);
  });
});

describe("what a workspace file's comment change leaves alone", () => {
  it("keeps the Drive's list markers and the file's destinations, which its comments do not move", () => {
    const { qc, seed, stale } = client();
    const keys = [queryKeys.drive.commentCounts('personal'), queryKeys.artifacts.destinations('a-1', null)];
    keys.forEach(seed);

    refreshArtifactComments(qc, [{ artifactId: 'a-1', workspaceId: 'ws', kind: 'file' }], 't-1');

    expect(keys.map(stale)).toEqual([false, false]);
  });

  it('reads them again for a file whose kind is not known, which may be a Drive file', () => {
    const { qc, seed, stale } = client();
    const keys = [queryKeys.drive.commentCounts('personal'), queryKeys.artifacts.destinations('a-1', null)];
    keys.forEach(seed);

    refreshArtifactComments(qc, [{ artifactId: 'a-1' }], 't-1');

    expect(keys.map(stale)).toEqual([true, true]);
  });
});

describe('what a Drive file trashed, restored or saved makes stale', () => {
  it("reads the list markers, every chat's queue and the destinations again", () => {
    const { qc, seed, stale } = client();
    const keys = [
      queryKeys.drive.commentCounts('personal'),
      queryKeys.artifacts.queued('t-1'),
      queryKeys.artifacts.destinations('a-1', null),
    ];
    keys.forEach(seed);

    refreshDriveComments(qc);

    expect(keys.map(stale)).toEqual([true, true, true]);
  });

  it("leaves the comment lists, which a trash or a save does not change", () => {
    const { qc, seed, stale } = client();
    const key = queryKeys.artifacts.comments('a-1');
    seed(key);

    refreshDriveComments(qc);

    expect(stale(key)).toBe(false);
  });
});
