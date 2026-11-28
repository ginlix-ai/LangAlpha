import { describe, expect, it } from 'vitest';
import { commentable, kindOf, targetFor } from '@/lib/artifactTarget';
import { queryKeys } from '@/lib/queryKeys';
import { targetParam } from '@/pages/ChatAgent/utils/api/artifacts';

const ENTRY = '0b9e1c52-6f0a-4c7e-9d1e-3a2b5c4d6e7f';

describe('a Drive record as a target', () => {
  it('is read off a payload that says drive, and keeps its entry id as given', () => {
    expect(kindOf({ kind: 'drive' })).toBe('drive');
    expect(kindOf({ kind: 'nonsense' })).toBe('file');
    expect(kindOf({ kind: 'toString' })).toBe('file');
    expect(targetFor('drive', ENTRY, '/home/workspace')).toEqual({ kind: 'drive', path: ENTRY });
  });

  it('asks the server by its kind\'s parameter', () => {
    expect(targetParam({ kind: 'drive', path: ENTRY })).toEqual({ drive: ENTRY });
    expect(targetParam({ kind: 'chart', path: 'NVDA:1D' })).toEqual({ chart: 'NVDA:1D' });
    expect(targetParam({ kind: 'file', path: 'a.html' })).toEqual({ path: 'a.html' });
  });
});

describe("a target's record keys", () => {
  // Shapes other code invalidates by prefix, and the feed's events reach by workspace.
  it('sit among its workspace\'s reads: a file by its bare path, any other kind by an object', () => {
    expect(queryKeys.artifacts.byTarget('ws', { kind: 'file', path: 'a.html' })).toEqual(['artifacts', 'path', 'ws', 'a.html']);
    expect(queryKeys.artifacts.byTarget('ws', { kind: 'chart', path: 'NVDA:1D' })).toEqual(['artifacts', 'path', 'ws', { chart: 'NVDA:1D' }]);
    expect(queryKeys.artifacts.byTarget('ws', { kind: 'drive', path: ENTRY })).toEqual(['artifacts', 'path', 'ws', { drive: ENTRY }]);
  });

  it('keep the get-or-create beside them, apart from a file at the same string', () => {
    expect(queryKeys.artifacts.ensuredTarget('ws', { kind: 'file', path: 'a.html' })).toEqual(['artifacts', 'ensured', 'ws', 'a.html']);
    expect(queryKeys.artifacts.ensuredTarget('ws', { kind: 'chart', path: 'NVDA:1D' })).toEqual(['artifacts', 'ensured', 'ws', { chart: 'NVDA:1D' }]);
    expect(queryKeys.artifacts.ensuredTarget('ws', { kind: 'drive', path: ENTRY })).toEqual(['artifacts', 'ensured', 'ws', { drive: ENTRY }]);
  });
});

describe('what may carry comments', () => {
  it("is any file but the agent's own and the workspace's notes, read from one parse of the path", () => {
    expect(commentable({ kind: 'file', path: 'results/report.md' })).toBe(true);
    expect(commentable({ kind: 'file', path: 'notes/agent.md' })).toBe(true);
    expect(commentable({ kind: 'file', path: '/home/workspace/.agents/x.md' })).toBe(false);
    expect(commentable({ kind: 'file', path: 'agent.md' })).toBe(false);
  });

  it('is every chart and Drive file, which have no workspace path', () => {
    expect(commentable({ kind: 'chart', path: 'NVDA:1D' })).toBe(true);
    expect(commentable({ kind: 'drive', path: ENTRY })).toBe(true);
  });
});
