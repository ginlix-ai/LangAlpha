// @vitest-environment node
/**
 * ChatView routing: the pure decision `handleOpenFileFromChat` applies.
 * Critical regressions:
 *  - the memo index opens the Memo tab on its list (empty-string key)
 *  - user-scoped artifacts (memo, user-tier memory) name no workspace, so a
 *    panel left on a sibling returns to the view's own
 *  - cross-workspace links carry the workspace they name
 */
import { describe, it, expect } from 'vitest';
import { computeAgentArtifactRouting } from '../../utils/agentArtifactRouting';
import { MEMORY_WORKSPACE_DIR } from '../../utils/agentPaths';

const panel = (target: object, workspaceId: string | null = null) => ({ kind: 'panel', target, workspaceId });

describe('computeAgentArtifactRouting: per-kind routing', () => {
  it('routes a user memory entry to the Memory tab, user tier, in no workspace', () => {
    expect(computeAgentArtifactRouting('.agents/user/memory/risk-preferences.md'))
      .toEqual(panel({ kind: 'memory', key: 'risk-preferences.md', tier: 'user' }));
  });

  it('routes workspace memory without a wsid to no workspace (wsid leak guard for chats whose links name other workspaces)', () => {
    // Regression: a stale ws id from a prior cross-workspace click once leaked
    // into the new memory query.
    expect(computeAgentArtifactRouting(`${MEMORY_WORKSPACE_DIR}/foo.md`))
      .toEqual(panel({ kind: 'memory', key: 'foo.md', tier: 'workspace' }));
  });

  it('routes a memo entry to the Memo tab by slug, in no workspace', () => {
    expect(computeAgentArtifactRouting('.agents/user/memo/my-report.pdf'))
      .toEqual(panel({ kind: 'memo', key: 'my-report.pdf' }));
  });

  it('routes the memo index to the Memo tab list (empty-string key, no entry pre-select)', () => {
    expect(computeAgentArtifactRouting('.agents/user/memo/memo.md')).toEqual(panel({ kind: 'memo', key: '' }));
  });

  it('routes a skill activation to the Files tab with its full path', () => {
    expect(computeAgentArtifactRouting('.agents/skills/investigate/SKILL.md'))
      .toEqual(panel({ kind: 'file', path: '.agents/skills/investigate/SKILL.md' }));
  });

  it('routes a regular file to the Files tab, in the workspace provided', () => {
    expect(computeAgentArtifactRouting('work/notes.md', 'ws-other'))
      .toEqual(panel({ kind: 'file', path: 'work/notes.md' }, 'ws-other'));
  });

  it('carries targetWorkspaceId for workspace-tier memory (cross-workspace __wsref__ links)', () => {
    // A link into another workspace's memory must hand that workspace to
    // MemoryPanel, not the chat's current one.
    expect(computeAgentArtifactRouting(`${MEMORY_WORKSPACE_DIR}/notes.md`, 'ws-other'))
      .toEqual(panel({ kind: 'memory', key: 'notes.md', tier: 'workspace' }, 'ws-other'));
  });

  it('user-tier memory names no workspace even through a __wsref__ path', () => {
    expect(computeAgentArtifactRouting('__wsref__/ws-X/.agents/user/memory/foo.md'))
      .toEqual(panel({ kind: 'memory', key: 'foo.md', tier: 'user' }));
  });

  it('extracts the wsid from __wsref__ for workspace-tier memory', () => {
    expect(computeAgentArtifactRouting(`__wsref__/ws-X/${MEMORY_WORKSPACE_DIR}/foo.md`))
      .toEqual(panel({ kind: 'memory', key: 'foo.md', tier: 'workspace' }, 'ws-X'));
  });

  it('user-tier memory names no workspace even when one is provided', () => {
    // User memory is global: the memory list must not open in the wrong workspace.
    expect(computeAgentArtifactRouting('.agents/user/memory/foo.md', 'ws-other'))
      .toEqual(panel({ kind: 'memory', key: 'foo.md', tier: 'user' }));
  });

  it('handles leading slash and sandbox-root prefix (path normalization)', () => {
    const memory = panel({ kind: 'memory', key: 'foo.md', tier: 'user' });
    expect(computeAgentArtifactRouting('/.agents/user/memory/foo.md')).toEqual(memory);
    expect(computeAgentArtifactRouting('home/workspace/.agents/user/memory/foo.md')).toEqual(memory);
  });

  it('routes a folder link to the Files tab on that folder instead of a file lookup', () => {
    expect(computeAgentArtifactRouting('results/q3/')).toEqual(panel({ kind: 'file', dir: 'results/q3' }));
    expect(computeAgentArtifactRouting('/home/workspace/results/')).toEqual(panel({ kind: 'file', dir: 'results' }));
    expect(computeAgentArtifactRouting('/home/workspace/')).toEqual(panel({ kind: 'file', dir: '' }));
    const ws = '20cc68e8-d057-41f4-9bb1-57aa8d310704';
    expect(computeAgentArtifactRouting(`__wsref__/${ws}/work/`, ws)).toEqual(panel({ kind: 'file', dir: 'work' }, ws));
  });

  it('removes the selected project folder from absolute artifact paths', () => {
    const folders = { dirName: 'acme-a1b2', siblings: [] };
    expect(computeAgentArtifactRouting('/home/workspace/acme-a1b2/results/', undefined, folders))
      .toEqual(panel({ kind: 'file', dir: 'results' }));
    expect(computeAgentArtifactRouting('/home/workspace/acme-a1b2/results/report.md', undefined, folders))
      .toEqual(panel({ kind: 'file', path: 'results/report.md' }));
    expect(computeAgentArtifactRouting('/home/workspace/acme-a1b2/.agents/workspace/memory/notes.md', undefined, folders))
      .toEqual(panel({ kind: 'memory', key: 'notes.md', tier: 'workspace' }));
  });

  it('falls back to file routing for unknown agent paths', () => {
    expect(computeAgentArtifactRouting('something/random.json')).toEqual(panel({ kind: 'file', path: 'something/random.json' }));
  });

  it('opens a Drive path on the Drive page', () => {
    expect(computeAgentArtifactRouting('.agents/drive/personal/notes/q3.md'))
      .toEqual({ kind: 'page', href: '/drive?file=notes%2Fq3.md' });
  });
});
