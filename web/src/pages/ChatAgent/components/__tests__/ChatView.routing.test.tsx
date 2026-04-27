/**
 * ChatView routing — verifies the pure routing function used by
 * `handleOpenAgentArtifactFromChat`. Critical regressions:
 *  - all targets are cleared per call (no stale-target hijacking)
 *  - memo index opens to LIST view (empty-string targetMemoKey sentinel)
 *  - user-scoped artifacts (memo, user-tier memory) clear filePanelWorkspaceId
 *  - cross-workspace ws:// links pass through targetWorkspaceId
 */
import { describe, it, expect } from 'vitest';
import { computeAgentArtifactRouting } from '../../utils/agentPaths';

describe('computeAgentArtifactRouting — per-kind routing', () => {
  it('routes user memory entry → Memory tab + tier user + clears workspace id', () => {
    const r = computeAgentArtifactRouting('.agents/user/memory/risk-preferences.md');
    expect(r).toEqual({
      targetFile: null,
      targetMemoryKey: 'risk-preferences.md',
      targetMemoryTier: 'user',
      targetMemoKey: null,
      clearWorkspaceId: true,
      setWorkspaceId: null,
    });
  });

  it('routes workspace memory without a wsid → clears stale filePanelWorkspaceId (flash-mode wsid leak guard)', () => {
    // Regression: prior versions left clearWorkspaceId=false, leaking a stale
    // ws id from a prior cross-workspace click into the new memory query.
    const r = computeAgentArtifactRouting('.agents/workspace/memory/foo.md');
    expect(r.targetMemoryKey).toBe('foo.md');
    expect(r.targetMemoryTier).toBe('workspace');
    expect(r.clearWorkspaceId).toBe(true);
    expect(r.setWorkspaceId).toBeNull();
    expect(r.targetFile).toBeNull();
    expect(r.targetMemoKey).toBeNull();
  });

  it('routes a memo entry → Memo tab + slug + clears workspace id', () => {
    const r = computeAgentArtifactRouting('.agents/user/memo/my-report.pdf');
    expect(r).toEqual({
      targetFile: null,
      targetMemoryKey: null,
      targetMemoryTier: null,
      targetMemoKey: 'my-report.pdf',
      clearWorkspaceId: true,
      setWorkspaceId: null,
    });
  });

  it('routes the memo index → Memo tab to LIST (empty-string sentinel, no entry pre-select)', () => {
    const r = computeAgentArtifactRouting('.agents/user/memo/memo.md');
    expect(r.targetMemoKey).toBe('');
    expect(r.targetMemoryKey).toBeNull();
    expect(r.targetFile).toBeNull();
    expect(r.clearWorkspaceId).toBe(true);
  });

  it('routes a skill activation → Files tab with full path', () => {
    const r = computeAgentArtifactRouting('.agents/skills/investigate/SKILL.md');
    expect(r.targetFile).toBe('.agents/skills/investigate/SKILL.md');
    expect(r.targetMemoryKey).toBeNull();
    expect(r.targetMemoKey).toBeNull();
    expect(r.clearWorkspaceId).toBe(false);
  });

  it('routes a regular file → Files tab and preserves workspace id when provided', () => {
    const r = computeAgentArtifactRouting('work/notes.md', 'ws-other');
    expect(r.targetFile).toBe('work/notes.md');
    expect(r.setWorkspaceId).toBe('ws-other');
    expect(r.clearWorkspaceId).toBe(false);
  });

  it('propagates targetWorkspaceId for workspace-tier memory (cross-workspace __wsref__ links)', () => {
    // Regression guard: when a markdown link reaches into another workspace's
    // memory (`__wsref__/ws-other/.agents/workspace/memory/notes.md`), the
    // routing must hand the linked workspace id to MemoryPanel so it queries
    // the correct workspace, not the chat's current one.
    const r = computeAgentArtifactRouting('.agents/workspace/memory/notes.md', 'ws-other');
    expect(r.targetMemoryKey).toBe('notes.md');
    expect(r.targetMemoryTier).toBe('workspace');
    expect(r.setWorkspaceId).toBe('ws-other');
    expect(r.clearWorkspaceId).toBe(false);
  });

  it('extracts wsid from a __wsref__/<wsid>/... path even without an explicit targetWorkspaceId arg', () => {
    // The path itself carries the workspace context; MemoryPanel must wire to
    // ws-X before it mounts. clearWorkspaceId stays false because we are SETTING.
    const r = computeAgentArtifactRouting('__wsref__/ws-X/.agents/user/memory/foo.md');
    expect(r.targetMemoryKey).toBe('foo.md');
    expect(r.targetMemoryTier).toBe('user');
    // User-tier memory always clears workspace id regardless of __wsref__.
    expect(r.clearWorkspaceId).toBe(true);
    expect(r.setWorkspaceId).toBeNull();
  });

  it('extracts wsid from __wsref__ for workspace-tier memory and sets it on the panel', () => {
    const r = computeAgentArtifactRouting('__wsref__/ws-X/.agents/workspace/memory/foo.md');
    expect(r.targetMemoryKey).toBe('foo.md');
    expect(r.targetMemoryTier).toBe('workspace');
    expect(r.setWorkspaceId).toBe('ws-X');
    expect(r.clearWorkspaceId).toBe(false);
  });

  it('user-tier memory always clears workspace id, even when one is provided', () => {
    // User memory is global — a stale ws id from a flash ws:// link must be
    // cleared so the memory list doesn't leak into the wrong workspace.
    const r = computeAgentArtifactRouting('.agents/user/memory/foo.md', 'ws-other');
    expect(r.targetMemoryTier).toBe('user');
    expect(r.clearWorkspaceId).toBe(true);
    expect(r.setWorkspaceId).toBeNull();
  });

  it('clears every other target on every call (no stale hijacking)', () => {
    // The function returns a fresh object every time; sibling fields are null.
    const memo = computeAgentArtifactRouting('.agents/user/memo/foo.pdf');
    expect(memo.targetFile).toBeNull();
    expect(memo.targetMemoryKey).toBeNull();

    const memory = computeAgentArtifactRouting('.agents/user/memory/foo.md');
    expect(memory.targetFile).toBeNull();
    expect(memory.targetMemoKey).toBeNull();

    const file = computeAgentArtifactRouting('work/x.md');
    expect(file.targetMemoryKey).toBeNull();
    expect(file.targetMemoKey).toBeNull();
    expect(file.targetMemoryTier).toBeNull();
  });

  it('handles leading slash and sandbox-root prefix (path normalization)', () => {
    const a = computeAgentArtifactRouting('/.agents/user/memory/foo.md');
    const b = computeAgentArtifactRouting('home/workspace/.agents/user/memory/foo.md');
    expect(a.targetMemoryKey).toBe('foo.md');
    expect(b.targetMemoryKey).toBe('foo.md');
  });

  it('falls back to file routing for unknown agent paths', () => {
    const r = computeAgentArtifactRouting('something/random.json');
    expect(r.targetFile).toBe('something/random.json');
    expect(r.targetMemoKey).toBeNull();
    expect(r.targetMemoryKey).toBeNull();
  });
});
