// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { computeAgentArtifactRouting, driveRouteLocation } from '../agentArtifactRouting';
import { AGENT_PATH_TRAITS } from '../agentPathTraits';
import { classifyAgentPath } from '../agentPaths';

describe('Drive paths', () => {
  it('classifies a file in the personal space, whatever root it is spelled from', () => {
    for (const raw of [
      '.agents/drive/personal/research/q3.md',
      '/home/workspace/.agents/drive/personal/research/q3.md',
      'file:///home/workspace/.agents/drive/personal/research/q3.md',
      '__wsref__/ws-1/.agents/drive/personal/research/q3.md',
    ]) {
      expect(classifyAgentPath(raw)).toMatchObject({
        kind: 'drive',
        space: 'personal',
        path: 'research/q3.md',
        directory: false,
      });
    }
  });

  it('keeps a `#` or `?` in a Drive file name, as a path has no fragment or query', () => {
    expect(classifyAgentPath('.agents/drive/personal/results/issue#1.md')).toMatchObject({ kind: 'drive', path: 'results/issue#1.md' });
    expect(classifyAgentPath('.agents/drive/personal/why?.md')).toMatchObject({ kind: 'drive', path: 'why?.md' });
    expect(computeAgentArtifactRouting('.agents/drive/personal/results/issue#1.md'))
      .toEqual({ target: { kind: 'drive', loc: { space: null, file: 'results/issue#1.md' } }, workspaceId: null });
  });

  it('reads a trailing slash or a bare space as a folder', () => {
    expect(classifyAgentPath('.agents/drive/personal/research/')).toMatchObject({ kind: 'drive', path: 'research', directory: true });
    expect(classifyAgentPath('.agents/drive/personal')).toMatchObject({ kind: 'drive', path: '', directory: true });
  });

  it('leaves look-alikes as workspace files', () => {
    expect(classifyAgentPath('.agents/drive').kind).toBe('file');
    expect(classifyAgentPath('notes/.agents/drive/personal/x.md').kind).toBe('file');
    expect(classifyAgentPath('drive/personal/x.md').kind).toBe('file');
  });

  it("opens every Drive path in the panel's Drive tab, in no workspace: a file, a folder or the Drive itself", () => {
    const drive = (space: string | null, at: object) => ({ target: { kind: 'drive', loc: { space, ...at } }, workspaceId: null });
    expect(computeAgentArtifactRouting('.agents/drive/personal/research/Q3 notes.md')).toEqual(drive(null, { file: 'research/Q3 notes.md' }));
    expect(computeAgentArtifactRouting('.agents/drive/team-1/plan.md')).toEqual(drive('team-1', { file: 'plan.md' }));
    expect(computeAgentArtifactRouting('.agents/drive/personal/a.md', 'ws-other')).toEqual(drive(null, { file: 'a.md' }));
    expect(computeAgentArtifactRouting('.agents/drive/personal/research/')).toEqual(drive(null, { dir: 'research' }));
    expect(computeAgentArtifactRouting('.agents/drive/personal/')).toEqual(drive(null, { dir: '' }));
  });

  it('reads any Drive page link as the place it names, and no other page', () => {
    const loc = (over: object) => ({ space: null, dir: '', file: null, trash: false, memos: false, version: null, tab: null, against: null, ...over });
    expect(driveRouteLocation('/drive?file=research%2FQ3+notes.md')).toEqual(loc({ file: 'research/Q3 notes.md' }));
    expect(driveRouteLocation('/drive?space=team-1&file=plan.md&tab=changes&v=3&vs=1'))
      .toEqual(loc({ space: 'team-1', file: 'plan.md', tab: 'changes', version: 3, against: 1 }));
    expect(driveRouteLocation('/drive?dir=research')).toEqual(loc({ dir: 'research' }));
    expect(driveRouteLocation('/drive?memos=1')).toEqual(loc({ memos: true }));
    expect(driveRouteLocation('/drive?trash=1')).toEqual(loc({ trash: true }));
    expect(driveRouteLocation('/drive')).toEqual(loc({}));
    expect(driveRouteLocation('/chat?file=a.md')).toBeNull();
  });

  it('routes the Drive from a sibling workspace folder the same way', () => {
    const folders = {
      dirName: 'Home',
      siblings: [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }],
    };
    expect(computeAgentArtifactRouting('../NVDA/.agents/drive/personal/a.md', undefined, folders))
      .toEqual({ target: { kind: 'drive', loc: { space: null, file: 'a.md' } }, workspaceId: null });
  });

  it("is a plain file to the tool rows but stays out of the turn's file deck", () => {
    expect(AGENT_PATH_TRAITS.drive).toEqual({ plainFile: true, panelFile: false });
    expect(AGENT_PATH_TRAITS.file).toEqual({ plainFile: true, panelFile: true });
    expect(AGENT_PATH_TRAITS.memo.plainFile).toBe(false);
  });
});
