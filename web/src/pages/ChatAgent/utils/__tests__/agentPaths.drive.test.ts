// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { computeAgentArtifactRouting } from '../agentArtifactRouting';
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
      .toEqual({ kind: 'page', href: '/drive?file=results%2Fissue%231.md' });
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

  it('routes to the Drive page, never the panel', () => {
    const page = (href: string) => ({ kind: 'page', href });
    expect(computeAgentArtifactRouting('.agents/drive/personal/research/Q3 notes.md')).toEqual(page('/drive?file=research%2FQ3+notes.md'));
    expect(computeAgentArtifactRouting('.agents/drive/personal/research/')).toEqual(page('/drive?dir=research'));
    expect(computeAgentArtifactRouting('.agents/drive/personal/')).toEqual(page('/drive'));
  });

  it('routes the Drive from a sibling workspace folder the same way', () => {
    const folders = {
      dirName: 'Home',
      siblings: [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }],
    };
    expect(computeAgentArtifactRouting('../NVDA/.agents/drive/personal/a.md', undefined, folders))
      .toEqual({ kind: 'page', href: '/drive?file=a.md' });
  });

  it('is a plain file to the tool rows but stays out of the panel', () => {
    expect(AGENT_PATH_TRAITS.drive).toEqual({ plainFile: true, panelFile: false });
    expect(AGENT_PATH_TRAITS.file).toEqual({ plainFile: true, panelFile: true });
    expect(AGENT_PATH_TRAITS.memo.plainFile).toBe(false);
  });
});
