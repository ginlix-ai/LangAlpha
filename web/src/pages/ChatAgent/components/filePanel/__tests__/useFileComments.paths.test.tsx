/**
 * The panel offers comments only on a file that can hold them: the server
 * keeps no record for the agent's own files and answers 400, so offering
 * them there only showed "Comments couldn't load".
 */
import { describe, expect, it, vi } from 'vitest';
import { renderHookWithProviders } from '@/test/utils';
import type { CommentUi } from '../../viewers/html/comments/useCommentUi';
import { useFileComments } from '../useFileComments';

const versionReads = vi.hoisted(() => ({ paths: [] as Array<string | null> }));
vi.mock('../useArtifactVersionView', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useArtifactVersionView: (_ws: string | null, path: string | null) => {
    versionReads.paths.push(path);
    return { version: null };
  },
}));
vi.mock('../../../contexts/WorkspaceContext', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  useWorkspaceFolders: () => null,
}));

const ui = { docOf: () => null } as unknown as CommentUi;

function comments(file: string, commentViewer: 'html' | 'markdown') {
  versionReads.paths.length = 0;
  return renderHookWithProviders(() => useFileComments({
    workspaceId: 'ws-1', tabId: 'tab-1', file, commentViewer, source: false, active: true, readAt: null, ui, dismissSelection: () => {},
  })).result.current;
}

describe('the file panel offers comments', () => {
  it.each([
    ['.agents/notes/plan.md', 'markdown'],
    ['.agents/report.html', 'html'],
    ['_internal/out.md', 'markdown'],
    ['agent.md', 'markdown'],
    ['/home/workspace/.agents/x.md', 'markdown'],
  ] as const)('not on %s, whose record the server refuses', (file, viewer) => {
    const got = comments(file, viewer);
    expect(got.target).toBeNull();
    expect(got.path).toBeNull();
    expect(got.tools).toBeFalsy();
    expect(versionReads.paths.every((p) => p === null)).toBe(true);
  });

  it.each([
    ['results/report.md', 'markdown'],
    ['notes/agent.md', 'markdown'],
    ['site/index.html', 'html'],
  ] as const)('on %s', (file, viewer) => {
    const got = comments(file, viewer);
    if (viewer === 'html') expect(got.path).toBe(file);
    else expect(got.target).toMatchObject({ kind: 'file', path: file });
  });
});
