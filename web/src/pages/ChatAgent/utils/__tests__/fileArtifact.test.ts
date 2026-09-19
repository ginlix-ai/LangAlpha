/**
 * A file-operation event is classified by its workspace-relative path.
 *
 * On a shared machine the absolute spelling of a memory write is
 * `/home/workspace/<dir>/.agents/memory/x.md`. The classifier strips the
 * machine roots and nothing else, so that spelling is a plain file to it; the
 * wire therefore carries the relative form as `file_path` and the raw one
 * beside it.
 */
import { describe, expect, it } from 'vitest';

import { classifyAgentPath } from '../agentPaths';
import { fileArtifactPath } from '../fileArtifact';

describe('fileArtifactPath', () => {
  it('prefers the relative spelling, which the classifier reads as memory', () => {
    const payload = {
      operation: 'Write',
      file_path: '.agents/memory/notes.md',
      sandbox_path: '/home/workspace/acme-ab12/.agents/memory/notes.md',
    };
    const path = fileArtifactPath(payload);
    expect(path).toBe('.agents/memory/notes.md');
    expect(classifyAgentPath(path)).toMatchObject({ kind: 'memory', tier: 'workspace' });
    // The raw spelling alone is why the relative one has to travel.
    expect(classifyAgentPath(payload.sandbox_path).kind).toBe('file');
  });

  it('falls back to the raw spelling on an event that carries only that', () => {
    expect(fileArtifactPath({ sandbox_path: '/home/workspace/report.md' })).toBe('/home/workspace/report.md');
    expect(fileArtifactPath(undefined)).toBe('');
  });
});
