import { describe, it, expect } from 'vitest';
import {
  classifyAgentPath,
  computeAgentArtifactRouting,
  isUserProfileReadmePath,
  topicFromMemoryKey,
  workspaceRelativePath,
} from '../agentPaths';

describe('workspaceRelativePath', () => {
  it('strips the /home/workspace sandbox root', () => {
    expect(workspaceRelativePath('/home/workspace/agent.md')).toBe('agent.md');
    expect(workspaceRelativePath('/home/workspace/work/scratch/chart.png')).toBe(
      'work/scratch/chart.png',
    );
  });

  it('strips the /home/daytona sandbox root', () => {
    expect(workspaceRelativePath('/home/daytona/notes.md')).toBe('notes.md');
  });

  it('collapses the bare sandbox root to empty (caller labels it)', () => {
    expect(workspaceRelativePath('/home/workspace')).toBe('');
    expect(workspaceRelativePath('/home/daytona')).toBe('');
  });

  it('unwraps file:/// and ./ forms like the router', () => {
    expect(workspaceRelativePath('file:///home/workspace/a.md')).toBe('a.md');
    expect(workspaceRelativePath('./work/out.csv')).toBe('work/out.csv');
  });

  it('leaves a path with no sandbox prefix unchanged', () => {
    expect(workspaceRelativePath('.agents/user/memo/note.md')).toBe(
      '.agents/user/memo/note.md',
    );
  });
});

describe('classifyAgentPath', () => {
  it('classifies a user memory entry', () => {
    const r = classifyAgentPath('.agents/user/memory/risk-preferences.md');
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') {
      expect(r.tier).toBe('user');
      expect(r.key).toBe('risk-preferences.md');
      expect(r.isIndex).toBe(false);
    }
  });

  it('flags the user memory index', () => {
    const r = classifyAgentPath('.agents/user/memory/memory.md');
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') {
      expect(r.tier).toBe('user');
      expect(r.isIndex).toBe(true);
    }
  });

  it('classifies a workspace memory entry', () => {
    const r = classifyAgentPath('.agents/workspace/memory/foo.md');
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') expect(r.tier).toBe('workspace');
  });

  it('classifies a memo entry, slug opaque', () => {
    const r = classifyAgentPath('.agents/user/memo/my-report.pdf');
    expect(r.kind).toBe('memo');
    if (r.kind === 'memo') {
      expect(r.key).toBe('my-report.pdf');
      expect(r.isIndex).toBe(false);
    }
  });

  it('flags the memo index', () => {
    const r = classifyAgentPath('.agents/user/memo/memo.md');
    expect(r.kind).toBe('memo');
    if (r.kind === 'memo') expect(r.isIndex).toBe(true);
  });

  it('classifies a skill activation', () => {
    const r = classifyAgentPath('.agents/skills/investigate/SKILL.md');
    expect(r.kind).toBe('skill');
    if (r.kind === 'skill') expect(r.name).toBe('investigate');
  });

  it('strips the leading slash', () => {
    const r = classifyAgentPath('/.agents/user/memory/foo.md');
    expect(r.kind).toBe('memory');
  });

  it('strips the home/workspace/ sandbox-root prefix', () => {
    const r = classifyAgentPath('home/workspace/.agents/user/memory/foo.md');
    expect(r.kind).toBe('memory');
  });

  it('treats every well-formed shape identically', () => {
    const variants = [
      '.agents/user/memory/memory.md',
      '/.agents/user/memory/memory.md',
      'home/workspace/.agents/user/memory/memory.md',
    ];
    const kinds = variants.map((p) => classifyAgentPath(p).kind);
    expect(new Set(kinds)).toEqual(new Set(['memory']));
  });

  it('falls back to file for unknown paths', () => {
    expect(classifyAgentPath('work/notes.md').kind).toBe('file');
    expect(classifyAgentPath('').kind).toBe('file');
  });

  it('treats bare memory.md / memo.md (no prefix) as a regular file', () => {
    // Agent middleware always emits the full prefix; bare names are user files.
    expect(classifyAgentPath('memory.md').kind).toBe('file');
    expect(classifyAgentPath('memo.md').kind).toBe('file');
  });

  it('unwraps __wsref__/<wsid>/... and decorates with crossWorkspaceId', () => {
    const r = classifyAgentPath('__wsref__/abc-123/.agents/user/memory/risk.md');
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') {
      expect(r.tier).toBe('user');
      expect(r.key).toBe('risk.md');
      expect(r.crossWorkspaceId).toBe('abc-123');
    }
  });

  it('unwraps __wsref__ for workspace memory and propagates the wsid', () => {
    const r = classifyAgentPath('__wsref__/ws-X/.agents/workspace/memory/notes.md');
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') {
      expect(r.tier).toBe('workspace');
      expect(r.key).toBe('notes.md');
      expect(r.crossWorkspaceId).toBe('ws-X');
    }
  });

  it('strips the file:///home/workspace/ markdown auto-link prefix', () => {
    const r = classifyAgentPath('file:///home/workspace/.agents/user/memo/x.md');
    expect(r.kind).toBe('memo');
    if (r.kind === 'memo') {
      expect(r.key).toBe('x.md');
    }
  });

  it('strips the bare /home/daytona/ sandbox-absolute prefix', () => {
    const r = classifyAgentPath('/home/daytona/.agents/skills/foo/SKILL.md');
    expect(r.kind).toBe('skill');
    if (r.kind === 'skill') {
      expect(r.name).toBe('foo');
    }
  });

  it('strips a leading ./ before classification', () => {
    const r = classifyAgentPath('./.agents/user/memory/foo.md');
    expect(r.kind).toBe('memory');
  });

  it('strips trailing ?query and #fragment before classification', () => {
    const r = classifyAgentPath('.agents/user/memo/foo.md?ts=1#sec');
    expect(r.kind).toBe('memo');
    if (r.kind === 'memo') {
      expect(r.key).toBe('foo.md');
    }
  });

  it('falls back to file for malformed memory dir paths (trailing slash)', () => {
    // `.agents/user/memory/` has empty key — would trigger MemoryPanel's
    // not-found banner. Treat as a Files-tab dir reference instead.
    expect(classifyAgentPath('.agents/user/memory/').kind).toBe('file');
    expect(classifyAgentPath('.agents/workspace/memory/').kind).toBe('file');
  });

  describe('user-profile classification', () => {
    it('classifies portfolio.json', () => {
      const r = classifyAgentPath('.agents/user/profile/portfolio.json');
      expect(r.kind).toBe('user-profile');
      if (r.kind === 'user-profile') {
        expect(r.entity).toBe('portfolio');
      }
    });

    it('classifies watchlist.json', () => {
      const r = classifyAgentPath('.agents/user/profile/watchlist.json');
      expect(r.kind).toBe('user-profile');
      if (r.kind === 'user-profile') expect(r.entity).toBe('watchlist');
    });

    it('classifies preference.json', () => {
      const r = classifyAgentPath('.agents/user/profile/preference.json');
      expect(r.kind).toBe('user-profile');
      if (r.kind === 'user-profile') expect(r.entity).toBe('preference');
    });

    it('strips home/workspace/ sandbox-root prefix', () => {
      const r = classifyAgentPath('home/workspace/.agents/user/profile/portfolio.json');
      expect(r.kind).toBe('user-profile');
    });

    it('falls back to file for unknown filenames under the profile dir', () => {
      expect(classifyAgentPath('.agents/user/profile/other.json').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/profile/').kind).toBe('file');
    });

    it('classifies README.md under the profile dir as a generic file', () => {
      // README is classified generically; hiding happens via
      // `isUserProfileReadmePath` at the UI layer, not via the routing kind.
      expect(classifyAgentPath('.agents/user/profile/README.md').kind).toBe('file');
    });

    it('unwraps __wsref__ and propagates crossWorkspaceId', () => {
      const r = classifyAgentPath('__wsref__/ws-7/.agents/user/profile/portfolio.json');
      expect(r.kind).toBe('user-profile');
      if (r.kind === 'user-profile') {
        expect(r.entity).toBe('portfolio');
        expect(r.crossWorkspaceId).toBe('ws-7');
      }
    });
  });
});

describe('computeAgentArtifactRouting — user-profile', () => {
  it('routes portfolio.json to Files tab with targetUserProfile + clearWorkspaceId', () => {
    const r = computeAgentArtifactRouting('.agents/user/profile/portfolio.json');
    expect(r.targetUserProfile).toBe('portfolio');
    expect(r.targetFile).toBe('.agents/user/profile/portfolio.json');
    expect(r.clearWorkspaceId).toBe(true);
    // Mutually exclusive with memory/memo targets
    expect(r.targetMemoryKey).toBeNull();
    expect(r.targetMemoKey).toBeNull();
  });

  it('does not set setWorkspaceId for user-scoped user-profile paths', () => {
    const r = computeAgentArtifactRouting('.agents/user/profile/watchlist.json', 'ws-A');
    // User-profile is global to the user; ignore caller-supplied wsid.
    expect(r.setWorkspaceId).toBeNull();
    expect(r.clearWorkspaceId).toBe(true);
  });
});

describe('topicFromMemoryKey', () => {
  it('strips .md and replaces dashes/underscores', () => {
    expect(topicFromMemoryKey('risk-preferences.md')).toBe('risk preferences');
    expect(topicFromMemoryKey('my_topic.md')).toBe('my topic');
    expect(topicFromMemoryKey('plain.md')).toBe('plain');
  });

  it('handles edge cases', () => {
    expect(topicFromMemoryKey('')).toBe('');
    expect(topicFromMemoryKey('mixed-with_both.md')).toBe('mixed with both');
    expect(topicFromMemoryKey('NoExt')).toBe('NoExt');
  });
});

describe('isUserProfileReadmePath', () => {
  it('matches the relative path', () => {
    expect(isUserProfileReadmePath('.agents/user/profile/README.md')).toBe(true);
  });

  it('matches an absolute sandbox path', () => {
    expect(isUserProfileReadmePath('/home/workspace/.agents/user/profile/README.md')).toBe(true);
    expect(isUserProfileReadmePath('home/daytona/.agents/user/profile/README.md')).toBe(true);
  });

  it('matches a file:/// wrapped path', () => {
    expect(
      isUserProfileReadmePath('file:///home/workspace/.agents/user/profile/README.md'),
    ).toBe(true);
  });

  it('matches a __wsref__ cross-workspace path', () => {
    expect(
      isUserProfileReadmePath('__wsref__/ws-7/.agents/user/profile/README.md'),
    ).toBe(true);
  });

  it('does not match the data files', () => {
    expect(isUserProfileReadmePath('.agents/user/profile/portfolio.json')).toBe(false);
    expect(isUserProfileReadmePath('.agents/user/profile/watchlist.json')).toBe(false);
    expect(isUserProfileReadmePath('.agents/user/profile/preference.json')).toBe(false);
  });

  it('does not match other READMEs in the sandbox', () => {
    expect(isUserProfileReadmePath('README.md')).toBe(false);
    expect(isUserProfileReadmePath('.agents/skills/some-skill/README.md')).toBe(false);
    expect(isUserProfileReadmePath('home/workspace/work/scratch/README.md')).toBe(false);
  });

  it('handles empty / nonsense input safely', () => {
    expect(isUserProfileReadmePath('')).toBe(false);
    expect(isUserProfileReadmePath('not-a-path')).toBe(false);
  });
});
