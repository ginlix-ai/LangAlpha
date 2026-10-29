// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  MEMORY_WORKSPACE_DIR,
  USER_DATA_FILES,
  classifyAgentPath,
  computeAgentArtifactRouting,
  computerFolders,
  isAgentNotesPath,
  isSubagentFilePath,
  isUserDataReadmePath,
  normalizeAgentHref,
  normalizeAgentPath,
  onOtherComputer,
  parseAgentHref,
  parseAgentPath,
  folderWorkspacePath,
  topicFromMemoryKey,
  workspaceRelativePath,
  workspaceScopedPath,
  type ComputerFolders,
  type ComputerRootRow,
  type FolderWorkspace,
} from '../agentPaths';

/** The viewed workspace's folders, and the other workspaces on its computer. */
function own(
  dirName: string | null,
  previousDirNames?: string[] | null,
  siblings: readonly FolderWorkspace[] = [],
): ComputerFolders {
  return { dirName, previousDirNames, siblings };
}

/** The user's computers, one per root, as the computer list gives them. */
function machines(...roots: (string | null)[]): ComputerRootRow[] {
  return roots.map((root_dir, i) => ({ computer_id: `c-${i}`, root_dir }));
}

/**
 * The one set of path rules. Every other helper is a projection of these, so
 * a path has to mean the same thing whichever route it arrived by.
 */
describe('normalizeAgentPath', () => {
  it('strips the sandbox root and folds dot segments', () => {
    expect(normalizeAgentPath('/home/workspace/results/report.md')).toBe('results/report.md');
    expect(normalizeAgentPath('file:///home/daytona/results/report.md')).toBe('results/report.md');
    expect(normalizeAgentPath('./results/../data/./x.csv')).toBe('data/x.csv');
  });

  it('keeps a true absolute path absolute', () => {
    expect(normalizeAgentPath('/tmp/out.csv')).toBe('/tmp/out.csv');
    expect(normalizeAgentPath('/large_tool_results/abc')).toBe('/large_tool_results/abc');
  });

  it('is idempotent, so a path may pass through more than one layer', () => {
    for (const raw of [
      '/home/workspace/a/../b.md', 'file:///home/daytona/x.csv', 'results/', '/tmp/y',
      // Names a decoded href arrives carrying. Each one used to lose everything
      // from its punctuation on, because the layer that read it a second time
      // read it as a URL again.
      'results/issue#1.md', 'results/a?b.md', '../data.csv', 'results/季度报告.md',
    ]) {
      expect(normalizeAgentPath(normalizeAgentPath(raw))).toBe(normalizeAgentPath(raw));
    }
  });

  it('keeps punctuation a file name is allowed to hold', () => {
    // A path, not a link: the `#` in `issue#1.md` is part of the name. Reading
    // it as a fragment truncated the path to `results/issue`, and the panel
    // then asked the server for a file nothing had written.
    expect(normalizeAgentPath('results/issue#1.md')).toBe('results/issue#1.md');
    expect(normalizeAgentPath('results/a?b.md')).toBe('results/a?b.md');
  });

  it('lets a relative path keep the levels it climbs', () => {
    // Dropping the `..` rewrote the reference into a different file, one that
    // often exists, so the link opened the wrong document instead of missing.
    expect(normalizeAgentPath('../data.csv')).toBe('../data.csv');
    expect(normalizeAgentPath('../../a/b.md')).toBe('../../a/b.md');
    expect(normalizeAgentPath('work/../../data.csv')).toBe('../data.csv');
    // A path that starts at a root has nowhere to climb.
    expect(normalizeAgentPath('/home/workspace/../etc/passwd')).toBe('etc/passwd');
    expect(normalizeAgentPath('/../etc/passwd')).toBe('/etc/passwd');
  });

  it('reports the workspace qualifier, the root and the directory intent', () => {
    expect(parseAgentPath('__wsref__/ws-7/results/review.md')).toEqual({
      workspaceId: 'ws-7', path: 'results/review.md', absolute: false, directory: false,
    });
    expect(parseAgentPath('/home/workspace/results/')).toMatchObject({ path: 'results/', absolute: true, directory: true });
    expect(parseAgentPath('./')).toMatchObject({ path: '', directory: true });
    expect(parseAgentPath('charts/fig.png')).toMatchObject({ absolute: false, directory: false });
  });
});

describe('normalizeAgentHref', () => {
  it('decodes percent escapes exactly once', () => {
    expect(normalizeAgentHref('results/a%20b.md')).toBe('results/a b.md');
    // The literal `%20` a correctly-encoded link spells `%2520`.
    expect(normalizeAgentHref('results/a%2520b.md')).toBe('results/a%20b.md');
  });

  it('drops a query or fragment a link can carry and a path cannot', () => {
    expect(normalizeAgentHref('results/report.md#heading')).toBe('results/report.md');
    expect(normalizeAgentHref('results/chart.png?v=2')).toBe('results/chart.png');
  });

  it('leaves a lone percent alone rather than throwing', () => {
    expect(normalizeAgentHref('results/100%_done.md')).toBe('results/100%_done.md');
  });
});

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

/**
 * A destination is a URL and a path is not, and the two readings differ by one
 * rule. The reading has to be chosen once, at the layer that holds the string,
 * because `normalizeAgentHref` decodes: after it runs, a `%23` has become a
 * literal `#` and any second reading as a URL eats the rest of the name.
 */
describe('normalizeAgentHref — a destination, read once', () => {
  it('decodes a percent-escaped name and hands back a path', () => {
    expect(normalizeAgentHref('results/%E5%AD%A3%E5%BA%A6%E6%8A%A5%E5%91%8A.md')).toBe('results/季度报告.md');
    expect(normalizeAgentHref('results/Q3%20deck.pptx')).toBe('results/Q3 deck.pptx');
    expect(normalizeAgentHref('results/report%20(final).md')).toBe('results/report (final).md');
  });

  it('gives an escaped `#` or `?` back as part of the name', () => {
    expect(normalizeAgentHref('results/issue%231.md')).toBe('results/issue#1.md');
    expect(normalizeAgentHref('results/a%3Fb.md')).toBe('results/a?b.md');
  });

  it('still reads an unescaped `?` or `#` as link syntax', () => {
    expect(normalizeAgentHref('results/report.md?ts=1')).toBe('results/report.md');
    expect(normalizeAgentHref('.agents/user/memo/foo.md#sec')).toBe('.agents/user/memo/foo.md');
  });

  it('leaves a lone `%` alone rather than reading it as a broken escape', () => {
    expect(normalizeAgentHref('results/100% done.md')).toBe('results/100% done.md');
    expect(normalizeAgentHref('results/100%25 done.md')).toBe('results/100% done.md');
  });

  it('carries non-Latin names and the punctuation around them intact', () => {
    for (const name of [
      'results/季度报告.md', 'results/日本語 ファイル.pdf', 'results/한국어 보고서.md',
      'results/Отчёт 2026.md', 'results/naïve résumé.docx', 'results/图表 📊.png',
      'results/tag[1].md', 'results/a&b.md', 'results/a+b.md', '分析/结果.md',
    ]) {
      expect(normalizeAgentHref(name)).toBe(name);
    }
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
    // Built from the generated dir: the spelling belongs to paths.py, and the
    // contract under test is that a path under it classifies as memory.
    const r = classifyAgentPath(`${MEMORY_WORKSPACE_DIR}/foo.md`);
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') expect(r.tier).toBe('workspace');
  });

  // The pre-folder spelling, written out on purpose: the generated constant no
  // longer carries it, and stored transcripts still do.
  it('classifies a legacy workspace memory entry, bare and root-prefixed', () => {
    for (const p of [
      '.agents/workspace/memory/foo.md',
      '/home/workspace/.agents/workspace/memory/foo.md',
      'file:///home/daytona/.agents/workspace/memory/foo.md',
    ]) {
      expect(classifyAgentPath(p)).toMatchObject({ kind: 'memory', tier: 'workspace', key: 'foo.md' });
    }
    expect(classifyAgentPath('.agents/workspace/memory/memory.md')).toMatchObject({
      kind: 'memory', tier: 'workspace', isIndex: true,
    });
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
    const r = classifyAgentPath(`__wsref__/ws-X/${MEMORY_WORKSPACE_DIR}/notes.md`);
    expect(r.kind).toBe('memory');
    if (r.kind === 'memory') {
      expect(r.tier).toBe('workspace');
      expect(r.key).toBe('notes.md');
      expect(r.crossWorkspaceId).toBe('ws-X');
    }
  });

  it('unwraps __wsref__ for legacy workspace memory and propagates the wsid', () => {
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
    expect(classifyAgentPath(`${MEMORY_WORKSPACE_DIR}/`).kind).toBe('file');
    expect(classifyAgentPath('.agents/workspace/memory/').kind).toBe('file');
  });

  describe('user-data classification', () => {
    it('classifies every file in USER_DATA_FILES by its name without .json', () => {
      for (const [dir, files] of Object.entries(USER_DATA_FILES)) {
        for (const file of files) {
          const r = classifyAgentPath(`${dir}/${file}`);
          expect(r.kind).toBe('user-data');
          if (r.kind === 'user-data') expect(r.entity).toBe(file.replace(/\.json$/, ''));
        }
      }
    });

    it('classifies the profile files and each automation file', () => {
      const entity = (path: string) => {
        const r = classifyAgentPath(path);
        return r.kind === 'user-data' ? r.entity : null;
      };
      expect(entity('.agents/user/profile/portfolio.json')).toBe('portfolio');
      expect(entity('.agents/user/profile/watchlist.json')).toBe('watchlist');
      expect(entity('.agents/user/profile/preference.json')).toBe('preference');
      // One file per automation, under any name the server accepts.
      expect(entity('.agents/user/automations/morning-brief.json')).toBe('automations');
      expect(entity('.agents/user/automations/AAPL_below_200.json')).toBe('automations');
      expect(entity('.agents/user/automations/portfolio.json')).toBe('automations');
    });

    it('strips the sandbox-root prefix', () => {
      expect(classifyAgentPath('home/workspace/.agents/user/profile/portfolio.json').kind).toBe('user-data');
      expect(classifyAgentPath('/home/workspace/.agents/user/automations/morning-brief.json').kind).toBe('user-data');
    });

    it('falls back to file for names no data file takes and the directories themselves', () => {
      expect(classifyAgentPath('.agents/user/profile/other.json').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/profile/').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/automations/').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/automations/notes.txt').kind).toBe('file');
      // An editor's temporary file, which the server refuses too.
      expect(classifyAgentPath('.agents/user/automations/brief.json.tmp').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/automations/-brief.json').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/automations/sub/brief.json').kind).toBe('file');
      expect(classifyAgentPath(`.agents/user/automations/${'a'.repeat(65)}.json`).kind).toBe('file');
      expect(classifyAgentPath('.agents/user/profile/morning-brief.json').kind).toBe('file');
    });

    it('classifies the README beside the data files as a generic file', () => {
      // README is classified generically; hiding happens via
      // `isUserDataReadmePath` at the UI layer, not via the routing kind.
      expect(classifyAgentPath('.agents/user/profile/README.md').kind).toBe('file');
      expect(classifyAgentPath('.agents/user/automations/README.md').kind).toBe('file');
    });

    it('unwraps __wsref__ and propagates crossWorkspaceId', () => {
      const profile = classifyAgentPath('__wsref__/ws-7/.agents/user/profile/portfolio.json');
      expect(profile.kind).toBe('user-data');
      if (profile.kind === 'user-data') {
        expect(profile.entity).toBe('portfolio');
        expect(profile.crossWorkspaceId).toBe('ws-7');
      }
      const automations = classifyAgentPath('__wsref__/ws-7/.agents/user/automations/morning-brief.json');
      expect(automations.kind).toBe('user-data');
      if (automations.kind === 'user-data') {
        expect(automations.entity).toBe('automations');
        expect(automations.crossWorkspaceId).toBe('ws-7');
      }
    });
  });
});

describe('computeAgentArtifactRouting: user data', () => {
  it('routes portfolio.json to the Files tab and clears the workspace id', () => {
    const r = computeAgentArtifactRouting('.agents/user/profile/portfolio.json');
    expect(r.targetFile).toBe('.agents/user/profile/portfolio.json');
    expect(r.clearWorkspaceId).toBe(true);
    // Mutually exclusive with memory/memo targets
    expect(r.targetMemoryKey).toBeNull();
    expect(r.targetMemoKey).toBeNull();
  });

  it('does not set setWorkspaceId for user-scoped profile paths', () => {
    const r = computeAgentArtifactRouting('.agents/user/profile/watchlist.json', 'ws-A');
    // The profile is global to the user; ignore caller-supplied wsid.
    expect(r.setWorkspaceId).toBeNull();
    expect(r.clearWorkspaceId).toBe(true);
  });

  it('routes an automation file to the Files tab as user-scoped, ignoring a caller wsid', () => {
    const r = computeAgentArtifactRouting('.agents/user/automations/morning-brief.json', 'ws-A');
    expect(r.targetFile).toBe('.agents/user/automations/morning-brief.json');
    expect(r.setWorkspaceId).toBeNull();
    expect(r.clearWorkspaceId).toBe(true);
  });

  it('ignores an embedded __wsref__ id too', () => {
    const r = computeAgentArtifactRouting('__wsref__/ws-7/.agents/user/profile/portfolio.json');
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

describe('isUserDataReadmePath', () => {
  it('matches the relative path', () => {
    expect(isUserDataReadmePath('.agents/user/profile/README.md')).toBe(true);
    expect(isUserDataReadmePath('.agents/user/automations/README.md')).toBe(true);
  });

  it('matches an absolute sandbox path', () => {
    expect(isUserDataReadmePath('/home/workspace/.agents/user/profile/README.md')).toBe(true);
    expect(isUserDataReadmePath('home/daytona/.agents/user/profile/README.md')).toBe(true);
    expect(isUserDataReadmePath('/home/workspace/.agents/user/automations/README.md')).toBe(true);
  });

  it('matches a file:/// wrapped path', () => {
    expect(
      isUserDataReadmePath('file:///home/workspace/.agents/user/profile/README.md'),
    ).toBe(true);
  });

  it('matches a __wsref__ cross-workspace path', () => {
    expect(
      isUserDataReadmePath('__wsref__/ws-7/.agents/user/profile/README.md'),
    ).toBe(true);
  });

  it('does not match the data files', () => {
    expect(isUserDataReadmePath('.agents/user/profile/portfolio.json')).toBe(false);
    expect(isUserDataReadmePath('.agents/user/profile/watchlist.json')).toBe(false);
    expect(isUserDataReadmePath('.agents/user/profile/preference.json')).toBe(false);
    expect(isUserDataReadmePath('.agents/user/automations/morning-brief.json')).toBe(false);
  });

  it('does not match other READMEs in the sandbox', () => {
    expect(isUserDataReadmePath('README.md')).toBe(false);
    expect(isUserDataReadmePath('.agents/skills/some-skill/README.md')).toBe(false);
    expect(isUserDataReadmePath('home/workspace/work/scratch/README.md')).toBe(false);
  });

  it('handles empty / nonsense input safely', () => {
    expect(isUserDataReadmePath('')).toBe(false);
    expect(isUserDataReadmePath('not-a-path')).toBe(false);
  });
});

describe('isSubagentFilePath', () => {
  it('matches a subagent file under any prefix the agent emits', () => {
    expect(isSubagentFilePath('.agents/user/agents/research.md')).toBe(true);
    expect(isSubagentFilePath('/home/workspace/.agents/user/agents/research.md')).toBe(true);
    expect(isSubagentFilePath('home/daytona/.agents/user/agents/filings-reader.md')).toBe(true);
    expect(isSubagentFilePath('file:///home/workspace/.agents/user/agents/research.md')).toBe(true);
    expect(isSubagentFilePath('./.agents/user/agents/research.md')).toBe(true);
    expect(isSubagentFilePath('__wsref__/ws-7/.agents/user/agents/research.md')).toBe(true);
  });

  it('does not match the README, a nested file, or a name the server refuses', () => {
    expect(isSubagentFilePath('.agents/user/agents/README.md')).toBe(false);
    expect(isSubagentFilePath('.agents/user/agents/drafts/research.md')).toBe(false);
    expect(isSubagentFilePath('.agents/user/agents/')).toBe(false);
    expect(isSubagentFilePath('.agents/user/agents/Research.md')).toBe(false);
    expect(isSubagentFilePath('.agents/user/agents/research.md.tmp')).toBe(false);
    expect(isSubagentFilePath('.agents/user/agents/-research.md')).toBe(false);
  });

  it('does not match other files', () => {
    expect(isSubagentFilePath('research.md')).toBe(false);
    expect(isSubagentFilePath('work/.agents/user/agents/research.md')).toBe(false);
    expect(isSubagentFilePath('.agents/user/automations/research.md')).toBe(false);
    expect(isSubagentFilePath('.agents/user/memory/research.md')).toBe(false);
    expect(isSubagentFilePath('')).toBe(false);
  });
});

describe('isAgentNotesPath', () => {
  const notes = (raw: string, dir?: string | null) => isAgentNotesPath(parseAgentPath(raw), dir);

  it('matches the notes file at the workspace root', () => {
    expect(notes('agent.md')).toBe(true);
    expect(notes('./agent.md')).toBe(true);
    expect(notes('/home/workspace/agent.md')).toBe(true);
    expect(notes('file:///home/daytona/agent.md')).toBe(true);
  });

  it('matches the notes file under the workspace\'s project folder', () => {
    expect(notes('/home/workspace/alpha/agent.md', 'alpha')).toBe(true);
    // Relative paths already resolve inside the folder, so this one is nested.
    expect(notes('alpha/agent.md', 'alpha')).toBe(false);
  });

  it('keeps an agent.md in any other folder', () => {
    expect(notes('/home/workspace/docs/agent.md', 'alpha')).toBe(false);
    expect(notes('/home/workspace/docs/agent.md')).toBe(false);
    expect(notes('reports/agent.md')).toBe(false);
    expect(notes('reports/agent.md', 'alpha')).toBe(false);
  });

  it('keeps an agent.md rooted outside the sandbox', () => {
    expect(notes('/tmp/agent.md')).toBe(false);
    expect(notes('/tmp/agent.md', 'tmp')).toBe(false);
    expect(notes('/alpha/agent.md', 'alpha')).toBe(false);
  });

  it('reads a link the same way as a tool path', () => {
    expect(isAgentNotesPath(parseAgentHref('agent.md#notes'))).toBe(true);
    expect(isAgentNotesPath(parseAgentHref('/home/workspace/alpha/agent.md?v=2'), 'alpha')).toBe(true);
    expect(isAgentNotesPath(parseAgentHref('agent.md/'))).toBe(false);
  });

  it('matches the notes file in a folder a rename moved the workspace out of', () => {
    const renamed = (raw: string) => isAgentNotesPath(parseAgentPath(raw), 'Research', ['research-ab12', 'alpha']);
    expect(renamed('/home/workspace/Research/agent.md')).toBe(true);
    expect(renamed('/home/workspace/research-ab12/agent.md')).toBe(true);
    expect(renamed('file:///home/daytona/alpha/agent.md')).toBe(true);
    // Still only a sandbox-anchored path names the folder.
    expect(renamed('research-ab12/agent.md')).toBe(false);
    expect(renamed('/home/workspace/beta/agent.md')).toBe(false);
    // Without a current folder the former ones still count.
    expect(isAgentNotesPath(parseAgentPath('/home/workspace/alpha/agent.md'), null, ['alpha'])).toBe(true);
  });

  it('matches the notes file one climb out of the working directory and back into the folder', () => {
    expect(notes('../alpha/agent.md', 'alpha')).toBe(true);
    expect(notes('./../alpha/agent.md', 'alpha')).toBe(true);
    expect(isAgentNotesPath(parseAgentPath('../RESEARCH-AB12/agent.md'), 'Research', ['research-ab12'])).toBe(true);
    expect(notes('../../alpha/agent.md', 'alpha')).toBe(false);
    expect(notes('../beta/agent.md', 'alpha')).toBe(false);
    expect(notes('../agent.md', 'alpha')).toBe(false);
    expect(notes('../alpha/agent.md')).toBe(false);
    // A `__wsref__` path climbs out of that workspace's folder.
    expect(isAgentNotesPath(parseAgentHref('__wsref__/ws-7/../alpha/agent.md'), 'alpha')).toBe(false);
  });
});

describe('computeAgentArtifactRouting: a renamed workspace', () => {
  const route = (raw: string) => computeAgentArtifactRouting(raw, undefined, own('Research', ['research-ab12', 'older']));

  it('folds a path an older turn wrote under a former folder', () => {
    expect(route('/home/workspace/research-ab12/report.md')).toMatchObject({ targetFile: 'report.md' });
    expect(route('file:///home/workspace/older/results/q3.csv')).toMatchObject({ targetFile: 'results/q3.csv' });
    expect(route('/home/workspace/research-ab12/results/')).toMatchObject({ targetFile: null, targetDirectory: 'results' });
    expect(route('/home/workspace/research-ab12/')).toMatchObject({ targetFile: null, targetDirectory: '' });
  });

  it('still folds the current folder', () => {
    expect(route('/home/workspace/Research/report.md')).toMatchObject({ targetFile: 'report.md' });
  });

  it('routes a former folder\'s store paths to their own tabs', () => {
    expect(route(`/home/workspace/research-ab12/${MEMORY_WORKSPACE_DIR}/risk.md`)).toMatchObject({
      targetMemoryKey: 'risk.md',
      targetMemoryTier: 'workspace',
    });
  });

  it('leaves relative paths and other folders alone', () => {
    expect(route('research-ab12/report.md')).toMatchObject({ targetFile: 'research-ab12/report.md' });
    expect(route('/home/workspace/other/report.md')).toMatchObject({ targetFile: '/home/workspace/other/report.md' });
  });

  it('folds one folder, not a subfolder that shares a former name', () => {
    expect(route('/home/workspace/Research/research-ab12/x.md')).toMatchObject({ targetFile: 'research-ab12/x.md' });
  });

  it('matches a former folder by its name key and the current one exactly, as the server does', () => {
    expect(route('/home/workspace/RESEARCH-AB12/report.md')).toMatchObject({ targetFile: 'report.md' });
    expect(route('/home/workspace/RESEARCH/report.md')).toMatchObject({ targetFile: '/home/workspace/RESEARCH/report.md' });
    const renamed = (raw: string) => computeAgentArtifactRouting(raw, undefined, own('Weg', ['Straße']));
    expect(renamed('/home/workspace/STRASSE/plan.md')).toMatchObject({ targetFile: 'plan.md' });
  });

  it('keeps a folder casefold tells apart from the former one', () => {
    const dotless = (raw: string) => computeAgentArtifactRouting(raw, undefined, own('Now', ['\u0131']));
    expect(dotless('/home/workspace/\u0131/report.md')).toMatchObject({ targetFile: 'report.md' });
    expect(dotless('/home/workspace/i/report.md')).toMatchObject({ targetFile: '/home/workspace/i/report.md' });
    expect(dotless('/home/workspace/I/report.md')).toMatchObject({ targetFile: '/home/workspace/I/report.md' });
    const cherokee = (raw: string) => computeAgentArtifactRouting(raw, undefined, own('Now', ['\u13a0']));
    expect(cherokee('/home/workspace/\uab70/report.md')).toMatchObject({ targetFile: 'report.md' });
  });
});

/**
 * Workspaces on one computer are folders side by side, so the agent names a
 * sibling's files through its folder. Those paths open in the sibling, as a
 * `__wsref__` link to it does; everything else reads as it did before.
 */
describe('folderWorkspacePath', () => {
  const siblings: FolderWorkspace[] = [
    { workspaceId: 'ws-nvda', dirName: 'NVDA', previousDirNames: ['nvidia-ab12'] },
    { workspaceId: 'ws-home', dirName: 'Home' },
    // TSLA left `Macro` and Macro now holds it: the current name wins.
    { workspaceId: 'ws-tsla', dirName: 'TSLA', previousDirNames: ['Macro'] },
    { workspaceId: 'ws-macro', dirName: 'Macro' },
  ];
  const sib = (raw: string, { dir, previous }: { dir?: string; previous?: string[] } = {}) =>
    folderWorkspacePath(parseAgentPath(raw), own(dir ?? 'AAPL', previous ?? ['apple-old'], siblings));

  it('reads one climb out of the workspace as the folder beside it', () => {
    expect(sib('../NVDA/results/report.md')).toEqual({ workspaceId: 'ws-nvda', path: 'results/report.md' });
    expect(sib('../Home/notes.md')).toEqual({ workspaceId: 'ws-home', path: 'notes.md' });
    expect(sib('./../NVDA/report.md')).toEqual({ workspaceId: 'ws-nvda', path: 'report.md' });
  });

  it('reads a sandbox-rooted folder that is not this workspace\'s', () => {
    expect(sib('/home/workspace/NVDA/report.md')).toEqual({ workspaceId: 'ws-nvda', path: 'report.md' });
    expect(sib('/home/daytona/NVDA/charts/a.png')).toEqual({ workspaceId: 'ws-nvda', path: 'charts/a.png' });
  });

  it('unwraps file:// on either form', () => {
    expect(sib('file:///home/workspace/NVDA/report.md')).toEqual({ workspaceId: 'ws-nvda', path: 'report.md' });
    expect(folderWorkspacePath(parseAgentHref('file:///home/workspace/NVDA/a%20b.md'), own('AAPL', null, siblings)))
      .toEqual({ workspaceId: 'ws-nvda', path: 'a b.md' });
  });

  it('matches a sibling\'s former folder by its name key', () => {
    expect(sib('../NVIDIA-AB12/report.md')).toEqual({ workspaceId: 'ws-nvda', path: 'report.md' });
    expect(sib('/home/workspace/nvidia-ab12/report.md')).toEqual({ workspaceId: 'ws-nvda', path: 'report.md' });
  });

  it('matches a current folder exactly, as a case-sensitive disk does', () => {
    expect(sib('../nvda/report.md')).toBeNull();
  });

  it('puts a current folder ahead of a former one on another workspace', () => {
    expect(sib('../Macro/report.md')).toEqual({ workspaceId: 'ws-macro', path: 'report.md' });
    expect(sib('../MACRO/report.md')).toEqual({ workspaceId: 'ws-tsla', path: 'report.md' });
  });

  it('keeps this workspace\'s own folders, current and former', () => {
    expect(sib('/home/workspace/AAPL/report.md')).toBeNull();
    expect(sib('../AAPL/report.md')).toBeNull();
    expect(sib('/home/workspace/APPLE-OLD/report.md')).toBeNull();
  });

  it('leaves a folder no sibling holds to the usual reading', () => {
    expect(sib('../MSFT/report.md')).toBeNull();
    expect(sib('/home/workspace/MSFT/report.md')).toBeNull();
  });

  it('reads a bare folder name as a path inside this workspace', () => {
    expect(sib('NVDA/report.md')).toBeNull();
    expect(sib('./NVDA/report.md')).toBeNull();
  });

  it('does not climb past the computer root, or out of an unclaimed root', () => {
    expect(sib('../../NVDA/report.md')).toBeNull();
    expect(sib('/tmp/NVDA/report.md')).toBeNull();
    expect(sib('../report.md')).toBeNull();
  });

  it('lets a __wsref__ link name its workspace outright', () => {
    expect(sib('__wsref__/ws-other/../NVDA/report.md')).toBeNull();
    expect(sib('__wsref__/ws-other/NVDA/report.md')).toBeNull();
  });

  it('keeps a directory link a directory, and reads the folder itself as its root', () => {
    expect(sib('../NVDA/data/')).toEqual({ workspaceId: 'ws-nvda', path: 'data/' });
    expect(sib('../NVDA/')).toEqual({ workspaceId: 'ws-nvda', path: './' });
    expect(sib('/home/workspace/NVDA')).toEqual({ workspaceId: 'ws-nvda', path: './' });
  });

  it('gives up on a former folder two siblings left', () => {
    const shared = [
      { workspaceId: 'ws-a', dirName: 'A', previousDirNames: ['Old'] },
      { workspaceId: 'ws-b', dirName: 'B', previousDirNames: ['old'] },
    ];
    expect(folderWorkspacePath(parseAgentPath('../Old/x.md'), own('C', null, shared))).toBeNull();
  });

  it('defers to this workspace\'s own former folder over a sibling\'s', () => {
    expect(sib('../nvidia-ab12/x.md', { previous: ['NVIDIA-AB12'] })).toBeNull();
  });

  it('finds nothing without siblings', () => {
    expect(folderWorkspacePath(parseAgentPath('../NVDA/x.md'), own('AAPL'))).toBeNull();
    expect(folderWorkspacePath(parseAgentPath('../NVDA/x.md'), null)).toBeNull();
  });
});

describe('computerFolders', () => {
  const rows = [
    { workspace_id: 'ws-aapl', computer_id: 'c1', dir_name: 'AAPL', previous_dir_names: ['apple-old'] },
    { workspace_id: 'ws-nvda', computer_id: 'c1', dir_name: 'NVDA', previous_dir_names: ['nvidia-ab12'] },
    { workspace_id: 'ws-home', computer_id: 'c1', dir_name: 'Home', is_home: true, status: 'running' },
    { workspace_id: 'ws-gone', computer_id: 'c1', dir_name: 'Gone', status: 'deleted' },
    { workspace_id: 'ws-new', computer_id: 'c1', dir_name: null },
    { workspace_id: 'ws-msft', computer_id: 'c2', dir_name: 'MSFT' },
  ];

  it('lists the live workspaces on the viewed one\'s computer, Home included', () => {
    expect(computerFolders(rows[0], rows)).toEqual({
      dirName: 'AAPL',
      previousDirNames: ['apple-old'],
      siblings: [
        { workspaceId: 'ws-nvda', dirName: 'NVDA', previousDirNames: ['nvidia-ab12'] },
        { workspaceId: 'ws-home', dirName: 'Home', previousDirNames: undefined },
      ],
    });
  });

  it('ignores a workspace on another computer', () => {
    const folders = computerFolders(rows[0], rows);
    expect(folders?.siblings.map((s) => s.workspaceId)).not.toContain('ws-msft');
    expect(folderWorkspacePath(parseAgentPath('../MSFT/x.md'), folders)).toBeNull();
  });

  it('gives a workspace on no computer its own folders and no siblings', () => {
    const loose = [{ workspace_id: 'ws-y', computer_id: null, dir_name: 'Y' }];
    expect(computerFolders({ workspace_id: 'ws-x', computer_id: null, dir_name: 'X' }, [...rows, ...loose]))
      .toEqual({ dirName: 'X', previousDirNames: undefined, siblings: [] });
  });

  it('has nothing until the viewed row is known', () => {
    expect(computerFolders(undefined, rows)).toBeNull();
  });

  it('carries the computer root when it is known', () => {
    expect(computerFolders(rows[0], rows, [{ computer_id: 'c1', root_dir: '/home/someone/langalpha' }])?.rootDir)
      .toBe('/home/someone/langalpha');
    expect(computerFolders(rows[0], rows)).not.toHaveProperty('rootDir');
  });
});

/**
 * A computer whose user cannot create the stock root keeps its workspace
 * folders under a root of its own, and the agent writes absolute paths there.
 * Given that root, those paths read exactly as the stock root's do.
 */
describe('a computer root of its own', () => {
  const ROOT = '/home/someone/langalpha';
  const siblings: FolderWorkspace[] = [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }];
  const folders: ComputerFolders = { rootDir: ROOT, dirName: 'AAPL', previousDirNames: ['apple-old'], siblings };

  it('strips it like the stock root, and only when given', () => {
    expect(normalizeAgentPath(`${ROOT}/AAPL/report.md`, ROOT)).toBe('AAPL/report.md');
    expect(normalizeAgentPath(`file://${ROOT}/x.md`, ROOT)).toBe('x.md');
    expect(normalizeAgentPath(ROOT, ROOT)).toBe('');
    expect(normalizeAgentPath(`${ROOT}/x.md`)).toBe(`${ROOT}/x.md`);
    // The stock roots still read as roots beside it.
    expect(normalizeAgentPath('/home/workspace/x.md', ROOT)).toBe('x.md');
    // A sibling directory that merely shares the prefix is not the root.
    expect(normalizeAgentPath(`${ROOT}-old/x.md`, ROOT)).toBe(`${ROOT}-old/x.md`);
  });

  it('keeps a relative path that merely starts like a one-segment root', () => {
    expect(normalizeAgentPath('data/x.csv', '/data')).toBe('data/x.csv');
    expect(normalizeAgentPath('/data/ws/x.csv', '/data')).toBe('ws/x.csv');
    expect(normalizeAgentPath('file:///data/ws/x.csv', '/data')).toBe('ws/x.csv');
  });

  it('reads a root with percent-encoded characters in a markdown destination', () => {
    expect(normalizeAgentHref('/my%20data/ws/x.csv', '/my data')).toBe('ws/x.csv');
  });

  it('routes a path through the own folder from the workspace root', () => {
    expect(computeAgentArtifactRouting(`${ROOT}/AAPL/results/q3.csv`, undefined, folders))
      .toMatchObject({ targetFile: 'results/q3.csv' });
    expect(computeAgentArtifactRouting(`${ROOT}/apple-old/report.md`, undefined, folders))
      .toMatchObject({ targetFile: 'report.md' });
    expect(computeAgentArtifactRouting(`${ROOT}/AAPL/${MEMORY_WORKSPACE_DIR}/risk.md`, undefined, folders))
      .toMatchObject({ targetMemoryKey: 'risk.md', targetMemoryTier: 'workspace' });
  });

  it('opens a sibling\'s folder in that workspace', () => {
    expect(folderWorkspacePath(parseAgentHref(`${ROOT}/NVDA/a%20b.md`, ROOT), folders))
      .toEqual({ workspaceId: 'ws-nvda', path: 'a b.md' });
    expect(computeAgentArtifactRouting(`${ROOT}/NVDA/report.md`, undefined, folders))
      .toMatchObject({ targetFile: 'report.md', setWorkspaceId: 'ws-nvda' });
  });

  it('classifies the stores under it', () => {
    expect(classifyAgentPath(`${ROOT}/.agents/user/memory/style.md`, ROOT)).toMatchObject({ kind: 'memory', tier: 'user' });
    expect(isSubagentFilePath(`${ROOT}/.agents/user/agents/critic.md`, ROOT)).toBe(true);
    expect(workspaceRelativePath(`${ROOT}/AAPL/x.md`, ROOT)).toBe('AAPL/x.md');
  });
});

/**
 * A workspace moves between computers with its folder, so a transcript can
 * hold absolute paths under the root of a computer it has left. Through its
 * folder they are still its files; the rest of that machine is not here.
 */
describe('a root the workspace has moved off', () => {
  const OLD = '/home/dev/langalpha';
  const folders: ComputerFolders = {
    dirName: 'AAPL',
    previousDirNames: ['apple-old'],
    otherRoots: [OLD],
    siblings: [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }],
  };

  it('lists the other computers\' own roots, sorted, without this one\'s or a stock one', () => {
    // The viewed workspace is on the fourth, `c-3`.
    const roots = ['/srv/b/', OLD, '/home/workspace', '/srv/here', OLD];
    expect(computerFolders({ workspace_id: 'ws-1', computer_id: 'c-3', dir_name: 'AAPL' }, [], machines(...roots))?.otherRoots)
      .toEqual([OLD, '/srv/b']);
    expect(computerFolders({ workspace_id: 'ws-1', computer_id: 'c-0', dir_name: 'AAPL' }, [], machines('/home/workspace')))
      .not.toHaveProperty('otherRoots');
  });

  it('keeps a root the row recorded leaving after its computer is gone from the list', () => {
    const viewed = { workspace_id: 'ws-1', computer_id: 'c-0', dir_name: 'AAPL', previous_root_dirs: [OLD, '/home/workspace'] };
    const read = computerFolders(viewed, [], machines('/srv/here'));
    expect(read?.otherRoots).toEqual([OLD]);
    expect(computeAgentArtifactRouting(`${OLD}/AAPL/results/q3.csv`, undefined, read))
      .toMatchObject({ targetFile: 'results/q3.csv', setWorkspaceId: null });
  });

  it('reads no other workspace\'s former roots', () => {
    // A sibling that left a computer has its folder under that root, not this
    // workspace's, so its record must not fold a path there into this one.
    const viewed = { workspace_id: 'ws-1', computer_id: 'c1', dir_name: 'AAPL' };
    const sibling = { workspace_id: 'ws-2', computer_id: 'c1', dir_name: 'AAPL-2', previous_root_dirs: [OLD] };
    const read = computerFolders(viewed, [viewed, sibling], [{ computer_id: 'c1', root_dir: '/srv/here' }]);
    expect(read).not.toHaveProperty('otherRoots');
    expect(computeAgentArtifactRouting(`${OLD}/AAPL/x.md`, undefined, read)).toMatchObject({ targetFile: `${OLD}/AAPL/x.md` });
  });

  it('reads its own root off the row before the computer list answers', () => {
    const viewed = { workspace_id: 'ws-1', computer_id: 'c-0', dir_name: 'AAPL', root_dir: '/srv/here' };
    expect(computerFolders(viewed, [])?.rootDir).toBe('/srv/here');
    // A row read without one, the answer to a change, takes the list's.
    expect(computerFolders({ ...viewed, root_dir: null }, [], machines('/srv/here'))?.rootDir).toBe('/srv/here');
    // And the own root is never listed as another one.
    expect(computerFolders(viewed, [], machines('/srv/here', OLD))?.otherRoots).toEqual([OLD]);
  });

  it('routes a path through its current or former folder into this workspace', () => {
    expect(computeAgentArtifactRouting(`${OLD}/AAPL/results/q3.csv`, undefined, folders))
      .toMatchObject({ targetFile: 'results/q3.csv', setWorkspaceId: null });
    expect(computeAgentArtifactRouting(`${OLD}/apple-old/report.md`, undefined, folders))
      .toMatchObject({ targetFile: 'report.md' });
    expect(computeAgentArtifactRouting(`${OLD}/AAPL/${MEMORY_WORKSPACE_DIR}/risk.md`, undefined, folders))
      .toMatchObject({ targetMemoryKey: 'risk.md', targetMemoryTier: 'workspace' });
    expect(workspaceScopedPath(parseAgentHref(`${OLD}/AAPL/a%20b.md`), 'AAPL', null, [OLD])).toBe('a b.md');
  });

  it('leaves the rest of that machine where it is', () => {
    expect(computeAgentArtifactRouting(`${OLD}/NVDA/x.md`, undefined, folders))
      .toMatchObject({ targetFile: `${OLD}/NVDA/x.md`, setWorkspaceId: null });
    expect(computeAgentArtifactRouting(`${OLD}/x.md`, undefined, folders)).toMatchObject({ targetFile: `${OLD}/x.md` });
    expect(computeAgentArtifactRouting(`${OLD}-2/AAPL/x.md`, undefined, folders)).toMatchObject({ targetFile: `${OLD}-2/AAPL/x.md` });
    expect(computeAgentArtifactRouting(`${OLD}/AAPL/x.md`, undefined, { ...folders, otherRoots: undefined }))
      .toMatchObject({ targetFile: `${OLD}/AAPL/x.md` });
  });

  it('tries each root a path starts under', () => {
    const nested = ['/home/dev', OLD];
    expect(workspaceScopedPath(parseAgentPath(`${OLD}/AAPL/x.md`), 'AAPL', null, nested)).toBe('x.md');
    expect(workspaceScopedPath(parseAgentPath('/home/dev/AAPL/x.md'), 'AAPL', null, nested)).toBe('x.md');
  });

  it('reads a path under two roots by the longer one first', () => {
    // Under `/srv/old` through a former folder, before `/srv` through the current one.
    const scoped = (path: string) => workspaceScopedPath(parseAgentPath(path), 'old', ['Research'], ['/srv', '/srv/old']);
    expect(scoped('/srv/old/Research/a.md')).toBe('a.md');
    expect(scoped('/srv/old/notes/a.md')).toBe('notes/a.md');
  });

  it('reads a root it left inside the one it is on now', () => {
    // Moved from `~/langalpha` to `~`: this computer's root takes the path first.
    const viewed = { workspace_id: 'ws-1', dir_name: 'AAPL', root_dir: '/home/dev', previous_root_dirs: [OLD] };
    const read = computerFolders(viewed, []);
    expect(computeAgentArtifactRouting(`${OLD}/AAPL/results/q3.csv`, undefined, read))
      .toMatchObject({ targetFile: 'results/q3.csv' });
    expect(computeAgentArtifactRouting('/home/dev/AAPL/x.md', undefined, read)).toMatchObject({ targetFile: 'x.md' });
    expect(workspaceScopedPath(parseAgentHref(`file://${OLD}/AAPL/a%20b.md`, '/home/dev'), 'AAPL', null, [OLD]))
      .toBe('a b.md');
    // Through no folder of this workspace's, it stays as this computer reads it.
    expect(computeAgentArtifactRouting(`${OLD}/NVDA/x.md`, undefined, read))
      .toMatchObject({ targetFile: `${OLD}/NVDA/x.md` });
  });
});

/**
 * The Chief of Staff reads the user's other computers and links a file there
 * at its whole path on that machine. Through a workspace's folder, that path
 * opens in that workspace, as a sibling's does; the rest of the machine does
 * not, and this computer's own folders read exactly as before.
 */
describe('a workspace on another computer', () => {
  const SSH = '/home/nosudo/langalpha-2';
  const rows = [
    { workspace_id: 'ws-home', computer_id: 'c-home', dir_name: 'Home', root_dir: '/home/workspace', status: 'running' },
    { workspace_id: 'ws-nvda', computer_id: 'c-home', dir_name: 'NVDA', root_dir: '/home/workspace' },
    { workspace_id: 'ws-ssh', computer_id: 'c-ssh', dir_name: 'ssh-e2e-d01c7a', previous_dir_names: ['ssh-old'], root_dir: SSH },
    { workspace_id: 'ws-gone', computer_id: 'c-ssh', dir_name: 'Gone', root_dir: SSH, status: 'deleted' },
    // A row read without its root takes the computer list's.
    { workspace_id: 'ws-semis', computer_id: 'c-ws2', dir_name: 'Semis', root_dir: null },
    { workspace_id: 'ws-cloud', computer_id: 'c-cloud', dir_name: 'Cloud', previous_dir_names: ['cloud-old'], root_dir: '/home/daytona/' },
    // On a computer the list does not have yet, so its root is unknown.
    { workspace_id: 'ws-new', computer_id: 'c-new', dir_name: 'Fresh', root_dir: null },
  ];
  const computers = [
    { computer_id: 'c-home', root_dir: '/home/workspace' },
    { computer_id: 'c-ssh', root_dir: SSH },
    { computer_id: 'c-ws2', root_dir: '/home/workspace-2/' },
    { computer_id: 'c-cloud', root_dir: '/home/daytona' },
  ];
  const home = computerFolders(rows[0], rows, computers);
  const route = (raw: string, folders = home) => computeAgentArtifactRouting(raw, undefined, folders);
  const withoutOthers = { ...home!, otherComputers: undefined };

  it('groups the live ones by root, a stock root as one, and skips a computer with no known root', () => {
    expect(home?.siblings).toEqual([{ workspaceId: 'ws-nvda', dirName: 'NVDA', previousDirNames: undefined }]);
    expect(home?.otherComputers).toEqual([
      { root: '', workspaces: [{ workspaceId: 'ws-cloud', dirName: 'Cloud', previousDirNames: ['cloud-old'] }] },
      { root: SSH, workspaces: [{ workspaceId: 'ws-ssh', dirName: 'ssh-e2e-d01c7a', previousDirNames: ['ssh-old'] }] },
      { root: '/home/workspace-2', workspaces: [{ workspaceId: 'ws-semis', dirName: 'Semis', previousDirNames: undefined }] },
    ]);
    expect(home?.otherRoots).toEqual([SSH, '/home/workspace-2']);
    // A workspace on no computer has nothing to read a path against.
    expect(computerFolders({ ...rows[0], computer_id: null }, rows, computers)).not.toHaveProperty('otherComputers');
  });

  it('opens a file through its folder there in that workspace, as a __wsref__ link would', () => {
    const wsref = computeAgentArtifactRouting('agent.md', 'ws-ssh');
    for (const raw of [`${SSH}/ssh-e2e-d01c7a/agent.md`, `file://${SSH}/ssh-e2e-d01c7a/agent.md`, `${SSH}//ssh-e2e-d01c7a/./agent.md`]) {
      expect(route(raw)).toEqual(wsref);
    }
    expect(route(`${SSH}/ssh-e2e-d01c7a/agent.md`)).toMatchObject({ targetFile: 'agent.md', setWorkspaceId: 'ws-ssh' });
    expect(folderWorkspacePath(parseAgentHref(`${SSH}/ssh-e2e-d01c7a/a%20b.md`), home))
      .toEqual({ workspaceId: 'ws-ssh', path: 'a b.md' });
    expect(route(`${SSH}/ssh-e2e-d01c7a/data/`)).toMatchObject({ targetDirectory: 'data', setWorkspaceId: 'ws-ssh' });
    expect(route(`${SSH}/ssh-e2e-d01c7a`)).toMatchObject({ targetDirectory: '', setWorkspaceId: 'ws-ssh' });
    expect(route('/home/workspace-2/Semis/q3.csv')).toMatchObject({ targetFile: 'q3.csv', setWorkspaceId: 'ws-semis' });
  });

  it('reaches a former folder there by its name key, and a current one exactly', () => {
    expect(route(`${SSH}/SSH-OLD/x.md`)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-ssh' });
    expect(route(`${SSH}/SSH-E2E-D01C7A/x.md`)).toMatchObject({ targetFile: `${SSH}/SSH-E2E-D01C7A/x.md`, setWorkspaceId: null });
  });

  it('reads a stock root on another computer after every folder on this one', () => {
    expect(route('/home/workspace/Cloud/x.md')).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-cloud' });
    expect(route('/home/daytona/cloud-old/x.md')).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-cloud' });
    // This computer's sibling and this workspace's own folder win the stock root.
    const shadowed = computerFolders(rows[0], [...rows, { workspace_id: 'ws-dup', computer_id: 'c-cloud', dir_name: 'X', previous_dir_names: ['NVDA', 'Home'] }], computers);
    expect(route('/home/workspace/NVDA/x.md', shadowed)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-nvda' });
    expect(route('/home/workspace/Home/x.md', shadowed)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
  });

  it('leaves a path through no workspace\'s folder where it was', () => {
    for (const raw of [
      `${SSH}/x.md`,
      `${SSH}/Other/x.md`,
      `${SSH}/Gone/x.md`,
      '/Users/me/Documents/x.pdf',
      '/home/workspace/Elsewhere/x.md',
      '/home/new-root/Fresh/x.md',
    ]) {
      expect(route(raw)).toEqual(route(raw, withoutOthers));
    }
    expect(route('/Users/me/Documents/x.pdf')).toMatchObject({ targetFile: '/Users/me/Documents/x.pdf', setWorkspaceId: null });
  });

  it('marks the rest of that machine as one this view does not read', () => {
    const elsewhere = (raw: string, folders = home) => onOtherComputer(parseAgentPath(raw, folders?.rootDir), folders);
    expect(elsewhere(`${SSH}/x.md`)).toBe(true);
    expect(elsewhere(`${SSH}/Other/x.md`)).toBe(true);
    expect(elsewhere(SSH)).toBe(true);
    expect(elsewhere(`${SSH}2/x.md`)).toBe(false);
    expect(elsewhere('/Users/me/Documents/x.pdf')).toBe(false);
    expect(elsewhere('x.md')).toBe(false);
    expect(elsewhere(`${SSH}/x.md`, withoutOthers)).toBe(false);
  });

  it('marks the rest of a computer whose root sits inside this one\'s, and nothing of one sharing it', () => {
    const MAC = '/Users/me';
    const mac = computerFolders({ workspace_id: 'ws-mac', computer_id: 'c-mac', dir_name: 'Mac', root_dir: MAC }, [
      { workspace_id: 'ws-mac', computer_id: 'c-mac', dir_name: 'Mac', root_dir: MAC },
      { workspace_id: 'ws-twin', computer_id: 'c-twin', dir_name: 'Twin', root_dir: MAC },
      { workspace_id: 'ws-deep', computer_id: 'c-deep', dir_name: 'Deep', root_dir: `${MAC}/langalpha` },
    ], []);
    const elsewhere = (raw: string) => onOtherComputer(parseAgentPath(raw, mac?.rootDir), mac);
    expect(elsewhere(`${MAC}/langalpha/scratch/x.csv`)).toBe(true);
    expect(elsewhere(`${MAC}/langalpha`)).toBe(true);
    expect(elsewhere(MAC)).toBe(false);
    expect(elsewhere(`${MAC}/scratch/x.csv`)).toBe(false);
    expect(elsewhere(`${MAC}/Mac/x.csv`)).toBe(false);
  });

  it('marks the rest of a computer with no workspace on it yet', () => {
    const bare = computerFolders(rows[0], rows, [...computers, { computer_id: 'c-bare', root_dir: '/Users/new/langalpha' }]);
    expect(onOtherComputer(parseAgentPath('/Users/new/langalpha/notes.md', bare?.rootDir), bare)).toBe(true);
    expect(onOtherComputer(parseAgentPath('/Users/new/langalpha/notes.md', home?.rootDir), home)).toBe(false);
  });

  it('compares roots by whole segments', () => {
    // `/home/workspace-2` is not the stock root, and `${SSH}2` is not under SSH.
    expect(route('/home/workspace-2/Semis/x.md')).toMatchObject({ setWorkspaceId: 'ws-semis' });
    expect(route('/home/workspace/Semis/x.md')).toEqual(route('/home/workspace/Semis/x.md', withoutOthers));
    expect(route(`${SSH}2/ssh-e2e-d01c7a/x.md`)).toMatchObject({ targetFile: `${SSH}2/ssh-e2e-d01c7a/x.md`, setWorkspaceId: null });
  });

  it('reads this computer\'s folders exactly as before', () => {
    for (const raw of [
      '../NVDA/x.md',
      '/home/workspace/NVDA/x.md',
      '../Home/x.md',
      '/home/workspace/Home/x.md',
      'NVDA/x.md',
      'results/x.md',
      // A climb out of the working directory stays on this computer.
      '../Cloud/x.md',
      '../ssh-e2e-d01c7a/x.md',
      '__wsref__/ws-other/x.md',
    ]) {
      expect(route(raw)).toEqual(route(raw, withoutOthers));
    }
  });

  it('keeps a folder this workspace sat under there before a move', () => {
    const moved = computerFolders({ ...rows[0], previous_root_dirs: [SSH] }, [
      ...rows,
      { workspace_id: 'ws-took', computer_id: 'c-ssh', dir_name: 'Took', previous_dir_names: ['Home'] },
    ], computers);
    expect(route(`${SSH}/Home/x.md`, moved)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
  });

  it('opens a namesake there under a root this workspace never recorded leaving', () => {
    const namesake = [...rows, { workspace_id: 'ws-ssh-nvda', computer_id: 'c-ssh', dir_name: 'NVDA', root_dir: SSH }];
    const nvda = computerFolders(rows[1], namesake, computers);
    expect(route(`${SSH}/NVDA/x.md`, nvda)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-ssh-nvda' });
    const moved = computerFolders({ ...rows[1], previous_root_dirs: [SSH] }, namesake, computers);
    expect(moved?.leftRoots).toEqual([SSH]);
    expect(route(`${SSH}/NVDA/x.md`, moved)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
  });

  it('gives up on a folder two computers on one root both hold', () => {
    const MAC = '/Users/me/langalpha';
    const twoMacs = computerFolders(rows[0], [
      rows[0],
      { workspace_id: 'ws-a', computer_id: 'c-mac1', dir_name: 'Notes', root_dir: MAC },
      { workspace_id: 'ws-b', computer_id: 'c-mac2', dir_name: 'Notes', root_dir: MAC },
      { workspace_id: 'ws-c', computer_id: 'c-mac2', dir_name: 'Only', root_dir: `${MAC}/` },
    ], []);
    expect(twoMacs?.otherComputers?.map((g) => [g.root, g.workspaces.length])).toEqual([[MAC, 3]]);
    expect(route(`${MAC}/Notes/x.md`, twoMacs)).toMatchObject({ targetFile: `${MAC}/Notes/x.md`, setWorkspaceId: null });
    expect(route(`${MAC}/Only/x.md`, twoMacs)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-c' });
  });

  it('reads a path under two nested roots by the longer first', () => {
    const nested = computerFolders(rows[0], [
      rows[0],
      { workspace_id: 'ws-old', computer_id: 'c-srv', dir_name: 'old', root_dir: '/srv' },
      { workspace_id: 'ws-a', computer_id: 'c-inner', dir_name: 'A', root_dir: '/srv/old' },
    ], []);
    expect(route('/srv/old/A/x.md', nested)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-a' });
    expect(route('/srv/old/notes.md', nested)).toMatchObject({ targetFile: 'notes.md', setWorkspaceId: 'ws-old' });
  });

  it('reads from a computer with a root of its own', () => {
    // An analyst on the user's Mac, where Home is on a stock cloud computer, a
    // second Mac shares this one's root, and a third keeps one inside it.
    const MAC = '/Users/me/langalpha';
    const mac = computerFolders({ workspace_id: 'ws-mac', computer_id: 'c-mac', dir_name: 'Mac', root_dir: MAC }, [
      ...rows,
      { workspace_id: 'ws-mac2', computer_id: 'c-mac2', dir_name: 'Mac2', root_dir: MAC },
      { workspace_id: 'ws-deep', computer_id: 'c-deep', dir_name: 'Deep', root_dir: `${MAC}/inner` },
    ], computers);
    expect(route('/home/workspace/Home/x.md', mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-home' });
    expect(route(`${MAC}/Mac2/x.md`, mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-mac2' });
    expect(route(`${MAC}/inner/Deep/x.md`, mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-deep' });
    expect(route(`${MAC}/Mac/x.md`, mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
    // A stock path here is not under this computer's root, nor is its own under the stock one.
    expect(route(`${MAC}/Cloud/x.md`, mac)).toMatchObject({ setWorkspaceId: null });
  });

  it('reads a stock root from a computer with a root of its own as the stock computer\'s first', () => {
    const MAC = '/Users/me/langalpha';
    const viewed = { workspace_id: 'ws-mac', computer_id: 'c-mac', dir_name: 'Mac', root_dir: MAC };
    const mac = computerFolders(viewed, [
      ...rows,
      viewed,
      { workspace_id: 'ws-local', computer_id: 'c-mac', dir_name: 'NVDA', root_dir: MAC },
      { workspace_id: 'ws-only-here', computer_id: 'c-mac', dir_name: 'Local', root_dir: MAC },
    ], computers);
    expect(route('/home/workspace/NVDA/x.md', mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-nvda' });
    expect(route(`${MAC}/NVDA/x.md`, mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-local' });
    // No stock computer holds it, so the old spelling still reads here.
    expect(route('/home/workspace/Local/x.md', mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-only-here' });
    expect(route('/home/workspace/Mac/x.md', mac)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });

    // Through no folder, the stock computer's file opens nothing here, while
    // on a stock computer, or with none elsewhere, it is this one's.
    const elsewhere = (raw: string, folders: ComputerFolders | null) => onOtherComputer(parseAgentPath(raw, folders?.rootDir), folders);
    const noStock = { ...mac!, otherComputers: mac!.otherComputers?.filter(({ root }) => root) };
    expect(elsewhere('/home/workspace/outside.md', mac)).toBe(true);
    expect(elsewhere('/home/workspace/Mac/outside.md', mac)).toBe(false);
    expect(elsewhere('/home/workspace/outside.md', noStock)).toBe(false);
    expect(elsewhere('/home/workspace/outside.md', home)).toBe(false);
  });
});

describe('computeAgentArtifactRouting: a sibling\'s folder', () => {
  const siblings: FolderWorkspace[] = [
    { workspaceId: 'ws-nvda', dirName: 'NVDA', previousDirNames: ['nvidia-ab12'] },
    { workspaceId: 'ws-home', dirName: 'Home' },
    { workspaceId: 'ws-tsla', dirName: 'TSLA', previousDirNames: ['Macro'] },
    { workspaceId: 'ws-macro', dirName: 'Macro' },
  ];
  const route = (raw: string, target?: string) =>
    computeAgentArtifactRouting(raw, target, own('AAPL', ['apple-old'], siblings));

  it('opens the file in the sibling, as a __wsref__ link would', () => {
    // A `__wsref__` link reaches routing as its inner path with the workspace
    // beside it (Markdown's `onOpenFile`), which is the routing to match.
    const wsref = computeAgentArtifactRouting('results/report.md', 'ws-nvda');
    for (const raw of ['../NVDA/results/report.md', '/home/workspace/NVDA/results/report.md', 'file:///home/workspace/NVDA/results/report.md']) {
      expect(route(raw)).toEqual(wsref);
      expect(route(raw)).toMatchObject({ targetFile: 'results/report.md', setWorkspaceId: 'ws-nvda' });
    }
  });

  it('reaches a former folder by its name key, and a current one first', () => {
    expect(route('../NVIDIA-AB12/report.md')).toMatchObject({ targetFile: 'report.md', setWorkspaceId: 'ws-nvda' });
    expect(route('/home/workspace/Macro/report.md')).toMatchObject({ targetFile: 'report.md', setWorkspaceId: 'ws-macro' });
    expect(route('/home/workspace/MACRO/report.md')).toMatchObject({ targetFile: 'report.md', setWorkspaceId: 'ws-tsla' });
  });

  it('opens a directory link in the sibling\'s Files tab', () => {
    expect(route('../NVDA/data/')).toMatchObject({ targetFile: null, targetDirectory: 'data', setWorkspaceId: 'ws-nvda' });
    expect(route('../NVDA/')).toMatchObject({ targetFile: null, targetDirectory: '', setWorkspaceId: 'ws-nvda' });
  });

  it('routes a sibling\'s store paths with that workspace', () => {
    expect(route(`../NVDA/${MEMORY_WORKSPACE_DIR}/risk.md`)).toMatchObject({
      targetMemoryKey: 'risk.md',
      targetMemoryTier: 'workspace',
      setWorkspaceId: 'ws-nvda',
    });
  });

  it('keeps a # inside a name', () => {
    expect(route('../NVDA/issue#1.md')).toMatchObject({ targetFile: 'issue#1.md', setWorkspaceId: 'ws-nvda' });
  });

  it('leaves a path that names no sibling as it read before', () => {
    const before = (raw: string) => computeAgentArtifactRouting(raw, undefined, own('AAPL', ['apple-old']));
    for (const raw of [
      'NVDA/report.md',
      '../MSFT/report.md',
      '/home/workspace/MSFT/report.md',
      '/home/workspace/AAPL/report.md',
      '/home/workspace/apple-old/report.md',
      '../report.md',
      'results/report.md',
    ]) {
      expect(route(raw)).toEqual(before(raw));
    }
  });

  it('lets a __wsref__ link and a caller\'s workspace win', () => {
    const before = (raw: string, target?: string) => computeAgentArtifactRouting(raw, target, own('AAPL', ['apple-old']));
    for (const raw of ['__wsref__/ws-other/../NVDA/report.md', '__wsref__/ws-other/NVDA/report.md', `__wsref__/ws-other/${MEMORY_WORKSPACE_DIR}/a.md`]) {
      expect(route(raw)).toEqual(before(raw));
    }
    expect(route(`__wsref__/ws-other/${MEMORY_WORKSPACE_DIR}/a.md`)).toMatchObject({ setWorkspaceId: 'ws-other' });
    expect(route('../NVDA/report.md', 'ws-caller')).toEqual(before('../NVDA/report.md', 'ws-caller'));
    expect(route('../NVDA/report.md', 'ws-caller')).toMatchObject({ targetFile: '../NVDA/report.md', setWorkspaceId: 'ws-caller' });
  });
});

/**
 * From the working directory, `../<own folder>/x` is `x`: Bash resolves it so,
 * and the agent writes it. Read as written it climbs out of the workspace,
 * which the server refuses.
 */
describe('computeAgentArtifactRouting: a climb back into the workspace\'s own folder', () => {
  const route = (raw: string, folders: ComputerFolders = own('Home')) => computeAgentArtifactRouting(raw, undefined, folders);

  it('routes as the path inside the folder', () => {
    expect(route('../Home/e2e_probe_comparison/probe_notes_comparison.md'))
      .toEqual(computeAgentArtifactRouting('e2e_probe_comparison/probe_notes_comparison.md'));
    expect(route('./../Home/notes.md')).toMatchObject({ targetFile: 'notes.md', setWorkspaceId: null });
    expect(route('reports/../../Home/notes.md')).toMatchObject({ targetFile: 'notes.md' });
  });

  it('opens a folder link in the Files tab, and a store path in its own', () => {
    expect(route('../Home/data/')).toMatchObject({ targetFile: null, targetDirectory: 'data' });
    expect(route('../Home/')).toMatchObject({ targetFile: null, targetDirectory: '' });
    expect(route(`../Home/${MEMORY_WORKSPACE_DIR}/risk.md`)).toMatchObject({
      targetMemoryKey: 'risk.md',
      targetMemoryTier: 'workspace',
    });
  });

  it('matches a former folder by its name key and the current one exactly', () => {
    const renamed = own('Research', ['research-ab12']);
    expect(route('../Research/report.md', renamed)).toMatchObject({ targetFile: 'report.md' });
    expect(route('../RESEARCH-AB12/report.md', renamed)).toMatchObject({ targetFile: 'report.md' });
    expect(route('../RESEARCH/report.md', renamed)).toMatchObject({ targetFile: '../RESEARCH/report.md' });
  });

  it('reads every other climb as it did before', () => {
    for (const raw of [
      '../../Home/notes.md',
      '../notes.md',
      '../Other/notes.md',
      'Home/notes.md',
      '../',
      '__wsref__/ws-other/../Home/notes.md',
    ]) {
      expect(route(raw)).toEqual(computeAgentArtifactRouting(raw));
    }
  });

  it('agrees with the sibling match on whose folder a name is', () => {
    const folders = own('Home', ['Macro', 'shared'], [
      // Took `Macro` after Home left it.
      { workspaceId: 'ws-macro', dirName: 'Macro' },
      { workspaceId: 'ws-tsla', dirName: 'TSLA', previousDirNames: ['old-tsla', 'Home'] },
      { workspaceId: 'ws-b', dirName: 'B', previousDirNames: ['Shared'] },
    ]);
    // This workspace's current folder, then a sibling's current one, then this
    // workspace's former ones, then a former one a single sibling left.
    expect(route('../Home/x.md', folders)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
    expect(route('../Macro/x.md', folders)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-macro' });
    expect(route('../MACRO/x.md', folders)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
    expect(route('../SHARED/x.md', folders)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: null });
    expect(route('../OLD-TSLA/x.md', folders)).toMatchObject({ targetFile: 'x.md', setWorkspaceId: 'ws-tsla' });
  });
});
