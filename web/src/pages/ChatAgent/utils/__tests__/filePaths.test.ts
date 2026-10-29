// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { isFilePath, normalizeFilePath, parseWsPath } from '../filePaths';

describe('normalizeFilePath', () => {
  it('returns ASCII paths unchanged', () => {
    expect(normalizeFilePath('results/amd_dcf_analysis.md')).toBe('results/amd_dcf_analysis.md');
  });

  it('returns raw Unicode paths unchanged', () => {
    expect(normalizeFilePath('results/示例_分析.md')).toBe('results/示例_分析.md');
  });

  it('decodes percent-encoded CJK paths emitted by the LLM in markdown links', () => {
    // LLM occasionally emits [name](results/%XX%XX...md) — the URL position
    // in HTML/markdown is conventionally percent-encoded for non-ASCII chars.
    // 示例_分析.md → %E7%A4%BA%E4%BE%8B_%E5%88%86%E6%9E%90.md
    const encoded = 'results/%E7%A4%BA%E4%BE%8B_%E5%88%86%E6%9E%90.md';
    expect(normalizeFilePath(encoded)).toBe('results/示例_分析.md');
  });

  it('strips the __wsref__ prefix', () => {
    expect(normalizeFilePath('__wsref__/abc-123/results/report.md')).toBe('results/report.md');
  });

  it('strips __wsref__ and decodes the inner path in one pass', () => {
    // 文件.md → %E6%96%87%E4%BB%B6.md
    const encoded = '__wsref__/abc-123/results/%E6%96%87%E4%BB%B6.md';
    expect(normalizeFilePath(encoded)).toBe('results/文件.md');
  });

  it('falls through unchanged on malformed percent sequences', () => {
    // `100%discount` is a legal filename — invalid as URI escape — decode throws.
    expect(normalizeFilePath('results/100%discount.md')).toBe('results/100%discount.md');
  });
});

describe('parseWsPath', () => {
  it('parses __wsref__/{wsid}/path', () => {
    expect(parseWsPath('__wsref__/ws-1/results/r.md')).toEqual({
      workspaceId: 'ws-1',
      path: 'results/r.md',
    });
  });

  it('returns null for non-wsref paths', () => {
    expect(parseWsPath('results/r.md')).toBeNull();
    expect(parseWsPath(undefined)).toBeNull();
  });
});

describe('isFilePath', () => {
  it('treats relative links as files whatever the extension', () => {
    expect(isFilePath('results/deck.pptx')).toBe(true);
    expect(isFilePath('results/model.xlsm')).toBe(true);
    expect(isFilePath('data/prices.parquet')).toBe(true);
    expect(isFilePath('results/README')).toBe(true);
    expect(isFilePath('report.md#summary')).toBe(true);
  });

  it('treats a workspace-qualified link as a file whatever the extension', () => {
    expect(isFilePath('__wsref__/ws-1/results/deck.pptx')).toBe(true);
  });

  it('keeps URLs, anchors and app routes external', () => {
    expect(isFilePath('https://example.com/report.md')).toBe(false);
    expect(isFilePath('mailto:someone@example.com')).toBe(false);
    expect(isFilePath('//example.com/a.md')).toBe(false);
    expect(isFilePath('#section')).toBe(false);
    expect(isFilePath('?tab=files')).toBe(false);
    expect(isFilePath('www.example.com')).toBe(false);
    expect(isFilePath('/settings')).toBe(false);
  });

  it('accepts a root-absolute path that names a file', () => {
    expect(isFilePath('/tmp/output.csv')).toBe(true);
  });

  it('accepts a folder that starts at the sandbox root, which has no extension to offer', () => {
    // The prompts tell the agent its working directory is `/home/workspace`, so
    // it writes rooted destinations. Asking every rooted one for an extension
    // read a folder as an app route, and the click left the app.
    expect(isFilePath('/home/workspace/data/')).toBe(true);
    expect(isFilePath('/home/daytona/work/q3/')).toBe(true);
    expect(isFilePath('/home/workspace/results/report.md')).toBe(true);
    expect(isFilePath('/home/workspace/')).toBe(true);
  });

  it('still asks every other rooted destination for an extension', () => {
    // The control: only the sandbox root says "this names the workspace".
    expect(isFilePath('/settings')).toBe(false);
    expect(isFilePath('/home/someone/data/')).toBe(false);
    expect(isFilePath('/home/workspaces/data/')).toBe(false);
  });

  it('will not let a query supply the extension a rooted destination lacks', () => {
    // `/search?q=notes.md` is an app route carrying a filename, not a file. The
    // whole destination ends in `.md`, so reading it whole swallowed the click
    // and handed the panel `/search`, which is all that survives normalizing.
    expect(isFilePath('/search?q=notes.md')).toBe(false);
    expect(isFilePath('/api/export?name=report.pdf')).toBe(false);
    // The control: a real rooted file keeps working, query or fragment and all.
    expect(isFilePath('/tmp/output.csv?v=2')).toBe(true);
    expect(isFilePath('/tmp/output.csv#L4')).toBe(true);
    // A `#` in a destination is a fragment, so a name that holds one says `%23`.
    expect(isFilePath('/tmp/issue%231.md')).toBe(true);
  });

  it('accepts a folder through the workspace\'s own folder under a root of its own or one it moved off', () => {
    // These roots keep their slash through the parser, so asking only the
    // stock root's question read the folder as an app route.
    const folders = {
      rootDir: '/home/someone/langalpha',
      otherRoots: ['/srv/old'],
      dirName: 'Workspace',
      previousDirNames: ['research-old'],
      siblings: [],
    };
    expect(isFilePath('/home/someone/langalpha/Workspace/reports/', folders)).toBe(true);
    expect(isFilePath('/srv/old/Workspace/reports/', folders)).toBe(true);
    expect(isFilePath('/srv/old/research-old/reports/', folders)).toBe(true);
    expect(isFilePath('/srv/old/My%20Data/', { ...folders, dirName: 'My Data' })).toBe(true);
    // The rest of that machine is not this workspace's, and without the root
    // the path is any other rooted destination.
    expect(isFilePath('/srv/old/Elsewhere/reports/', folders)).toBe(false);
    expect(isFilePath('/srv/old/', folders)).toBe(false);
    expect(isFilePath('/srv/old/Workspace/reports/', { ...folders, otherRoots: undefined })).toBe(false);
    expect(isFilePath('/srv/old/Workspace/reports/')).toBe(false);
  });

  it('accepts a folder through a workspace\'s folder on another computer', () => {
    const SSH = '/home/nosudo/langalpha-2';
    const folders = {
      otherRoots: [SSH],
      dirName: 'Home',
      siblings: [],
      otherComputers: [{ root: SSH, workspaces: [{ workspaceId: 'ws-ssh', dirName: 'ssh-e2e' }] }],
    };
    expect(isFilePath(`${SSH}/ssh-e2e/reports/`, folders)).toBe(true);
    expect(isFilePath(`${SSH}/ssh-e2e`, folders)).toBe(true);
    // The rest of that machine is a file there too, with an extension or
    // without, so its link is shown as its name and never as an app route.
    expect(isFilePath(`${SSH}/reports/`, folders)).toBe(true);
    expect(isFilePath(`${SSH}/LICENSE`, folders)).toBe(true);
    expect(isFilePath(`${SSH}/ssh-e2e/reports/`, { ...folders, otherComputers: undefined })).toBe(false);
    expect(isFilePath(`${SSH}/LICENSE`, { ...folders, otherComputers: undefined })).toBe(false);
  });

  it('reads a line suffix as a file, not a URL scheme', () => {
    expect(isFilePath('model.py:42')).toBe(true);
    expect(isFilePath('work/code/model.py:40-55')).toBe(true);
    expect(isFilePath('https://example.com/model.py:42')).toBe(false);
    expect(isFilePath('www.example.com/model.py:42')).toBe(false);
  });
});

describe('normalizeFilePath fragments', () => {
  it('drops a fragment or query the link carried', () => {
    expect(normalizeFilePath('results/report.md#risks')).toBe('results/report.md');
    expect(normalizeFilePath('results/report.md?v=2')).toBe('results/report.md');
  });

  it('keeps an encoded # that belongs to the name', () => {
    expect(normalizeFilePath('results/issue%231.md')).toBe('results/issue#1.md');
  });
});
