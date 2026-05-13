import { describe, it, expect } from 'vitest';
import { normalizeFilePath, parseWsPath } from '../FileCard';

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
