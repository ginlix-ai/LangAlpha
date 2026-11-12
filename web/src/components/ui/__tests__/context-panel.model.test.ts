// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { barSegments, contextRows } from '../context-panel.model';

const sum = (rows: { tokens: number }[]) => rows.reduce((acc, { tokens }) => acc + tokens, 0);

describe('contextRows', () => {
  it('gives the conversation the rest of the window, so the rows sum to it', () => {
    const rows = contextRows(
      { system: 13_100, tools: 16_100, mcp: 2_000, skills: 900, memory: 400, environment: 600 },
      { total: 144_400, lastInput: 140_000 },
    );
    expect(rows.map((r) => r.key)).toEqual(['system', 'tools', 'mcp', 'skills', 'memory', 'environment', 'conversation']);
    expect(rows.at(-1)).toEqual({ key: 'conversation', tokens: 144_400 - 33_100 });
    expect(sum(rows)).toBe(144_400);
  });

  it('keeps its order whatever order the parts arrive in', () => {
    const rows = contextRows({ environment: 5, system: 10 }, { total: 100, lastInput: 90 });
    expect(rows).toEqual([
      { key: 'system', tokens: 10 },
      { key: 'environment', tokens: 5 },
      { key: 'conversation', tokens: 85 },
    ]);
  });

  it('scales the parts down together when they outgrow the newest call', () => {
    const rows = contextRows({ system: 600, tools: 300, skills: 101 }, { total: 520, lastInput: 500 });
    // 1001 counted against 500 read: each floored at about half.
    expect(rows).toEqual([
      { key: 'system', tokens: 299 },
      { key: 'tools', tokens: 149 },
      { key: 'skills', tokens: 50 },
      { key: 'conversation', tokens: 22 },
    ]);
    expect(sum(rows.slice(0, -1))).toBeLessThanOrEqual(500);
    expect(sum(rows)).toBe(520);
  });

  it('drops rows with nothing in them, the conversation included', () => {
    const rows = contextRows({ system: 400, tools: 0 }, { total: 400, lastInput: 400 });
    expect(rows).toEqual([{ key: 'system', tokens: 400 }]);
  });

  it('caps at the window when no call has been read yet', () => {
    const rows = contextRows({ system: 300, tools: 300 }, { total: 300, lastInput: 0 });
    expect(sum(rows)).toBe(300);
    expect(rows.map((r) => r.key)).toEqual(['system', 'tools']);
  });
});

describe('barSegments', () => {
  it('gives each row its share of the threshold, cut off where the bar ends', () => {
    const segments = barSegments(
      [{ key: 'system', tokens: 100 }, { key: 'tools', tokens: 300 }, { key: 'conversation', tokens: 800 }],
      1000,
    );
    expect(segments.map((s) => s.key)).toEqual(['system', 'tools', 'conversation']);
    expect(segments[0].width).toBeCloseTo(0.1);
    expect(segments[1].width).toBeCloseTo(0.3);
    expect(segments[2].width).toBeCloseTo(0.6);
  });

  it('is empty with no threshold', () => {
    expect(barSegments([{ key: 'system', tokens: 100 }], 0)).toEqual([]);
  });
});
