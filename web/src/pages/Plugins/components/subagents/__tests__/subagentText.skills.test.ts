import { describe, expect, it } from 'vitest';
import type { SkillInfo } from '@/pages/ChatAgent/utils/api';
import { preferredSkillOptions } from '../subagentText';

/**
 * A skill switched off, itself or with its plugin, stays in the list marked
 * off, so a chip that names it can say so rather than read as active.
 */

type Row = Pick<SkillInfo, 'name' | 'command' | 'enabled' | 'plugin_enabled' | 'workspace_id'>;
const row = (over: Partial<Row> & { name: string }): Row => ({
  command: null,
  enabled: true,
  plugin_enabled: null,
  workspace_id: null,
  ...over,
});

describe('preferredSkillOptions', () => {
  it('keeps a switched-off skill and marks it off, by its own switch or its plugin', () => {
    expect(
      preferredSkillOptions([
        row({ name: 'charts', command: 'chart' }),
        row({ name: 'filings', command: 'sec', enabled: false }),
        row({ name: 'bundle-skill', plugin_enabled: false }),
      ]),
    ).toEqual([
      { name: 'bundle-skill', command: null, off: true },
      { name: 'charts', command: 'chart', off: false },
      { name: 'filings', command: 'sec', off: true },
    ]);
  });

  it("leaves out one workspace's own skills", () => {
    expect(preferredSkillOptions([row({ name: 'local-only', workspace_id: 'ws-1' })])).toEqual([]);
  });

  it('reads a name listed twice as the row that is on', () => {
    const on = row({ name: 'charts', command: 'chart' });
    const off = row({ name: 'charts', command: 'old-chart', enabled: false });
    expect(preferredSkillOptions([off, on])).toEqual([{ name: 'charts', command: 'chart', off: false }]);
    expect(preferredSkillOptions([on, off])).toEqual([{ name: 'charts', command: 'chart', off: false }]);
  });
});
