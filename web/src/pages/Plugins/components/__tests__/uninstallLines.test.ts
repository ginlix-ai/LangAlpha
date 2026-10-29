/**
 * What an uninstall says it removes, before and after. Subagents are a third
 * kind a plugin carries, so a count that left them out would promise less
 * than the uninstall takes, and the user's changes to them go with them.
 */
import { describe, expect, it } from 'vitest';

import i18n from '@/i18n';
import type { PluginInfo } from '@/pages/ChatAgent/utils/api/plugins';
import type { SubagentView } from '@/pages/ChatAgent/utils/api/subagents';

import { bulkUninstallConfirm, changedSubagents, uninstallLines } from '../pluginsPage/pluginStatus';

const t = i18n.getFixedT('en-US');

const removed = (connectors: number, skills: number, subagents: number, changed = 0) => ({
  connectors,
  skills,
  subagents,
  changedSubagents: changed,
});

describe('uninstallLines', () => {
  it.each([
    [removed(2, 1, 3), 'Removes 2 connectors, 1 skill and 3 subagents from every workspace.'],
    [removed(0, 0, 2), 'Removes 2 subagents from every workspace.'],
    [removed(0, 3, 1), 'Removes 3 skills and 1 subagent from every workspace.'],
    [removed(1, 2, 0), 'Removes 1 connector and 2 skills from every workspace.'],
  ])('names every kind it removes: %o', (counts, line) => {
    expect(uninstallLines(t, counts, [], 'before')).toEqual([line]);
  });

  it('says the user changes to its subagents go too, only when there are some', () => {
    expect(uninstallLines(t, removed(0, 0, 2, 1), [], 'before')).toEqual([
      'Removes 2 subagents from every workspace.',
      'Your changes to its subagents are removed too.',
    ]);
    expect(uninstallLines(t, removed(0, 0, 2, 1), [], 'after')).toEqual([
      'Removed 2 subagents.',
      'Your changes to its subagents were removed too.',
    ]);
    expect(uninstallLines(t, removed(0, 0, 2, 0), [], 'after')).toEqual(['Removed 2 subagents.']);
    // A server that predates plugin subagents reports none removed.
    expect(uninstallLines(t, removed(1, 0, 0, 1), [], 'after')).toEqual(['Removed 1 connector.']);
  });

  it('says "their" when several plugins go at once', () => {
    expect(uninstallLines(t, { ...removed(2, 0, 3, 1), plugins: 2 }, [], 'after')).toEqual([
      'Removed 2 connectors and 3 subagents.',
      'Your changes to their subagents were removed too.',
    ]);
    expect(uninstallLines(t, { ...removed(0, 0, 1, 1), plugins: 1 }, [], 'after')).toEqual([
      'Removed 1 subagent.',
      'Your changes to its subagents were removed too.',
    ]);
  });
});

describe('bulkUninstallConfirm', () => {
  const plugin = (name: string, kinds: PluginInfo['components'][number]['kind'][]) =>
    ({
      name,
      enabled: true,
      components: kinds.map((kind, i) => ({ kind, name: `${kind}-${i}`, key: `${kind}-${i}` })),
    }) as PluginInfo;
  const edited = (from: string) =>
    ({ name: `${from}-agent`, edited: true, source: 'plugin', plugin: { name: from, display_name: from } }) as SubagentView;

  it.each([
    [[plugin('acme', ['mcp']), plugin('ledger', ['skill'])], [], 'Uninstall 2 plugins? Their connectors and skills go with them.'],
    [
      [plugin('acme', ['mcp', 'subagent']), plugin('ledger', ['skill'])],
      [],
      'Uninstall 2 plugins? Their connectors, skills and subagents go with them.',
    ],
    [
      [plugin('acme', ['mcp', 'subagent']), plugin('ledger', ['skill'])],
      [edited('acme')],
      'Uninstall 2 plugins? Their connectors, skills and subagents go with them. Your changes to their subagents are removed too.',
    ],
    [
      [plugin('acme', ['subagent'])],
      [edited('acme')],
      'Uninstall 1 plugin? Its connectors, skills and subagents go with it. Your changes to its subagents are removed too.',
    ],
    // An edited subagent of a plugin that is not being uninstalled says nothing.
    [[plugin('acme', ['subagent'])], [edited('other')], 'Uninstall 1 plugin? Its connectors, skills and subagents go with it.'],
  ])('names subagents only when one goes: %#', (plugins, views, line) => {
    expect(bulkUninstallConfirm(t, plugins, views)).toBe(line);
  });
});

describe('changedSubagents', () => {
  const plugin = { name: 'acme', enabled: true, components: [] as PluginInfo['components'] } as PluginInfo;
  const view = (name: string, edited: boolean, source: SubagentView['source'], from: string | null) =>
    ({
      name,
      edited,
      source,
      plugin: from ? { name: from, display_name: from } : null,
    }) as SubagentView;

  it("counts only this plugin's edited subagents", () => {
    expect(
      changedSubagents(plugin, [
        view('screener', true, 'plugin', 'acme'),
        view('writer', false, 'plugin', 'acme'),
        view('other', true, 'plugin', 'other-plugin'),
        view('research', true, 'builtin', null),
      ]),
    ).toBe(1);
  });

  it('claims none before the list is read', () => {
    expect(changedSubagents(plugin, undefined)).toBe(0);
  });

  // Switched off, its subagents leave the list, but uninstall still takes
  // the user's changes to them.
  it("counts a switched-off plugin's changes from its components", () => {
    const off = {
      name: 'acme',
      enabled: false,
      components: [
        { kind: 'subagent', name: 'screener', key: 'screener', edited: true },
        { kind: 'subagent', name: 'writer', key: 'writer', edited: false },
        { kind: 'skill', name: 'lint', key: 'lint' },
      ],
    } as PluginInfo;
    expect(changedSubagents(off, [])).toBe(1);
    expect(bulkUninstallConfirm(t, [off], [])).toContain('Your changes to its subagents are removed too.');
  });

  // A deployment subagent of the same name takes its place in the list, but
  // uninstall still takes the user's changes to it; a listed view stays the
  // fresher read.
  it("counts a shadowed subagent's changes from its components", () => {
    const shadowed = {
      name: 'acme',
      enabled: true,
      components: [
        { kind: 'subagent', name: 'screener', key: 'screener', edited: true },
        { kind: 'subagent', name: 'writer', key: 'writer', edited: true },
      ],
    } as PluginInfo;
    expect(
      changedSubagents(shadowed, [view('screener', false, 'deployment', null), view('writer', false, 'plugin', 'acme')]),
    ).toBe(1);
  });
});
