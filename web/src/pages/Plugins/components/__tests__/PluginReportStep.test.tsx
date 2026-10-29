/**
 * A package with no plugin.json is packaged from what it carries, and the
 * report says what that was: a folder of agents/ alone is not "0 skills".
 */
import { describe, expect, it } from 'vitest';
import { screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type {
  PluginComponentResult,
  PluginInstallReport,
} from '@/pages/ChatAgent/utils/api/plugins';

import { PluginReportStep } from '../PluginReportStep';

const row = (kind: PluginComponentResult['kind'], key: string): PluginComponentResult => ({
  kind,
  key,
  name: key,
  renamed: false,
  status: 'created',
  reason: '',
  warnings: [],
});

function packaged(components: PluginComponentResult[]): PluginInstallReport {
  return {
    components,
    diagnostics: [
      {
        level: 'info',
        scope: 'plugin',
        target: 'demo',
        code: 'synthesized_manifest',
        message: 'No plugin.json; packaged from the folder.',
      },
    ],
    secrets_created: [],
    secrets_required: [],
    dropped_files: [],
    servers_created: 0,
    skills_created: 0,
  };
}

describe('PluginReportStep packaged note', () => {
  it.each([
    [[row('subagent', 'screener')], "No plugin.json, so this subagent was packaged as 'demo'"],
    [
      [row('subagent', 'screener'), row('subagent', 'writer')],
      "No plugin.json, so these 2 subagents were packaged as 'demo'",
    ],
    [
      [row('skill', 'charts'), row('subagent', 'screener'), row('subagent', 'writer')],
      "No plugin.json, so 1 skill and 2 subagents were packaged as 'demo'",
    ],
    [
      [row('skill', 'charts'), row('skill', 'tables')],
      "No plugin.json, so these 2 skills were packaged as 'demo'",
    ],
    [
      // An update by a folder of one subagent that drops another.
      [row('subagent', 'screener'), { ...row('subagent', 'writer'), status: 'deleted' as const }],
      "No plugin.json, so this subagent was packaged as 'demo'",
    ],
  ])('counts what the folder carried: %#', (components, note) => {
    renderWithProviders(
      <PluginReportStep report={packaged(components)} plugin="demo" onLeave={() => {}} />,
    );
    expect(screen.getByText(note)).toBeInTheDocument();
  });
});
