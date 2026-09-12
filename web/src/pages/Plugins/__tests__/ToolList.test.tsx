import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { McpToolSummary } from '@/pages/ChatAgent/utils/api';
import { ToolList } from '../components/ToolList';
import type { CapabilityGroup } from '../brokerages';

/**
 * A tool outside every capability group is in one of two opposite states, and
 * the page is about what an agent may do with a trading account, so drawing one
 * as the other is the failure that matters here.
 *
 * `always_denied` is the field that separates them, and it is optional on the
 * wire. Absent means the server predates it -- and that server's policy was an
 * allowlist, under which an ungrouped tool was refused. So absent has to read as
 * refused, not as permitted, or a new page against an old server lists exactly
 * the withheld tools as callable.
 */

const GROUPS: CapabilityGroup[] = [
  { key: 'market_data', tone: 'neutral' } as CapabilityGroup,
];

const tool = (overrides: Partial<McpToolSummary>): McpToolSummary =>
  ({ name: 'a_tool', description: '', ...overrides }) as McpToolSummary;

describe('the trailing buckets', () => {
  it('lists an unclassified tool as reachable when the server says so', () => {
    renderWithProviders(
      <ToolList
        groups={GROUPS}
        granted={['market_data']}
        tools={[tool({ name: 'odd_tool', capability: null, always_denied: false })]}
      />,
    );
    expect(screen.getByText('odd_tool')).toBeInTheDocument();
    expect(screen.getByText(/does not refuse them/)).toBeInTheDocument();
  });

  it('lists a deliberately withheld tool as refused', () => {
    renderWithProviders(
      <ToolList
        groups={GROUPS}
        granted={['market_data']}
        tools={[tool({ name: 'held_tool', capability: null, always_denied: true })]}
      />,
    );
    expect(screen.getByText(/Refused whatever you grant/)).toBeInTheDocument();
  });

  it('does not call a tool reachable when the server never said it was', () => {
    // The version-skew case: an older server omits the field entirely.
    renderWithProviders(
      <ToolList
        groups={GROUPS}
        granted={['market_data']}
        tools={[tool({ name: 'unknown_tool', capability: null })]}
      />,
    );
    expect(screen.getByText(/Refused whatever you grant/)).toBeInTheDocument();
    expect(screen.queryByText(/does not refuse them/)).not.toBeInTheDocument();
  });

  it('does not promise reach on a connection that has none', () => {
    // granted === null is "nothing connected here", so the note must not say
    // the agent can call these.
    renderWithProviders(
      <ToolList
        groups={GROUPS}
        granted={null}
        tools={[tool({ name: 'odd_tool', capability: null, always_denied: false })]}
      />,
    );
    expect(screen.getByText(/nothing can call them yet/)).toBeInTheDocument();
  });
});

/**
 * A server with no capability groups is the same bucketed list with one
 * headerless bucket, not a second list shape. Nothing stands above these rows
 * to say what the tools are for, so each name brings its own description.
 */
describe('a server with no capability groups', () => {
  const FLAT = [
    tool({ name: 'fetch_page', description: 'Read one URL.' }),
    tool({ name: 'search_web', description: 'Search the open web.' }),
  ];

  it('draws each tool with its description and no header over them', () => {
    renderWithProviders(<ToolList groups={[]} granted={null} tools={FLAT} />);

    expect(screen.getByText('fetch_page')).toBeInTheDocument();
    expect(screen.getByText('Read one URL.')).toBeInTheDocument();
    expect(screen.getByText('search_web')).toBeInTheDocument();
    expect(screen.queryByTestId('tool-count-flat')).toBeNull();
  });

  // The same rows carry a box and a control when the caller has both, and the
  // only header over them is the list's own select-all: there is no bucket to
  // stand for, so a second box would stand for the whole list twice.
  it('carries a box and a control on the same rows', () => {
    renderWithProviders(
      <ToolList
        groups={[]}
        granted={null}
        tools={FLAT}
        renderControl={(t) => <button type="button">{`bind ${t.name}`}</button>}
        onBulkPatch={() => {}}
      />,
    );

    expect(screen.getByRole('checkbox', { name: 'Select fetch_page' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'bind fetch_page' })).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'Select every tool shown' })).toBeInTheDocument();
    expect(screen.queryByRole('checkbox', { name: /Select every tool in/ })).toBeNull();
  });
});
