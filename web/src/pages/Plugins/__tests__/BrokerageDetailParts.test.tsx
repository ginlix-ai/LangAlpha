import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import type { McpToolSummary } from '@/pages/ChatAgent/utils/api';
import { GroupedToolList } from '../components/BrokerageDetailParts';
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

describe('GroupedToolList trailing buckets', () => {
  it('lists an unclassified tool as reachable when the server says so', () => {
    renderWithProviders(
      <GroupedToolList
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
      <GroupedToolList
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
      <GroupedToolList
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
      <GroupedToolList
        groups={GROUPS}
        granted={null}
        tools={[tool({ name: 'odd_tool', capability: null, always_denied: false })]}
      />,
    );
    expect(screen.getByText(/nothing can call them yet/)).toBeInTheDocument();
  });
});
