import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { McpStatusPill } from '../McpStatusPill';
import type { McpStatus } from '../../../utils/api';

describe('McpStatusPill — status matrix', () => {
  it.each([
    ['connected', 'Connected'],
    ['error', 'Error'],
    ['needs_secret', 'Needs secret'],
    ['pending', 'Pending'],
    ['unknown', 'Unknown'],
  ] as [McpStatus, string][])('renders %s label when enabled', (status, label) => {
    render(<McpStatusPill status={status} enabled />);
    expect(screen.getByText(label)).toBeInTheDocument();
    expect(screen.getByTestId(`mcp-status-${status}`)).toBeInTheDocument();
  });

  it('renders the disabled (muted) state when the row is disabled, overriding status', () => {
    render(<McpStatusPill status="connected" enabled={false} />);
    expect(screen.getByText('Disabled')).toBeInTheDocument();
    expect(screen.getByTestId('mcp-status-disabled')).toBeInTheDocument();
    // The underlying status label must NOT show when disabled.
    expect(screen.queryByText('Connected')).not.toBeInTheDocument();
  });

  it('surfaces the pending hint via title for the pending state', () => {
    render(<McpStatusPill status="pending" enabled />);
    const pill = screen.getByTestId('mcp-status-pending');
    expect(pill).toHaveAttribute('title', expect.stringMatching(/waiting for discovery/i));
  });

});
