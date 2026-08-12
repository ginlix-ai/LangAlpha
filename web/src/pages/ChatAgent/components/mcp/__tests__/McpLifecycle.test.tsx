import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { McpLifecycle } from '../McpLifecycle';

/**
 * McpLifecycle fuses the two axes (verify: discovery, apply: synced) into one
 * honest signal. Terminal states render a pill; progressing states render the
 * animated track with a truthful current phase.
 */

describe('McpLifecycle — terminal pills', () => {
  it('renders the disabled pill for a disabled row', () => {
    render(<McpLifecycle status="disabled" enabled={false} origin="workspace" checking={false} synced={false} sandboxRunning />);
    expect(screen.getByTestId('mcp-status-disabled')).toBeInTheDocument();
    expect(screen.queryByTestId('mcp-lifecycle')).not.toBeInTheDocument();
  });

  it('renders the error pill', () => {
    render(<McpLifecycle status="error" enabled origin="workspace" checking={false} synced={false} sandboxRunning />);
    expect(screen.getByTestId('mcp-status-error')).toBeInTheDocument();
  });

  it('renders the needs_secret pill', () => {
    render(<McpLifecycle status="needs_secret" enabled origin="workspace" checking={false} synced={false} sandboxRunning />);
    expect(screen.getByTestId('mcp-status-needs_secret')).toBeInTheDocument();
  });

  it('collapses to the connected pill once verified AND synced', () => {
    render(<McpLifecycle status="connected" enabled origin="workspace" checking={false} synced sandboxRunning />);
    expect(screen.getByTestId('mcp-status-connected')).toBeInTheDocument();
    expect(screen.queryByTestId('mcp-lifecycle')).not.toBeInTheDocument();
  });

  it('built-ins never show the lifecycle track (always-connected, process-global)', () => {
    // Even when the workspace-level apply axis is behind (synced=false), a
    // built-in stays a plain connected pill — it has no per-workspace lifecycle.
    render(<McpLifecycle status="connected" enabled origin="builtin" checking={false} synced={false} sandboxRunning={false} />);
    expect(screen.getByTestId('mcp-status-connected')).toBeInTheDocument();
    expect(screen.queryByTestId('mcp-lifecycle')).not.toBeInTheDocument();
  });
});

describe('McpLifecycle — progressing track', () => {
  it('shows "Verifying…" while a probe is in flight', () => {
    render(<McpLifecycle status="pending" enabled origin="workspace" checking synced={false} sandboxRunning />);
    const track = screen.getByTestId('mcp-lifecycle');
    expect(track).toHaveAttribute('data-phase', 'verifying');
    expect(screen.getByText('Verifying…')).toBeInTheDocument();
  });

  it('auto-verifies a pending server while the sandbox is running', () => {
    render(<McpLifecycle status="pending" enabled origin="workspace" checking={false} synced={false} sandboxRunning />);
    expect(screen.getByTestId('mcp-lifecycle')).toHaveAttribute('data-phase', 'verifying');
    expect(screen.getByText('Verifying…')).toBeInTheDocument();
  });

  it('says it waits for the workspace when stopped + pending', () => {
    render(<McpLifecycle status="pending" enabled origin="workspace" checking={false} synced={false} sandboxRunning={false} />);
    const track = screen.getByTestId('mcp-lifecycle');
    expect(track).toHaveAttribute('data-phase', 'waiting');
    expect(screen.getByText('Waiting for workspace to start')).toBeInTheDocument();
  });

  it('says "Starting workspace…" when pending + a warm is in flight', () => {
    // A save kicked a background warm: the sandbox isn't running yet, but it's
    // on its way up, so the verify step is active (not a dead "Waiting…").
    render(
      <McpLifecycle
        status="pending"
        enabled
        origin="workspace"
        checking={false}
        synced={false}
        sandboxRunning={false}
        sandboxWarming
      />,
    );
    const track = screen.getByTestId('mcp-lifecycle');
    expect(track).toHaveAttribute('data-phase', 'starting');
    expect(screen.getByText('Starting workspace…')).toBeInTheDocument();
  });

  it('reads "Applying to agent…" when verified but the running agent has not loaded it', () => {
    render(<McpLifecycle status="connected" enabled origin="workspace" checking={false} synced={false} sandboxRunning />);
    const track = screen.getByTestId('mcp-lifecycle');
    expect(track).toHaveAttribute('data-phase', 'applying');
    expect(screen.getByText('Applying to agent…')).toBeInTheDocument();
  });
});
