import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { McpServerRow } from '../McpServerRow';
import type { EffectiveServer } from '../../../utils/api';

// Mirror the repo convention (FileHeaderActions.test): render the Radix
// dropdown inline so items are queryable without portal/pointer machinery. A
// disabled item must NOT fire onSelect, mirroring real Radix behaviour.
vi.mock('@/components/ui/dropdown-menu', () => ({
  DropdownMenu: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  DropdownMenuContent: ({ children }: { children: React.ReactNode }) => (
    <div role="menu">{children}</div>
  ),
  DropdownMenuItem: ({
    children,
    onSelect,
    disabled,
    className,
  }: {
    children: React.ReactNode;
    onSelect?: () => void;
    disabled?: boolean;
    className?: string;
  }) => (
    <button
      role="menuitem"
      aria-disabled={disabled ? 'true' : undefined}
      className={className}
      onClick={() => { if (!disabled) onSelect?.(); }}
    >
      {children}
    </button>
  ),
}));

function makeServer(overrides: Partial<EffectiveServer> = {}): EffectiveServer {
  return {
    name: 'placeholder_server',
    origin: 'workspace',
    transport: 'stdio',
    enabled: true,
    editable: true,
    deletable: true,
    status: 'connected',
    error: '',
    tool_count: 3,
    tools: [],
    missing_secrets: [],
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    command: 'npx',
    args: [],
    url: null,
    config_version: 1,
    ...overrides,
  };
}

const handlers = () => ({
  onToggle: vi.fn(),
  onEdit: vi.fn(),
  onDiscover: vi.fn(),
  onDelete: vi.fn(),
  onPromoteToTemplate: vi.fn(),
  onSetupSecret: vi.fn(),
});

beforeEach(() => {
  vi.clearAllMocks();
});

describe('McpServerRow — origin badge + base render', () => {
  it('shows the workspace badge, tool count, and connected pill when verified + synced', () => {
    // A fully-settled server (verified AND applied to the live agent) collapses
    // to the clean green pill — no perpetual lifecycle track.
    render(<McpServerRow server={makeServer()} synced sandboxRunning {...handlers()} />);
    expect(screen.getByText('workspace')).toBeInTheDocument();
    expect(screen.getByText('3 tools')).toBeInTheDocument();
    expect(screen.getByTestId('mcp-status-connected')).toBeInTheDocument();
  });

  it('shows the built-in badge for builtins', () => {
    render(<McpServerRow server={makeServer({ origin: 'builtin', editable: false, deletable: false })} {...handlers()} />);
    expect(screen.getByText('built-in')).toBeInTheDocument();
  });
});

describe('McpServerRow — enabled toggle', () => {
  it('toggles via the switch (interactive for builtins too)', () => {
    const h = handlers();
    const server = makeServer({ origin: 'builtin', editable: false, deletable: false });
    render(<McpServerRow server={server} {...h} />);
    fireEvent.click(screen.getByRole('switch'));
    // Handlers receive the row's own server (stable-prop pattern) + the new value.
    expect(h.onToggle).toHaveBeenCalledWith(server, false);
  });
});

describe('McpServerRow — kebab menu (builtins restricted)', () => {
  it('disables Edit/Test/Delete for a built-in server', () => {
    const h = handlers();
    render(<McpServerRow server={makeServer({ origin: 'builtin', editable: false, deletable: false })} {...h} />);

    for (const label of ['Edit', 'Test connection', 'Save to your servers', 'Delete']) {
      const item = screen.getByText(label).closest('[role="menuitem"]')!;
      expect(item).toHaveAttribute('aria-disabled', 'true');
    }

    // Clicking a disabled item is a no-op.
    fireEvent.click(screen.getByText('Edit'));
    fireEvent.click(screen.getByText('Delete'));
    fireEvent.click(screen.getByText('Save to your servers'));
    expect(h.onEdit).not.toHaveBeenCalled();
    expect(h.onDelete).not.toHaveBeenCalled();
    expect(h.onPromoteToTemplate).not.toHaveBeenCalled();
  });

  it('enables Edit/Test/Save/Delete for a workspace server and fires handlers', () => {
    const h = handlers();
    render(<McpServerRow server={makeServer()} {...h} />);

    fireEvent.click(screen.getByText('Edit'));
    expect(h.onEdit).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('Test connection'));
    expect(h.onDiscover).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('Save to your servers'));
    expect(h.onPromoteToTemplate).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('Delete'));
    expect(h.onDelete).toHaveBeenCalledTimes(1);
  });

  it('disables "Save to your servers" when no promote handler is provided', () => {
    const { onPromoteToTemplate, ...rest } = handlers();
    void onPromoteToTemplate;
    render(<McpServerRow server={makeServer()} {...rest} />);
    const item = screen.getByText('Save to your servers').closest('[role="menuitem"]')!;
    expect(item).toHaveAttribute('aria-disabled', 'true');
  });

  it('keeps a disabled workspace server re-enableable but ungates only the toggle', () => {
    const h = handlers();
    const server = makeServer({ enabled: false, status: 'disabled' });
    render(<McpServerRow server={server} {...h} />);

    // The toggle is the way back on.
    fireEvent.click(screen.getByRole('switch'));
    expect(h.onToggle).toHaveBeenCalledWith(server, true);

    // "Test connection" is off (discovery only runs against enabled servers)…
    expect(screen.getByText('Test connection').closest('[role="menuitem"]'))
      .toHaveAttribute('aria-disabled', 'true');

    // …but Edit / Save to your servers / Delete still work on a disabled server.
    fireEvent.click(screen.getByText('Edit'));
    fireEvent.click(screen.getByText('Save to your servers'));
    fireEvent.click(screen.getByText('Delete'));
    expect(h.onEdit).toHaveBeenCalledTimes(1);
    expect(h.onPromoteToTemplate).toHaveBeenCalledTimes(1);
    expect(h.onDelete).toHaveBeenCalledTimes(1);
  });
});

describe('McpServerRow — in-flight affordances', () => {
  it('shows the kebab spinner while deleting but NOT while toggling (optimistic)', () => {
    // Toggle is optimistic — the switch already moved, so a spinning "reload"
    // icon on the kebab is just flicker. Only a real delete (row leaving) spins.
    const { container, rerender } = render(
      <McpServerRow server={makeServer()} toggling {...handlers()} />,
    );
    expect(container.querySelector('[role="status"]')).toBeNull();

    rerender(<McpServerRow server={makeServer()} deleting {...handlers()} />);
    expect(container.querySelector('[role="status"]')).not.toBeNull();
  });
});

describe('McpServerRow — status-specific affordances', () => {
  it('surfaces the error text on an error row', () => {
    render(<McpServerRow server={makeServer({ status: 'error', error: 'could not start' })} {...handlers()} />);
    expect(screen.getByText('could not start')).toBeInTheDocument();
  });

  it('renders a "Set up NAME" affordance for needs_secret rows', () => {
    const h = handlers();
    render(
      <McpServerRow
        server={makeServer({ status: 'needs_secret', missing_secrets: ['MY_API_KEY'] })}
        {...h}
      />,
    );
    const setup = screen.getByText('Set up MY_API_KEY');
    fireEvent.click(setup);
    expect(h.onSetupSecret).toHaveBeenCalledWith('MY_API_KEY');
  });

  it('shows the live lifecycle track (verifying) while a probe is in flight', () => {
    render(
      <McpServerRow server={makeServer({ status: 'pending' })} checking sandboxRunning {...handlers()} />,
    );
    // A still-progressing server shows the animated lifecycle track, not the
    // (stale, about-to-change) backend status pill.
    const track = screen.getByTestId('mcp-lifecycle');
    expect(track).toHaveAttribute('data-phase', 'verifying');
    expect(screen.getByText('Verifying…')).toBeInTheDocument();
    expect(screen.queryByTestId('mcp-status-pending')).not.toBeInTheDocument();
  });

  it('reads "Applying to agent…" when verified but not yet synced', () => {
    // Discovery found the tools (connected) but the running agent hasn't loaded
    // the new config yet (synced=false) — the apply axis is still in flight.
    render(
      <McpServerRow server={makeServer({ status: 'connected' })} synced={false} sandboxRunning {...handlers()} />,
    );
    const track = screen.getByTestId('mcp-lifecycle');
    expect(track).toHaveAttribute('data-phase', 'applying');
    expect(screen.getByText('Applying to agent…')).toBeInTheDocument();
    expect(screen.queryByTestId('mcp-status-connected')).not.toBeInTheDocument();
  });

  it('offers exactly one next step on a revoked row with a stale needs_secret status', () => {
    // Regression: the needs_secret gate was missing the `!oauthBroken` conjunct
    // its two sibling gates had, so a revoked inherited row rendered BOTH
    // "Set up NAME" and "Reconnect in Plugins" — two contradictory fixes,
    // only one of which works. The missing secret is not the real problem here;
    // the cached status predates the disconnect.
    const h = handlers();
    const onManageInPlugins = vi.fn();
    render(
      <McpServerRow
        server={makeServer({
          origin: 'user',
          status: 'needs_secret',
          missing_secrets: ['PLACEHOLDER_TOKEN'],
          oauth_status: 'revoked',
        })}
        onManageInPlugins={onManageInPlugins}
        {...h}
      />,
    );

    expect(screen.queryByText('Set up PLACEHOLDER_TOKEN')).not.toBeInTheDocument();
    fireEvent.click(screen.getByText('Reconnect in Plugins'));
    expect(onManageInPlugins).toHaveBeenCalledTimes(1);
    expect(h.onSetupSecret).not.toHaveBeenCalled();
  });

  it('still shows "Set up NAME" on a needs_secret row whose OAuth connection is healthy', () => {
    // The guard must not over-fire: a connected OAuth server genuinely missing
    // a vault secret keeps its local fix.
    const h = handlers();
    render(
      <McpServerRow
        server={makeServer({
          origin: 'user',
          status: 'needs_secret',
          missing_secrets: ['PLACEHOLDER_TOKEN'],
          oauth_status: 'connected',
        })}
        onManageInPlugins={vi.fn()}
        {...h}
      />,
    );
    fireEvent.click(screen.getByText('Set up PLACEHOLDER_TOKEN'));
    expect(h.onSetupSecret).toHaveBeenCalledWith('PLACEHOLDER_TOKEN');
    expect(screen.queryByText('Reconnect in Plugins')).not.toBeInTheDocument();
  });

  it('suppresses the tool count while still verifying', () => {
    render(
      <McpServerRow
        server={makeServer({ status: 'pending', tool_count: 5 })}
        checking
        sandboxRunning
        {...handlers()}
      />,
    );
    expect(screen.getByTestId('mcp-lifecycle')).toBeInTheDocument();
    expect(screen.queryByText('5 tools')).not.toBeInTheDocument();
  });
});
