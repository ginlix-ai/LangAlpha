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
  DropdownMenuLabel: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
  DropdownMenuSeparator: () => <hr />,
}));

function makeServer(overrides: Partial<EffectiveServer> = {}): EffectiveServer {
  return {
    name: 'placeholder_server',
    origin: 'user',
    transport: 'stdio',
    enabled: true,
    editable: true,
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
  onSetupSecret: vi.fn(),
});

beforeEach(() => {
  vi.clearAllMocks();
});

describe('McpServerRow — origin badge + base render', () => {
  it('shows the account badge, tool count, and connected pill when verified + synced', () => {
    // A fully-settled server (verified AND applied to the live agent) collapses
    // to the clean green pill — no perpetual lifecycle track.
    render(<McpServerRow server={makeServer()} synced sandboxRunning {...handlers()} />);
    expect(screen.getByText('account')).toBeInTheDocument();
    expect(screen.getByText('3 tools')).toBeInTheDocument();
    expect(screen.getByTestId('mcp-status-connected')).toBeInTheDocument();
  });

  it('shows the built-in badge for builtins', () => {
    render(<McpServerRow server={makeServer({ origin: 'builtin', editable: false })} {...handlers()} />);
    expect(screen.getByText('built-in')).toBeInTheDocument();
  });
});

describe('McpServerRow — enabled toggle', () => {
  it('toggles via the switch (interactive for builtins too)', () => {
    const h = handlers();
    const server = makeServer({ origin: 'builtin', editable: false });
    render(<McpServerRow server={server} {...h} />);
    fireEvent.click(screen.getByRole('switch'));
    // Handlers receive the row's own server (stable-prop pattern) + the new value.
    expect(h.onToggle).toHaveBeenCalledWith(server, false);
  });
});

describe('McpServerRow — kebab menu (builtins restricted)', () => {
  it('disables Edit/Test for a built-in server', () => {
    const h = handlers();
    render(<McpServerRow server={makeServer({ origin: 'builtin', editable: false })} {...h} />);

    for (const label of ['Edit', 'Test connection']) {
      const item = screen.getByText(label).closest('[role="menuitem"]')!;
      expect(item).toHaveAttribute('aria-disabled', 'true');
    }

    // Clicking a disabled item is a no-op.
    fireEvent.click(screen.getByText('Edit'));
    expect(h.onEdit).not.toHaveBeenCalled();
  });

  it('enables Edit/Test for an account server and fires handlers', () => {
    const h = handlers();
    render(<McpServerRow server={makeServer()} {...h} />);

    fireEvent.click(screen.getByText('Edit'));
    expect(h.onEdit).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByText('Test connection'));
    expect(h.onDiscover).toHaveBeenCalledTimes(1);
  });

  it('has no delete here: removal is an account action, so the menu says where', () => {
    render(<McpServerRow server={makeServer()} onManageInPlugins={vi.fn()} {...handlers()} />);
    expect(screen.queryByText('Delete')).not.toBeInTheDocument();
    expect(screen.getByText(/remove it from your account/i)).toBeInTheDocument();
  });

  it('keeps a disabled server re-enableable but ungates only the toggle', () => {
    const h = handlers();
    const server = makeServer({ enabled: false, status: 'disabled' });
    render(<McpServerRow server={server} {...h} />);

    // The toggle is the way back on.
    fireEvent.click(screen.getByRole('switch'));
    expect(h.onToggle).toHaveBeenCalledWith(server, true);

    // "Test connection" is off (discovery only runs against enabled servers)…
    expect(screen.getByText('Test connection').closest('[role="menuitem"]'))
      .toHaveAttribute('aria-disabled', 'true');

    // …but Edit still works on a disabled server.
    fireEvent.click(screen.getByText('Edit'));
    expect(h.onEdit).toHaveBeenCalledTimes(1);
  });
});

describe('McpServerRow — in-flight affordances', () => {
  it('shows no kebab spinner while toggling (optimistic)', () => {
    // Toggle is optimistic — the switch already moved, so a spinning "reload"
    // icon on the kebab is just flicker.
    const { container } = render(
      <McpServerRow server={makeServer()} toggling {...handlers()} />,
    );
    expect(container.querySelector('[role="status"]')).toBeNull();
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
