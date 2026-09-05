import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { McpServerModal } from '../McpServerModal';
import type { McpServerDraft } from '../../../utils/api';

const baseProps = {
  secretNames: ['EXISTING_TOKEN'],
  onClose: vi.fn(),
  onSubmit: vi.fn().mockResolvedValue(undefined),
  createSecret: vi.fn().mockResolvedValue({ name: 'NEW' }),
};

/**
 * An edited server as the modal now receives it — the config-shaped `Pick`, not
 * a whole row. Both surfaces hand it one of their real rows; nothing about
 * status, permissions or tool counts reaches this component.
 */
function makeDraft(overrides: Partial<McpServerDraft> = {}): McpServerDraft {
  return {
    name: 'srv',
    transport: 'stdio',
    command: 'npx',
    args: [],
    url: null,
    env_refs: [],
    header_refs: [],
    description: '',
    instruction: '',
    tool_exposure_mode: 'summary',
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('McpServerModal — conditional fields per transport', () => {
  it('shows stdio fields (command, args, env) by default', () => {
    render(<McpServerModal {...baseProps} />);
    expect(screen.getByText('Command')).toBeInTheDocument();
    expect(screen.getByText('Arguments')).toBeInTheDocument();
    expect(screen.getByText('Environment variables')).toBeInTheDocument();
    // No URL/Headers fields for stdio.
    expect(screen.queryByText('URL')).not.toBeInTheDocument();
    expect(screen.queryByText('Headers')).not.toBeInTheDocument();
  });

  it('switches to URL + Headers when transport is http', () => {
    render(<McpServerModal {...baseProps} />);
    fireEvent.click(screen.getByRole('button', { name: 'http' }));
    expect(screen.getByText('URL')).toBeInTheDocument();
    expect(screen.getByText('Headers')).toBeInTheDocument();
    // Command/Args/Env gone for remote transports.
    expect(screen.queryByText('Command')).not.toBeInTheDocument();
    expect(screen.queryByText('Arguments')).not.toBeInTheDocument();
    expect(screen.queryByText('Environment variables')).not.toBeInTheDocument();
  });

  it('switches to URL + Headers when transport is sse', () => {
    render(<McpServerModal {...baseProps} />);
    fireEvent.click(screen.getByRole('button', { name: 'sse' }));
    expect(screen.getByText('URL')).toBeInTheDocument();
    expect(screen.getByText('Headers')).toBeInTheDocument();
  });

  // Each hint has to say something the other doesn't. They used to be one
  // string repeated, which told the user nothing about which field to fill in.
  it('gives description and instruction their own helper text', () => {
    render(<McpServerModal {...baseProps} />);
    expect(screen.getByText(/decide when to reach for this server/i)).toBeInTheDocument();
    expect(screen.getByText(/before it calls this server's tools/i)).toBeInTheDocument();
  });

  it('exposes the summary/detailed exposure toggle', () => {
    render(<McpServerModal {...baseProps} />);
    expect(screen.getByRole('button', { name: 'summary' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'detailed' })).toBeInTheDocument();
  });

  it('renders the discovery-secrets toggle, off by default', () => {
    render(<McpServerModal {...baseProps} />);
    const checkbox = screen.getByRole('checkbox', { name: /use my secrets during discovery/i });
    expect(checkbox).toBeInTheDocument();
    expect(checkbox).not.toBeChecked();
  });
});

describe('McpServerModal — discovery_uses_secrets toggle', () => {
  it('defaults discovery_uses_secrets to false in the submit payload', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ discovery_uses_secrets: false }),
    );
  });

  it('includes discovery_uses_secrets=true in the payload when toggled on', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('checkbox', { name: /use my secrets during discovery/i }));
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ discovery_uses_secrets: true }),
    );
  });

  it('pre-fills the toggle from the edited server', () => {
    render(<McpServerModal {...baseProps} initial={makeDraft({ discovery_uses_secrets: true })} />);
    expect(
      screen.getByRole('checkbox', { name: /use my secrets during discovery/i }),
    ).toBeChecked();
  });
});

describe('McpServerModal — edit-mode env/header hydration (data-loss guard)', () => {
  // FIX 1: in edit mode env/headers must hydrate from the stored reference maps
  // (real keys + ${vault:NAME}/literal values), so an unrelated edit re-saves the
  // existing config intact. The old code seeded BLANK keys from env_refs, and
  // kvsToMap drops blank-key rows → PUT silently erased every entry on save.
  const editingStdio = makeDraft({
    env_refs: ['API_TOKEN'],
    env: { API_TOKEN: '${vault:API_TOKEN}', REGION: 'us-east-1' },
    headers: {},
    description: 'old description',
  });

  it('preserves env entries from the stored map when saving an unrelated edit', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} initial={editingStdio} onSubmit={onSubmit} />);

    // The env editor should be pre-filled with the REAL keys (not blank).
    expect(screen.getByDisplayValue('API_TOKEN')).toBeInTheDocument();
    expect(screen.getByDisplayValue('REGION')).toBeInTheDocument();

    // Touch an unrelated field, then save.
    fireEvent.change(screen.getByPlaceholderText('What this server does'), {
      target: { value: 'new description' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'srv',
        description: 'new description',
        // The full env config survives the save — no silent erasure.
        env: { API_TOKEN: '${vault:API_TOKEN}', REGION: 'us-east-1' },
      }),
    );
  });

  it('preserves header entries from the stored map when saving an http server', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    const editingHttp: McpServerDraft = {
      ...editingStdio,
      transport: 'http',
      command: null,
      url: 'https://example.com/mcp',
      env: {},
      env_refs: [],
      headers: { Authorization: '${vault:AUTH}', 'X-Region': 'eu' },
      header_refs: ['AUTH'],
    };
    render(<McpServerModal {...baseProps} initial={editingHttp} onSubmit={onSubmit} />);

    expect(screen.getByDisplayValue('Authorization')).toBeInTheDocument();
    expect(screen.getByDisplayValue('X-Region')).toBeInTheDocument();

    fireEvent.change(screen.getByPlaceholderText('What this server does'), {
      target: { value: 'edited' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        headers: { Authorization: '${vault:AUTH}', 'X-Region': 'eu' },
      }),
    );
  });

  it('blocks save on refs-only hydration (blank keys) until the user re-labels them', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    // No `env`/`headers` maps — only the legacy refs. Keys come back blank, and
    // kvsToMap silently drops blank-key rows — so save must REFUSE rather than
    // erase the entries.
    const legacy = { ...editingStdio, env: undefined, headers: undefined };
    render(<McpServerModal {...baseProps} initial={legacy} onSubmit={onSubmit} />);

    // Refs-only hydration seeds a BLANK key (it can't recover the real key name).
    expect(screen.queryByDisplayValue('API_TOKEN')).not.toBeInTheDocument();
    expect(screen.queryByDisplayValue('REGION')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    expect(await screen.findByText(/key is required/i)).toBeInTheDocument();
    expect(onSubmit).not.toHaveBeenCalled();

    // Re-labeling the blank key clears the guard; the entry survives the save.
    fireEvent.change(screen.getByPlaceholderText('ENV_VAR'), {
      target: { value: 'API_TOKEN' },
    });
    fireEvent.click(screen.getByRole('button', { name: /^save$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ env: { API_TOKEN: '${vault:API_TOKEN}' } }),
    );
  });
});

describe('McpServerModal — validation gating', () => {
  it('disables Add until a valid name is entered', () => {
    render(<McpServerModal {...baseProps} />);
    const addBtn = screen.getByRole('button', { name: /^add$/i });
    expect(addBtn).toBeDisabled();
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    expect(addBtn).not.toBeDisabled();
  });

  it('keeps Add disabled for an http server with a private-IP url (SSRF policy)', () => {
    render(<McpServerModal {...baseProps} />);
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'remote' } });
    fireEvent.click(screen.getByRole('button', { name: 'http' }));
    fireEvent.change(screen.getByPlaceholderText('https://example.com/mcp'), {
      target: { value: 'https://169.254.169.254/' },
    });
    expect(screen.getByRole('button', { name: /^add$/i })).toBeDisabled();
  });

  it('submits the built payload on Add', async () => {
    const onSubmit = vi.fn().mockResolvedValue(undefined);
    render(<McpServerModal {...baseProps} onSubmit={onSubmit} />);
    fireEvent.change(screen.getByPlaceholderText('my_server'), { target: { value: 'good_name' } });
    fireEvent.click(screen.getByRole('button', { name: /^add$/i }));
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'good_name', transport: 'stdio', command: 'npx' }),
    );
  });

  it('locks the name field when editing', () => {
    render(<McpServerModal {...baseProps} initial={makeDraft({ name: 'locked_name' })} />);
    const nameInput = screen.getByDisplayValue('locked_name') as HTMLInputElement;
    expect(nameInput).toBeDisabled();
  });
});
