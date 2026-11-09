import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MemoryRouter } from 'react-router';
import type { ClaudeCodeSetupState } from '@/api/model';
import type { SubagentView } from '@/pages/ChatAgent/utils/api';
import { MODEL_ACCESS_URL } from '@/lib/claudeCode';
import { ModelBlock, type SubagentModels } from '../SubagentAside';

/**
 * A subagent's pin offers Claude Code's models, which it runs where the
 * turn's workspace runs Claude Code. A pin applies in every workspace, so
 * each row reads the user's Claude Code as a whole, worded as the composer
 * words it, and a pin on one says where it runs.
 */

const mocks = vi.hoisted(() => ({
  state: 'connected' as ClaudeCodeSetupState,
}));

vi.mock('@/hooks/useClaudeCode', () => ({
  useClaudeCodeSetup: () => ({ setup: { state: mocks.state, on: true, computers: [] } }),
}));
vi.mock('@/hooks/usePayerLabel', () => ({ usePayerLabel: () => () => '' }));

const MODELS = {
  models: { openai: { models: ['gpt-x'] }, 'claude-code': { display_name: 'Claude Code', models: ['claude-x-code'] } },
  slotModels: { openai: { models: ['gpt-x'] } },
  lockedModels: {},
  payers: {},
  metadata: {
    'gpt-x': { provider: 'openai' },
    'claude-x-code': { provider: 'claude-code', sdk: 'claude-code', display_name: 'Claude X' },
  },
} as unknown as SubagentModels;

function renderBlock(model: string | null) {
  const view = { name: 'r3-cc-sa', model, effort: null, problems: [], base: null } as unknown as SubagentView;
  render(
    <MemoryRouter>
      <ModelBlock view={view} data={{} as never} models={MODELS} write={() => {}} />
    </MemoryRouter>,
  );
}

const ccOption = () => screen.getByRole('option', { name: /^Claude X/ }) as HTMLOptionElement;
const PIN_HINT = "Runs on your Claude plan where the thread's workspace runs Claude Code, and on the thread's model elsewhere.";

describe('ModelBlock with Claude Code', () => {
  beforeEach(() => {
    mocks.state = 'connected';
  });

  it('offers Claude Code models as a pick while a token works somewhere', () => {
    renderBlock(null);
    expect(ccOption().textContent).toBe('Claude X');
    expect(ccOption()).not.toBeDisabled();
  });

  it('says what holds them as the composer does, a pick while the user can fix it', () => {
    mocks.state = 'not_connected';
    renderBlock(null);
    expect(ccOption().textContent).toBe('Claude X · Add a setup token in Settings to use this model');
    expect(ccOption()).not.toBeDisabled();
    const other = document.querySelector<HTMLOptionElement>('option[value="gpt-x"]')!;
    expect(other.textContent).not.toContain('·');
    expect(other).not.toBeDisabled();
  });

  it('is no pick where nothing the user has runs Claude Code', () => {
    mocks.state = 'no_computer';
    renderBlock(null);
    expect(ccOption().textContent).toBe('Claude X · Runs only on workspaces on your own computers');
    expect(ccOption()).toBeDisabled();
  });

  it('says where a Claude Code pin runs, with the fix while it is held', () => {
    renderBlock('claude-x-code');
    expect(screen.getByText(PIN_HINT)).toBeInTheDocument();
    expect(screen.queryByRole('link')).toBeNull();
  });

  it('links a held pin to its fix', () => {
    mocks.state = 'refused';
    renderBlock('claude-x-code');
    expect(screen.getByRole('link', { name: 'Add setup token' })).toHaveAttribute('href', MODEL_ACCESS_URL);
  });

  it('leaves the hint off a pin on another model', () => {
    renderBlock('gpt-x');
    expect(screen.queryByText(PIN_HINT)).toBeNull();
  });
});
