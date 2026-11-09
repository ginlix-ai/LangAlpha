import { describe, expect, it } from 'vitest';
import type { ClaudeCodeSetup } from '@/api/model';
import { accountClaudeCodeHold } from '@/lib/claudeCode';

const setup = (over: Partial<ClaudeCodeSetup>) =>
  ({ state: 'connected', on: true, computers: [], saved_token_runs: true, cloud: false, cli_in_sandbox: false, ...over }) as ClaudeCodeSetup;

describe('accountClaudeCodeHold', () => {
  // While the web is newer than the server, that server may still keep pins
  // off the stored token and says so in `pin_state`.
  it('holds a pin where a server that still sends pin_state says no token signs it in', () => {
    expect(accountClaudeCodeHold(setup({ pin_state: 'refused' }))).not.toBeNull();
  });

  it('reads the setup state where the server sends no pin_state', () => {
    expect(accountClaudeCodeHold(setup({}))).toBeNull();
    expect(accountClaudeCodeHold(setup({ state: 'refused' }))).not.toBeNull();
  });
});
