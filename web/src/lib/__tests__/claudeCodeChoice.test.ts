/**
 * Claude Code's choice under Connect a subscription names where this server
 * runs it: on the platform, the user's computer, and a cloud computer where
 * the server offers one; on a self-hosted server, this server where it runs
 * turns on a saved token, or the cloud computers' sandboxes.
 */
import { describe, expect, it, vi } from 'vitest';
import type { ClaudeCodeSetup } from '@/api/model';

const host = vi.hoisted(() => ({ platform: false }));
vi.mock('@/config/hostMode', async (original) => ({
  ...(await original<typeof import('@/config/hostMode')>()),
  get isPlatformMode() { return host.platform; },
}));

import { claudeCodeChoice } from '../claudeCode';

const setupOf = (over: Partial<ClaudeCodeSetup>): ClaudeCodeSetup => ({
  state: 'not_connected', on: true, computers: [], saved_token_runs: false, cloud: false, too_small: [], ...over,
});
const noteOf = (setup: ClaudeCodeSetup | null, platform: boolean) => {
  host.platform = platform;
  return claudeCodeChoice(setup, true, vi.fn())?.note;
};
const S = 'modelAccess.claudeCode.setup.';

describe('claudeCodeChoice', () => {
  it.each([
    ['the platform with cloud computers offered', setupOf({ cloud: true, saved_token_runs: true }), true, S + 'pickNoteCloud'],
    ['the platform with only own computers', setupOf({}), true, S + 'pickNote'],
    ['a self-hosted server running the saved token itself', setupOf({ saved_token_runs: true }), false, S + 'pickNoteServer'],
    ['a self-hosted server running it in cloud computers\' sandboxes', setupOf({ saved_token_runs: true, cli_in_sandbox: true }), false, S + 'pickNoteCloud'],
    ['a self-hosted server that runs no saved token', setupOf({}), false, S + 'pickNote'],
    ['a setup not read yet', null, true, S + 'pickNote'],
  ])('on %s', (_, setup, platform, note) => {
    expect(noteOf(setup, platform)).toBe(note);
  });

  it('carries the status word, and nothing where the server offers no Claude Code', () => {
    expect(claudeCodeChoice(setupOf({ state: 'offline' }), false, vi.fn())?.status).toBe('offline');
    expect(claudeCodeChoice(null, true, vi.fn())?.status).toBe('unknown');
    expect(claudeCodeChoice(null, false, vi.fn())).toBeUndefined();
  });
});
