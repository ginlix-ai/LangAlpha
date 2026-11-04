/**
 * A Claude Code refusal says how it is fixed: a token Claude refused is fixed
 * with a new one in Settings, as no token is, and the app words it itself.
 * On a cloud computer, the choice of how it runs there or the Anthropic API
 * key it runs on fixes it.
 */
import { describe, it, expect } from 'vitest';
import { ANTHROPIC_KEY_URL, CLAUDE_CODE_CHOICE_URL, claudeCodeRefusal, claudeCodeTokenUrl } from '../claudeCode';

describe('claudeCodeRefusal', () => {
  it('links a refused token to its sheet, reads the tokens again, and words it in the app', () => {
    expect(claudeCodeRefusal('token_refused', 'ws-1')).toEqual({
      reread: true,
      link: { url: claudeCodeTokenUrl('ws-1'), label: 'Add setup token', labelKey: 'modelAccess.claudeCode.add' },
      messageKey: 'chat.claudeCode.off.token_refused',
    });
  });

  it('links a cloud computer kept off to Model access, and one without a key it can use to the Anthropic key, reading them again', () => {
    expect(claudeCodeRefusal('hosted_off', 'ws-1')).toEqual({
      reread: true,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.hosted_off',
    });
    expect(claudeCodeRefusal('needs_api_key', 'ws-1')).toEqual({
      reread: true,
      link: { url: ANTHROPIC_KEY_URL, label: 'Add an API key', labelKey: 'chat.errorLinkAddApiKey' },
      messageKey: 'chat.claudeCode.off.needs_api_key',
    });
    expect(claudeCodeRefusal('api_key_endpoint', 'ws-1')).toEqual({
      reread: true,
      link: { url: ANTHROPIC_KEY_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.api_key_endpoint',
    });
  });

  it('links Claude Code off for the account to its view under Model access, reading the opt-in again', () => {
    expect(claudeCodeRefusal('account', 'ws-1')).toEqual({
      reread: true,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.account',
    });
  });

  it('links automations on a stored token to Model access, reading nothing again', () => {
    expect(claudeCodeRefusal('stored_unattended', 'ws-1')).toEqual({
      reread: false,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.stored_unattended',
    });
  });

  it.each(['hosted_paused', 'stored_blocked', 'choice_changed'])('words %s in the app with nothing to fix, and reads the choice again', (reason) => {
    expect(claudeCodeRefusal(reason, 'ws-1')).toEqual({ reread: true, link: null, messageKey: `chat.claudeCode.off.${reason}` });
  });

  it.each(['api_key_refused', 'busy'])('words %s mid-turn in the app, with nothing to link', (reason) => {
    expect(claudeCodeRefusal(reason, 'ws-1')).toEqual({ reread: false, link: null, messageKey: `chat.claudeCode.off.${reason}` });
  });

  it("names the user's cap on turns at once, and keeps the server words without one", () => {
    expect(claudeCodeRefusal('too_many_turns', 'ws-1', 3)).toEqual({
      reread: false, link: null, messageKey: 'chat.claudeCode.off.too_many_turns', messageValues: { n: 3 },
    });
    expect(claudeCodeRefusal('too_many_turns', 'ws-1')).toEqual({ reread: false, link: null, messageKey: null });
  });

  it('keeps the server words for a reason the app does not word', () => {
    expect(claudeCodeRefusal('signed_out', null).messageKey).toBeNull();
    expect(claudeCodeRefusal('server', null)).toEqual({ reread: false, link: null, messageKey: null });
  });
});
