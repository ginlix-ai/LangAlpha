/**
 * A Claude Code refusal says how it is fixed: a token Claude refused is fixed
 * with a new one in Settings, as no token is, and the app words it itself.
 * On a cloud computer, the choice of how it runs there or the Anthropic API
 * key it runs on fixes it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ANTHROPIC_KEY_URL, CLAUDE_CODE_CHOICE_URL, claudeCodeRefusal, claudeCodeTokenUrl } from '../claudeCode';

const build = vi.hoisted(() => ({ planUrl: null as string | null }));

vi.mock('@/config/supportLinks', async (original) => ({
  ...(await original<typeof import('@/config/supportLinks')>()),
  get PLAN_URL() { return build.planUrl; },
}));

describe('claudeCodeRefusal', () => {
  beforeEach(() => {
    build.planUrl = null;
  });

  it('links a refused token to its sheet, reads the tokens again, and words it in the app', () => {
    expect(claudeCodeRefusal({ reason: 'token_refused' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: claudeCodeTokenUrl('ws-1'), label: 'Add setup token', labelKey: 'modelAccess.claudeCode.add' },
      messageKey: 'chat.claudeCode.off.token_refused',
    });
  });

  it('links a cloud computer kept off to Model access, and one without a key it can use to the Anthropic key, reading them again', () => {
    expect(claudeCodeRefusal({ reason: 'hosted_off' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.hosted_off',
    });
    expect(claudeCodeRefusal({ reason: 'needs_api_key' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: ANTHROPIC_KEY_URL, label: 'Add an API key', labelKey: 'chat.errorLinkAddApiKey' },
      messageKey: 'chat.claudeCode.off.needs_api_key',
    });
    expect(claudeCodeRefusal({ reason: 'api_key_endpoint' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: ANTHROPIC_KEY_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.api_key_endpoint',
    });
  });

  it('links Claude Code off for the account to its view under Model access, reading the opt-in again', () => {
    expect(claudeCodeRefusal({ reason: 'account' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.account',
    });
  });

  it('links automations on a stored token to Model access, reading nothing again', () => {
    expect(claudeCodeRefusal({ reason: 'stored_unattended' }, 'ws-1')).toEqual({
      reread: false,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.stored_unattended',
    });
  });

  it.each(['hosted_paused', 'stored_blocked', 'choice_changed', 'needs_bigger_computer'])('words %s in the app with nothing to fix, and reads the choice again', (reason) => {
    expect(claudeCodeRefusal({ reason }, 'ws-1')).toEqual({ reread: true, link: null, messageKey: `chat.claudeCode.off.${reason}` });
  });

  it('where the build names a plan page, words a cloud computer too small for Claude Code as a plan and links that page', () => {
    build.planUrl = '/account/plans';
    expect(claudeCodeRefusal({ reason: 'needs_bigger_computer' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: '/account/plans', label: 'Upgrade plan', labelKey: 'computer.spec.upgradePlan', external: true },
      messageKey: 'chat.claudeCode.off.needs_bigger_computer_plan',
    });
    expect(claudeCodeRefusal({ reason: 'hosted_paused' }, 'ws-1')).toEqual({ reread: true, link: null, messageKey: 'chat.claudeCode.off.hosted_paused' });
  });

  it('words a refused API key mid-turn in the app, with nothing to link', () => {
    expect(claudeCodeRefusal({ reason: 'api_key_refused' }, 'ws-1')).toEqual({ reread: false, link: null, messageKey: 'chat.claudeCode.off.api_key_refused' });
  });

  it("names the user's cap on turns at once, and keeps the server words without one", () => {
    expect(claudeCodeRefusal({ reason: 'too_many_turns', limit: 3 }, 'ws-1')).toEqual({
      reread: false, link: null, messageKey: 'chat.claudeCode.off.too_many_turns', messageValues: { n: 3 },
    });
    expect(claudeCodeRefusal({ reason: 'too_many_turns' }, 'ws-1')).toEqual({ reread: false, link: null, messageKey: null });
  });

  it("reads the plan again at a Claude plan's limit, carrying its window and reset for the card to word", () => {
    const until = '2026-11-06T20:40:00Z';
    expect(claudeCodeRefusal({ reason: 'plan_stopped', window: 'five_hour', until, credits_billed: true }, 'ws-1')).toEqual({
      reread: true, link: null, messageKey: null,
      planRefusal: { reason: 'plan_stopped', window: 'five_hour', until, creditsBilled: true },
    });
    expect(claudeCodeRefusal({ reason: 'plan_limited', window: 'seven_day_overage_included', until }, 'ws-1').planRefusal)
      .toEqual({ reason: 'plan_limited', window: 'seven_day_overage_included', until, creditsBilled: false });
    expect(claudeCodeRefusal({ reason: 'plan_limited', window: 'five_hour' }, 'ws-1')).toEqual({ reread: true, link: null, messageKey: null });
  });

  it('keeps the server words for a reason the app does not word', () => {
    expect(claudeCodeRefusal({ reason: 'signed_out' }, null).messageKey).toBeNull();
    expect(claudeCodeRefusal({ reason: 'server' }, null)).toEqual({ reread: false, link: null, messageKey: null });
  });
});
