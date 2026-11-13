/**
 * A Claude Code refusal says how it is fixed: a token Claude refused is fixed
 * with a new one in Settings, as no token is, and the app words it itself.
 * Where the CLI runs on an own computer, Claude Code's view fixes none being
 * online, and the offline one is named.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { CLAUDE_CODE_CHOICE_URL, claudeCodeRefusal, claudeCodeTokenUrl } from '../claudeCode';

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

  it('links no own computer online with its own token to Claude Code under Model access, reading it again', () => {
    expect(claudeCodeRefusal({ reason: 'no_own_computer' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.no_own_computer',
    });
  });

  it('names the offline computer that runs Claude Code, reading the computers again, and keeps the server words without it', () => {
    expect(claudeCodeRefusal({ reason: 'cli_computer_offline', name: 'Studio Mac' }, 'ws-1')).toEqual({
      reread: true, link: null, messageKey: 'chat.claudeCode.off.cli_computer_offline', messageValues: { name: 'Studio Mac' },
    });
    expect(claudeCodeRefusal({ reason: 'cli_computer_offline' }, 'ws-1')).toEqual({ reread: true, link: null, messageKey: null });
  });

  it('links Claude Code off for the account to its view under Model access, reading the opt-in again', () => {
    expect(claudeCodeRefusal({ reason: 'account' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: CLAUDE_CODE_CHOICE_URL, label: 'Model access', labelKey: 'modelAccess.title' },
      messageKey: 'chat.claudeCode.off.account',
    });
  });

  it.each(['choice_changed', 'needs_bigger_computer'])('words %s in the app with nothing to fix, and reads the choice again', (reason) => {
    expect(claudeCodeRefusal({ reason }, 'ws-1')).toEqual({ reread: true, link: null, messageKey: `chat.claudeCode.off.${reason}` });
  });

  it('where the build names a plan page, words a cloud computer too small for Claude Code as a plan and links that page', () => {
    build.planUrl = '/account/plans';
    expect(claudeCodeRefusal({ reason: 'needs_bigger_computer' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: '/account/plans', label: 'Upgrade plan', labelKey: 'computer.spec.upgradePlan', console: true },
      messageKey: 'chat.claudeCode.off.needs_bigger_computer_plan',
    });
  });

  it('where the refusal names the computer too small, links what gets that computer to Medium', () => {
    build.planUrl = '/account/plans';
    expect(claudeCodeRefusal({ reason: 'needs_bigger_computer', computer_id: 'c-3' }, 'ws-1')).toEqual({
      reread: true,
      link: { url: '?resize=c-3', label: 'Change spec', labelKey: 'workspace.changeSpec', resize: 'c-3' },
      messageKey: 'chat.claudeCode.off.needs_bigger_computer_plan',
    });
  });

  it("words Home's computer too small for Claude Code as Home's, with or without a plan page", () => {
    expect(claudeCodeRefusal({ reason: 'needs_bigger_computer', home: true }, 'home-1').messageKey).toBe('chat.claudeCode.off.needs_bigger_computer_home');
    build.planUrl = '/account/plans';
    expect(claudeCodeRefusal({ reason: 'needs_bigger_computer', home: true }, 'home-1')).toMatchObject({
      link: { url: '/account/plans' }, messageKey: 'chat.claudeCode.off.needs_bigger_computer_plan_home',
    });
  });

  it('keeps the server words for the refusals cloud computers no longer give', () => {
    for (const reason of ['hosted_off', 'stored_blocked']) {
      expect(claudeCodeRefusal({ reason }, 'ws-1')).toEqual({ reread: false, link: null, messageKey: null });
    }
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
