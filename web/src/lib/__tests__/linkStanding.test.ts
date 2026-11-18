// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { linkStanding } from '../linkStanding';
import type { ShareOutState } from '@/hooks/useSharedWorkspaces';

const who = { start: 'Northfield', mid: 'Northfield' };
const open: ShareOutState = { rule: null, canShareOut: true, canChangeLinks: true, people: null };
const managers: ShareOutState = { rule: 'managers', canShareOut: false, canChangeLinks: false, people: null };
const none: ShareOutState = { rule: 'none', canShareOut: false, canChangeLinks: true, people: 4 };

const base = { linkOn: false, publicLinks: true, shareOut: null, audience: undefined, audiences: ['public'] as const, who };

describe('linkStanding for a thread', () => {
  const thread = (over: Partial<Parameters<typeof linkStanding>[0]>) => linkStanding({ ...base, surface: 'thread', ...over });

  it('leaves a personal account as it always was', () => {
    expect(thread({})).toEqual({
      membersOnly: false, lockedOut: false, keptIn: false, state: 'open', turnsOnToMembers: false, picker: null, note: null,
    });
    expect(thread({ linkOn: true }).state).toBe('open');
  });

  it("offers an organization member the audience, public allowed, before and after the link is on", () => {
    const audiences = ['public', 'organization'] as const;
    expect(thread({ audiences }).picker).toEqual({ value: 'public', publicAllowed: true });
    expect(thread({ audiences, linkOn: true, audience: 'organization' })).toMatchObject({
      membersOnly: true, picker: { value: 'organization', publicAllowed: true }, note: null,
    });
  });

  it('turns the members link on under a rule against public links, and says so', () => {
    const s = thread({ publicLinks: false, audiences: ['public', 'organization'] });
    expect(s).toMatchObject({ state: 'open', turnsOnToMembers: true, picker: null });
    expect(s.note).toEqual({ kind: 'rule', key: 'organization.share.membersInstead', values: { who: 'Northfield' } });
  });

  it("blocks turning one on where the rule leaves nothing to share, and pauses one shared before", () => {
    const blocked = thread({ publicLinks: false });
    expect(blocked.state).toBe('blocked');
    expect(blocked.note).toEqual({ kind: 'rule', key: 'organization.share.cantShare', values: { who: 'Northfield' } });
    const paused = thread({ publicLinks: false, linkOn: true, audiences: ['public', 'organization'] });
    expect(paused).toMatchObject({ state: 'paused', picker: { value: 'public', publicAllowed: false } });
    expect(paused.note).toEqual({ kind: 'rule', key: 'organization.share.pausedBody', values: { whoMid: 'Northfield' } });
  });

  it("in a shared workspace turns the members' link on, then shows its audience with anyone held back", () => {
    const off = thread({ shareOut: open, audiences: ['organization', 'public'] });
    expect(off).toMatchObject({ state: 'open', turnsOnToMembers: true, picker: null });
    expect(off.note).toEqual({ kind: 'rule', key: 'sharedWorkspace.share.membersOnly' });
    const on = thread({ shareOut: open, linkOn: true, audience: 'organization', audiences: ['organization'] });
    expect(on).toMatchObject({ state: 'open', membersOnly: true, picker: { value: 'organization', publicAllowed: false }, note: null });
  });

  it("holds a member the managers hold, and under \"no one\" drops the audience for the workspace's line", () => {
    const held = thread({ shareOut: managers, audiences: ['organization', 'public'] });
    expect(held).toMatchObject({ lockedOut: true, note: { kind: 'shareOut', state: managers } });
    const keptIn = thread({ shareOut: none, linkOn: true, audience: 'organization', audiences: ['organization'] });
    expect(keptIn).toMatchObject({ keptIn: true, picker: null, note: { kind: 'shareOut', state: none } });
  });
});

describe('linkStanding for a file', () => {
  const file = (over: Partial<Parameters<typeof linkStanding>[0]>) => linkStanding({ ...base, surface: 'file', ...over });

  it('leaves a personal account as it always was', () => {
    expect(file({})).toEqual({
      membersOnly: false, lockedOut: false, keptIn: false, state: 'open', turnsOnToMembers: false, picker: null, note: null,
    });
  });

  it('offers the audience before publishing, so an organization member can publish to members under the rule', () => {
    const audiences = ['public', 'organization'] as const;
    const blocked = file({ publicLinks: false, audiences });
    expect(blocked).toMatchObject({ state: 'blocked', picker: { value: 'public', publicAllowed: false } });
    expect(blocked.note).toEqual({ kind: 'rule', key: 'organization.share.publishMembersInstead', values: { whoMid: 'Northfield' } });
    expect(file({ publicLinks: false, audiences, audience: 'organization' })).toMatchObject({ state: 'open', membersOnly: true, note: null });
  });

  it('pauses a public page published before the rule, and blocks publishing one with nothing to offer', () => {
    expect(file({ publicLinks: false, linkOn: true }).state).toBe('paused');
    expect(file({ publicLinks: false }).note).toEqual({ kind: 'rule', key: 'organization.share.publishOff', values: { whoMid: 'Northfield' } });
  });

  it("in a shared workspace publishes to members, with no picker, saying so", () => {
    for (const linkOn of [false, true]) {
      const s = file({ shareOut: open, linkOn, audiences: ['organization', 'public'] });
      expect(s).toMatchObject({ state: 'open', membersOnly: true, picker: null });
      expect(s.note).toEqual({ kind: 'rule', key: 'sharedWorkspace.share.membersOnly' });
    }
  });

  it('says what the managers hold instead, under "managers" and "no one"', () => {
    expect(file({ shareOut: managers })).toMatchObject({ lockedOut: true, note: { kind: 'shareOut', state: managers } });
    expect(file({ shareOut: none, linkOn: true })).toMatchObject({ keptIn: true, note: { kind: 'shareOut', state: none } });
  });
});
