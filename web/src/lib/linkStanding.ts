/**
 * Where a link stands against the rules that govern it, for the thread's
 * Share panel and the file's Publish dialog alike: the account's rule on
 * public links, and in a shared workspace its managers' rule on what leaves
 * it.
 *
 * A shared workspace's links open to its organization's members only: the
 * server offers its holder no public audience, a thread's switch turns the
 * members' link on, and a file's first publish takes that audience itself.
 * The two surfaces differ in how they reach a members' link. A thread's
 * switch takes it when public links are off, so its picker shows once the
 * link is on; a file is published under the audience its picker chose, so
 * the picker shows before, and a shared workspace's file, which can take no
 * other, shows none.
 */
import type { ShareAudience } from '@/types/api';
import type { ShareOutState } from '@/hooks/useSharedWorkspaces';

export type LinkSurface = 'thread' | 'file';

export type LinkNote =
  /** The workspace's managers hold the link, or keep it to the workspace. */
  | { kind: 'shareOut'; state: ShareOutState }
  /** A rule on public links, worded by `key`. */
  | { kind: 'rule'; key: string; values?: Record<string, string> };

export interface LinkStanding {
  /** The link opens, or will open, only to members. */
  membersOnly: boolean;
  /** A member held by the workspace's managers reads the link but changes nothing. */
  lockedOut: boolean;
  /** Under the managers' "no one" the link opens for the workspace's people. */
  keptIn: boolean;
  /** `paused`: shared, but opening to anyone, which a rule refuses, so
   *  visitors wait. `blocked`: not shared, and nothing here can share it. */
  state: 'open' | 'paused' | 'blocked';
  /** Turning the thread's link on makes it the members' one. */
  turnsOnToMembers: boolean;
  picker: { value: ShareAudience; publicAllowed: boolean } | null;
  note: LinkNote | null;
}

export function linkStanding({ surface, linkOn, publicLinks, shareOut, audience, audiences, who }: {
  surface: LinkSurface;
  linkOn: boolean;
  /** Whether the account's rules allow a link that opens to anyone. */
  publicLinks: boolean;
  /** Null outside a shared workspace. */
  shareOut: ShareOutState | null;
  audience: ShareAudience | undefined;
  audiences: readonly ShareAudience[] | undefined;
  /** Who set the rule on public links, to start a sentence and inside one. */
  who: { start: string; mid: string };
}): LinkStanding {
  const thread = surface === 'thread';
  const inShared = shareOut !== null;
  const publicAllowed = !inShared && publicLinks;
  const lockedOut = inShared && !shareOut.canChangeLinks;
  const keptIn = shareOut?.rule === 'none';
  const offersMembers = audiences?.includes('organization') === true && (thread || !inShared);
  const membersOnly = audience === 'organization' || (!thread && inShared);
  // Opening to anyone, which nothing allows here.
  const held = !publicAllowed && !membersOnly;
  const turnsOnToMembers = thread && !linkOn && held && offersMembers;
  const state = !held || turnsOnToMembers ? 'open' : linkOn ? 'paused' : 'blocked';
  const showsPicker = offersMembers && (!thread || (linkOn ? state === 'paused' || !keptIn : publicAllowed));
  const managersNote: LinkNote | null = shareOut && (lockedOut || keptIn) ? { kind: 'shareOut', state: shareOut } : null;

  let note: LinkNote | null;
  if (thread && linkOn) {
    note = state === 'paused' ? { kind: 'rule', key: 'organization.share.pausedBody', values: { whoMid: who.mid } } : managersNote;
  } else if (managersNote) {
    note = managersNote;
  } else if (inShared) {
    note = { kind: 'rule', key: 'sharedWorkspace.share.membersOnly' };
  } else if (thread && !publicAllowed) {
    note = { kind: 'rule', key: offersMembers ? 'organization.share.membersInstead' : 'organization.share.cantShare', values: { who: who.start } };
  } else if (!thread && state !== 'open') {
    note = { kind: 'rule', key: offersMembers ? 'organization.share.publishMembersInstead' : 'organization.share.publishOff', values: { whoMid: who.mid } };
  } else {
    note = null;
  }

  return {
    membersOnly,
    lockedOut,
    keptIn,
    state,
    turnsOnToMembers,
    picker: showsPicker ? { value: membersOnly ? 'organization' : 'public', publicAllowed } : null,
    note,
  };
}
