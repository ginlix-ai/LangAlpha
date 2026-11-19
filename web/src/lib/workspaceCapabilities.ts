/**
 * What a workspace offers its surfaces, by whether it is shared. A shared
 * workspace's content is its holder's, whose settings, memos and skills no
 * member reaches, so each of those is withheld there:
 *
 * - memos: its files go to no one's memos.
 * - user memory: every member's agent writes the workspace's memory, and no
 *   member's own memory is shown there.
 * - restore offer: a restore is the folder's owner's to answer.
 * - machine settings: the computer is the holder's.
 * - Claude Code: its session files would land on a member's own computer.
 * - skills: the holder's, whose settings members do not reach yet, so the
 *   slash menu holds the built-in commands alone.
 *
 * While the list of shared workspaces is still read, whether this one is
 * shared is unknown, and everything a shared workspace loses is held back
 * until it is known not to be one: a personal workspace gains it a moment
 * later, where the other way a member would briefly see, or fetch, what is
 * not theirs. The composer's placeholder is the one exception, since a
 * default one claims nothing. With the feature off the entry is null at
 * once, so nothing is held.
 */
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';

export interface WorkspaceCapabilities {
  memos: boolean;
  userMemory: boolean;
  restoreOffer: boolean;
  machineSettings: boolean;
  claudeCode: boolean;
  skills: boolean;
  placeholderKey: 'sharedWorkspace.placeholder' | 'chat.placeholderDefault';
}

/** `entry` is the workspace's shared entry: null for a personal one, and
 *  undefined while the list is read. */
export function capabilitiesOf(entry: SharedWorkspaceEntry | null | undefined): WorkspaceCapabilities {
  const personal = entry === null;
  return {
    memos: personal,
    userMemory: personal,
    restoreOffer: personal,
    machineSettings: personal,
    claudeCode: personal,
    skills: personal,
    placeholderKey: entry ? 'sharedWorkspace.placeholder' : 'chat.placeholderDefault',
  };
}
