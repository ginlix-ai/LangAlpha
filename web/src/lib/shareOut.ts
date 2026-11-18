import type { TFunction } from 'i18next';
import type { ShareOutState } from '@/hooks/useSharedWorkspaces';

/** What a share control says when a shared workspace's managers hold it: a
 *  member's link is not theirs to change, or under "no one" the link opens
 *  only for the workspace's people. Null when nothing narrows it.
 *  `organization` is the organization's name or "your organization". */
export function shareOutLine(t: TFunction, state: ShareOutState, organization: string): string | null {
  if (state.rule === 'none') {
    return state.people != null
      ? t('sharedWorkspace.shareOut.noneLink', { count: state.people })
      : t('sharedWorkspace.shareOut.noneLinkUncounted');
  }
  if (!state.canChangeLinks) {
    return t('sharedWorkspace.shareOut.managersLink', { organization });
  }
  return null;
}
