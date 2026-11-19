import { RULE, allows } from '@/lib/accountRules';
import { linkStanding, type LinkStanding, type LinkSurface } from '@/lib/linkStanding';
import type { ShareAudience } from '@/types/api';
import { useAccount } from './useAccount';
import { useShareOut } from './useSharedWorkspaces';

/** `linkStanding` for a link in `workspaceId`, read against the account's
 *  rules and the workspace's managers' choice. */
export function useLinkStanding(workspaceId: string | null | undefined, link: {
  surface: LinkSurface;
  linkOn: boolean;
  audience: ShareAudience | undefined;
  audiences: readonly ShareAudience[] | undefined;
}): LinkStanding {
  const { policy, setterName } = useAccount();
  const shareOut = useShareOut(workspaceId);
  return linkStanding({
    ...link,
    publicLinks: allows(policy, RULE.publicLinks),
    shareOut,
    who: setterName(RULE.publicLinks),
  });
}
