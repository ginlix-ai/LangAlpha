import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import {
  addMember, createSharedWorkspace, getMembers, getTeam, listSharedWorkspaces, removeMember, setEveryone, setMemberRole, setShareOut, setTeam,
} from '@/api/sharedWorkspaces';
import type {
  MembersView, SharedMember, SharedRole, ShareOut, SharedWorkspaceCreate, SharedWorkspaceEntry, TeamPerson,
} from '@/types/sharedWorkspaces';
import { capabilitiesOf, type WorkspaceCapabilities } from '@/lib/workspaceCapabilities';
import { rereadRules } from '@/lib/ruleRefusal';
import { sharedRefusal } from '@/lib/sharedWorkspaceRefusal';
import { useFeatureEnabled } from './useFeatures';
import { useAccount } from './useAccount';

/** Whether shared workspaces are on for this account: an active member of an
 *  organization granted them. Off while the flags load, so nothing flashes in,
 *  and off for a suspended account, which reaches none. */
export function useSharedWorkspacesEnabled(): boolean {
  const flag = useFeatureEnabled('shared_workspaces');
  const { organization, suspended } = useAccount();
  return flag && !!organization?.id && !suspended;
}

/** The caller's shared workspaces and whether they may create one. Read again
 *  on focus: who is in a workspace is changed by other people, and a member
 *  removed from one should find it gone. */
export function useSharedWorkspaces() {
  const enabled = useSharedWorkspacesEnabled();
  const query = useQuery({
    queryKey: queryKeys.sharedWorkspaces.list(),
    queryFn: listSharedWorkspaces,
    enabled,
    staleTime: 30_000,
    refetchOnWindowFocus: true,
  });
  return { ...query, enabled };
}

/** The shared entry for a workspace the caller reaches as a member; null for
 *  a personal workspace (and for every workspace with the feature off), and
 *  undefined while the list is read. */
export function useSharedEntry(workspaceId: string | null | undefined): SharedWorkspaceEntry | null | undefined {
  const { data, enabled, isLoading } = useSharedWorkspaces();
  if (!enabled || !workspaceId) return null;
  if (isLoading && !data) return undefined;
  return data?.workspaces.find((e) => e.workspace.workspace_id === workspaceId) ?? null;
}

/** What the workspace offers its surfaces (`capabilitiesOf`). */
export function useWorkspaceCapabilities(workspaceId: string | null | undefined): WorkspaceCapabilities {
  return capabilitiesOf(useSharedEntry(workspaceId));
}

/** What a shared workspace's managers let leave it, as the caller's share
 *  and publish controls read it: null for a personal workspace (and with the
 *  feature off), so those controls stay as they always were. `rule` is the
 *  choice in force, null while the workspace is open to everyone. `people`
 *  counts who can open its links under "no one", once the list is read. */
export interface ShareOutState {
  rule: ShareOut | null;
  canShareOut: boolean;
  canChangeLinks: boolean;
  people: number | null;
}

export function useShareOut(workspaceId: string | null | undefined): ShareOutState | null {
  const entry = useSharedEntry(workspaceId);
  const shared = entry?.shared;
  const rule = shared && !shared.everyone ? shared.sharing : null;
  const { reach } = useWorkspaceAudience(entry, { people: rule === 'none', roster: rule === 'none' });
  if (!shared) return null;
  return { rule, canShareOut: shared.can_share_out, canChangeLinks: shared.can_change_links, people: reach };
}

/** The team a shared workspace names, as far as it is known. */
export interface AudienceTeam {
  id: string;
  /** Null until someone says it: the list names only the viewer's own teams,
   *  and the roster names it to anyone. A team is never shown by its id. */
  name: string | null;
  /** Everyone in it, inactive people included, once the roster is read. */
  people: TeamPerson[] | undefined;
  loading: boolean;
  failed: boolean;
}

/** Who can open a shared workspace: the team it names and the people added
 *  to it individually. */
export interface WorkspaceAudience {
  team: AudienceTeam | null;
  /** The people added individually, inactive ones included, once read. */
  listed: SharedMember[] | undefined;
  /** Reaches it through the team. */
  inTeam: (userId: string) => boolean;
  /** Everyone who can open it, each once: its active listed people and its
   *  team's. Null until both are read. */
  reach: number | null;
}

/**
 * The workspace's audience, read only as far as a surface needs it: `people`
 * reads who is listed, and `roster` the named team's people, or with
 * `'to name'` only when the list does not say the team's name, which the
 * roster gives anyone. The auth service keeps the roster, and nothing here
 * stores it.
 */
export function useWorkspaceAudience(
  entry: SharedWorkspaceEntry | null | undefined,
  { people, roster }: { people: boolean; roster: boolean | 'to name' },
): WorkspaceAudience {
  const workspaceId = entry?.workspace.workspace_id;
  const members = useWorkspaceMembers(workspaceId, people);
  // The people view's team once it is read, which a change answers first.
  const ref = members.data ? members.data.team ?? null : entry?.shared.team ?? null;
  const read = useWorkspaceTeam(workspaceId, !!ref && (roster === true || (roster === 'to name' && !ref.name)));
  const view = ref && read.data?.id === ref.id ? read.data : undefined;
  const team = ref && {
    id: ref.id,
    name: view?.name ?? ref.name ?? null,
    people: view?.people,
    loading: read.isLoading,
    failed: read.isError,
  };
  const listed = members.data?.members;
  let reach: number | null = null;
  if (listed && (!ref || view)) {
    const ids = new Set(listed.filter((m) => m.active !== false).map((m) => m.user_id));
    for (const p of view?.people ?? []) if (p.active) ids.add(p.user_id);
    reach = ids.size;
  }
  return {
    team,
    listed,
    inTeam: (userId) => !!view?.people.some((p) => p.user_id === userId),
    reach,
  };
}

export function useWorkspaceMembers(workspaceId: string | null | undefined, enabled = true) {
  return useQuery({
    queryKey: queryKeys.sharedWorkspaces.members(workspaceId ?? ''),
    queryFn: () => getMembers(workspaceId!),
    enabled: enabled && !!workspaceId,
    staleTime: 30_000,
    refetchOnWindowFocus: true,
  });
}

/** The team a workspace names and its people. */
function useWorkspaceTeam(workspaceId: string | null | undefined, enabled: boolean) {
  return useQuery({
    queryKey: queryKeys.sharedWorkspaces.team(workspaceId ?? ''),
    queryFn: () => getTeam(workspaceId!),
    enabled: enabled && !!workspaceId,
    staleTime: 30_000,
    retry: false,
  });
}

export function useCreateSharedWorkspace() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: SharedWorkspaceCreate) => createSharedWorkspace(body),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() }),
    onError: (err) => {
      if (sharedRefusal(err).kind === 'rule') rereadRules(queryClient);
    },
  });
}

type MemberChange =
  | { kind: 'add'; email: string; role?: SharedRole }
  | { kind: 'role'; userId: string; role: SharedRole }
  | { kind: 'everyone'; everyone: boolean }
  | { kind: 'sharing'; sharing: ShareOut }
  | { kind: 'team'; teamId: string | null }
  | { kind: 'remove'; userId: string };

/** Every change to who is in a workspace, as one mutation: each answers with
 *  the member list (a removal with nothing), which replaces the cached one,
 *  and the caller's own list is read again since their role may have moved. */
export function useChangeMembers(workspaceId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (change: MemberChange): Promise<MembersView | null> => {
      switch (change.kind) {
        case 'add': return addMember(workspaceId, change.email, change.role);
        case 'role': return setMemberRole(workspaceId, change.userId, change.role);
        case 'everyone': return setEveryone(workspaceId, change.everyone);
        case 'sharing': return setShareOut(workspaceId, change.sharing);
        case 'team': return setTeam(workspaceId, change.teamId);
        case 'remove': await removeMember(workspaceId, change.userId); return null;
      }
    },
    onError: (err) => {
      if (sharedRefusal(err).kind === 'rule') rereadRules(queryClient);
    },
    onSuccess: (view, change) => {
      if (view) queryClient.setQueryData(queryKeys.sharedWorkspaces.members(workspaceId), view);
      else void queryClient.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.members(workspaceId) });
      void queryClient.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.list() });
      // Who is in the team, and which of them are also listed here.
      void queryClient.invalidateQueries({ queryKey: queryKeys.sharedWorkspaces.team(workspaceId) });
      // Who may publish from here may have moved.
      if (change.kind === 'sharing' || change.kind === 'everyone' || change.kind === 'team') {
        void queryClient.invalidateQueries({ queryKey: queryKeys.orgLibrary.all });
      }
    },
  });
}
