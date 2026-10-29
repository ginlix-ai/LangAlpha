import { createElement } from 'react';
import { queryOptions, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { Layers, type LucideProps } from 'lucide-react';
import { queryKeys } from '@/lib/queryKeys';
import type { ComposerScope } from '@/components/ui/chat-input.types';
import { getHomeWorkspace } from '@/pages/ChatAgent/utils/api';

/**
 * The query every surface reading `workspaces.home()` shares, so none can
 * fetch it without the catalog refresh below.
 *
 * The request upserts the row, and the insert switches Home off for every
 * server new workspaces start without, which a catalog read from before it
 * lacks. A fetch with the row already cached cannot be that insert, so only
 * one on an empty cache refetches the catalog.
 */
export function homeWorkspaceQuery(queryClient: QueryClient) {
  return queryOptions({
    queryKey: queryKeys.workspaces.home(),
    queryFn: async () => {
      const first = queryClient.getQueryData(queryKeys.workspaces.home()) === undefined;
      const workspace = await getHomeWorkspace();
      if (first) void queryClient.invalidateQueries({ queryKey: queryKeys.mcp.catalog() });
      return workspace;
    },
    // Only its id never changes. Home's computer and folder arrive with its
    // first start or turn, which re-read them into the detail query; read
    // them there.
    staleTime: Infinity,
    // Kept with no reader too, since the app shell reads it only on a click:
    // a fetch on an empty cache counts as the first and refetches the catalog.
    gcTime: Infinity,
  });
}

/**
 * The workspace a chat opened under `scope` runs in: Home under All
 * workspaces, else the selected one, or null with none selected. Home is read
 * through its shared query, which also seeds the row the chat view names Home
 * by. Throws when Home cannot be read, which each caller words its own way.
 */
export function resolveChatWorkspace(queryClient: QueryClient, scope: 'all'): Promise<string>;
export function resolveChatWorkspace(
  queryClient: QueryClient,
  scope: ComposerScope,
  selectedId?: string | null,
): Promise<string | null>;
export async function resolveChatWorkspace(
  queryClient: QueryClient,
  scope: ComposerScope,
  selectedId: string | null = null,
): Promise<string | null> {
  if (scope === 'workspace') return selectedId;
  return (await queryClient.ensureQueryData(homeWorkspaceQuery(queryClient))).workspace_id;
}

/**
 * Home's id, or `undefined` until it resolves.
 *
 * Home runs on one per-user workspace that the gallery listing hides. It is
 * upserted on first use, so ensure it here: a server can be switched off for
 * Home before the user ever opens a chat there.
 */
export function useHomeWorkspaceId(): string | undefined {
  const queryClient = useQueryClient();
  return useQuery(homeWorkspaceQuery(queryClient)).data?.workspace_id;
}

/** Home's glyph, the Chief of Staff's stack: it answers across every workspace. */
export function HomeRowIcon(props: LucideProps) {
  return createElement(Layers, props);
}
