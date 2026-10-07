import { createElement, useCallback } from 'react';
import { queryOptions, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { House, Layers, Zap, type LucideProps } from 'lucide-react';
import { queryKeys } from '@/lib/queryKeys';
import { getFlashWorkspace } from '@/pages/ChatAgent/utils/api';
import { useAllWorkspacesAgent } from './useAllWorkspacesAgent';

/**
 * The query every surface reading `workspaces.flash()` shares, so none can
 * fetch it without the catalog refresh below.
 *
 * The request upserts the row, and the insert switches Flash off for every
 * server new workspaces start without, which a catalog read from before it
 * lacks. A fetch with the row already cached cannot be that insert, so only
 * one on an empty cache refetches the catalog.
 */
export function flashWorkspaceQuery(queryClient: QueryClient) {
  return queryOptions({
    queryKey: queryKeys.workspaces.flash(),
    queryFn: async () => {
      const first = queryClient.getQueryData(queryKeys.workspaces.flash()) === undefined;
      const workspace = await getFlashWorkspace();
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
 * The Flash workspace as a scope-control option, or `undefined` until it
 * resolves.
 *
 * Flash runs on one per-user workspace that the gallery listing hides. It is
 * upserted on first use, so ensure it here: a server can be switched off for
 * Flash before the user ever opens a Flash chat. Under the all-workspaces
 * agent the row is the Chief of Staff's Home, named so here because "All
 * workspaces" already means every workspace in a plugin's scope.
 */
export function useFlashWorkspace(): { id: string; name: string } | undefined {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const allWorkspaces = useAllWorkspacesAgent();
  const { data } = useQuery(flashWorkspaceQuery(queryClient));
  const id = data?.workspace_id;
  if (!id) return undefined;
  return { id, name: allWorkspaces ? t('agents.home') : t('plugins.scope.flash') };
}

/**
 * What a navigation into the flash workspace tells the chat: Flash's chats
 * need its agent mode and status, since nothing else names them before the
 * row loads. Under the all-workspaces agent the thread view reads the same
 * hint as Home (resolveChatMode).
 */
export const FLASH_ROUTE_STATE = { agentMode: 'flash', workspaceStatus: 'flash' } as const;

/**
 * The flash row as a plugin row's scope option, by the rule every plugin
 * surface shares. Flash has no sandbox, so it can install only directly bound
 * tools, and offering it on a row with none is a switch that does nothing.
 * Home runs the full agent, so under the all-workspaces agent every row
 * offers it.
 */
export function useFlashScope(): (hasDirectTools: boolean | null | undefined) => { id: string; name: string } | undefined {
  const workspace = useFlashWorkspace();
  const allWorkspaces = useAllWorkspacesAgent();
  return useCallback(
    (hasDirectTools) => (allWorkspaces || hasDirectTools ? workspace : undefined),
    [allWorkspaces, workspace],
  );
}

export interface FlashGlyphProps extends LucideProps {
  /** Which product the row stands for, from the flag or whatever stands in
   *  for it where the features query cannot run. */
  allWorkspaces: boolean;
  /** Fills Flash's bolt, as surfaces that paint it solid do. The stack and
   *  the house are outlines either way. */
  solid?: boolean;
  /** Where the row is named Home, as in a plugin's scope (useFlashWorkspace):
   *  the house that name goes with, since "All workspaces" there already
   *  means every workspace. */
  home?: boolean;
}

/** The flash row's glyph: Flash's bolt, or the stack that reads as All
 *  workspaces. Takes the flag so a surface without the features query (the
 *  onboarding preview) can draw it too. */
export function FlashGlyph({ allWorkspaces, solid = false, home = false, ...props }: FlashGlyphProps) {
  if (allWorkspaces) return createElement(home ? House : Layers, props);
  return createElement(Zap, solid ? { fill: 'currentColor', ...props } : props);
}

/** FlashGlyph for the signed-in user. A component rather than a hook that
 *  hands one back: the React Compiler reads a component a hook returned as
 *  one created during render. */
export function FlashRowIcon(props: Omit<FlashGlyphProps, 'allWorkspaces'>) {
  return createElement(FlashGlyph, { allWorkspaces: useAllWorkspacesAgent(), ...props });
}
