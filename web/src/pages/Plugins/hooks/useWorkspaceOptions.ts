import { useTranslation } from 'react-i18next';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import { useHomeWorkspaceId } from '@/hooks/useHomeWorkspace';
import type { ScopeWorkspace } from '../components/ScopeControl';

/**
 * The user's workspaces as the Plugins page consumes them: scope-control
 * options with a display name already resolved, plus the id → name lookup the
 * deck headers read.
 */

export interface WorkspaceOptions {
  /** Real workspaces only. Home is never a member: a surface that folds it in
   * here offers it as a move target and as a bulk destination, neither of
   * which it can be. */
  workspaces: ScopeWorkspace[];
  /** No list in hand yet, still loading or failed. `workspaces` is then empty
   * for want of an answer, not because there are none, so a reach counted
   * against it would read "No workspaces". */
  loading: boolean;
  /** The load failed. The scope controls stay hidden all the same, so the page
   * has to say why and offer the retry. */
  loadFailed: boolean;
  retry: () => void;
  /** Both tiers, so a deck header still resolves a Home-scoped name. */
  nameById: Map<string, string>;
}

/**
 * Home as a scope-control option, or `undefined` until it resolves. It is
 * named Home here, with a house for its glyph, because "All workspaces"
 * already means every workspace in a plugin's scope.
 */
export function useHomeOption(): ScopeWorkspace | undefined {
  const { t } = useTranslation();
  const id = useHomeWorkspaceId();
  return id ? { id, name: t('agents.home') } : undefined;
}

export function useWorkspaceOptions(): WorkspaceOptions {
  const { t } = useTranslation();
  const { data, isError, refetch } = useAllWorkspaces();
  const homeWorkspace = useHomeOption();

  const rows = data?.workspaces ?? [];
  const workspaces: ScopeWorkspace[] = rows.map((w) => ({
    id: w.workspace_id,
    name: w.name || t('plugins.scope.unknownWorkspace'),
  }));
  return {
    workspaces,
    loading: data === undefined,
    loadFailed: data === undefined && isError,
    retry: () => void refetch(),
    nameById: new Map(
      [...workspaces, ...(homeWorkspace ? [homeWorkspace] : [])].map((w) => [w.id, w.name]),
    ),
  };
}
