import { useTranslation } from 'react-i18next';
import { useWorkspaces } from '@/hooks/useWorkspaces';
import type { ScopeWorkspace } from '../components/ScopeControl';

/**
 * The user's workspaces as the Plugins page consumes them: scope-control
 * options with a display name already resolved, plus the id → name lookup the
 * deck headers read.
 *
 * The workspaces endpoint is untyped, so every list was writing its own
 * `wsData as { workspaces?: … }` assertion — three hand-written shapes that
 * nothing checks against the response. Asserting it once keeps the unchecked
 * part to a single line.
 */

export interface WorkspaceOptions {
  workspaces: ScopeWorkspace[];
  nameById: Map<string, string>;
}

export function useWorkspaceOptions(): WorkspaceOptions {
  const { t } = useTranslation();
  const { data } = useWorkspaces({ limit: 100 });

  const rows =
    (data as { workspaces?: { workspace_id: string; name?: string }[] } | undefined)
      ?.workspaces ?? [];
  const workspaces: ScopeWorkspace[] = rows.map((w) => ({
    id: w.workspace_id,
    name: w.name || t('plugins.scope.unknownWorkspace'),
  }));

  return {
    workspaces,
    nameById: new Map(workspaces.map((w) => [w.id, w.name])),
  };
}
