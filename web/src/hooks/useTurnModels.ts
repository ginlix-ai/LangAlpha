import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useAllModels } from './useAllModels';
import { isClaudeCodeModel } from '@/lib/claudeCode';
import { useClaudeCodeSetup } from './useClaudeCode';
import { homeWorkspaceQuery } from './useHomeWorkspace';
import { useDefaultModel } from './useSlotDefaultModels';
import { useTurnClaudeCode } from './useWorkspaceClaudeCode';

/**
 * The Default a turn starts on: the account's, except on a Chief of Staff
 * turn (`home`) with a Claude Code Default the server does not run there
 * (`ClaudeCodeSetup.chief_of_staff.default_runs`, read only while the setup
 * says Claude Code is `on`, which turning it off writes at once), which
 * starts on the deployment's. Until the server says, such a Default names no
 * model, as an unread preference does, and the server resolves it.
 */
export function useTurnDefaultModel(home: boolean): string | null {
  const { metadata, systemDefaults } = useAllModels();
  const accountDefault = useDefaultModel();
  const { setup, isLoading, unread } = useClaudeCodeSetup({ enabled: home });
  const claudeCode = home && !!accountDefault && isClaudeCodeModel(metadata[accountDefault]);
  const runs = !!setup?.on && !!setup.chief_of_staff?.default_runs;
  if (!claudeCode) return accountDefault;
  if (isLoading || unread) return null;
  return runs ? accountDefault : systemDefaults?.default_model ?? null;
}

/**
 * The models a composer offers, the model it starts on, and where Claude Code
 * stands for the turn its send starts: in the selected workspace, or where the
 * server places the Chief of Staff's CLI (`home`). Home's id is read only in
 * Home, for the link that adds a token for its turns, since Settings opens
 * that sheet by workspace; elsewhere the read would upsert Home and refetch
 * the MCP catalog for nothing.
 */
export function useTurnModels(home: boolean, selectedWorkspaceId: string | null) {
  const { models } = useAllModels();
  const defaultModel = useTurnDefaultModel(home);
  const queryClient = useQueryClient();
  const { data: homeId = null } = useQuery({ ...homeWorkspaceQuery(queryClient), enabled: home, select: (w) => w.workspace_id });
  const claudeCode = useTurnClaudeCode(home, selectedWorkspaceId);
  return { models, defaultModel, workspaceId: home ? homeId : selectedWorkspaceId, claudeCode };
}
