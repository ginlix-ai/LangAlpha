import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useAllModels } from './useAllModels';
import { getClaudeCodeChiefOfStaff } from '@/api/model';
import { isClaudeCodeModel } from '@/lib/claudeCode';
import { queryKeys } from '@/lib/queryKeys';
import { useClaudeCodeSetup } from './useClaudeCode';
import { homeWorkspaceQuery } from './useHomeWorkspace';
import { useDefaultModel } from './useSlotDefaultModels';
import { useTurnClaudeCode } from './useWorkspaceClaudeCode';

/**
 * The Default a turn starts on: the account's, except on a Chief of Staff
 * turn (`home`) with a Claude Code Default the server does not run there
 * (`ClaudeCodeSetup.chief_of_staff.default_runs`, read only while the setup
 * says Claude Code is `on`, which turning it off writes at once), which
 * starts on the deployment's. A Home thread whose row names no model it runs (`threadId`)
 * is asked about too, since the server also starts one that another model
 * last replied on on the deployment's. Until the server says, such a Default
 * names no model, as an unread preference does, and the server resolves it.
 */
export function useTurnDefaultModel(home: boolean, threadId: string | null = null): string | null {
  const { metadata, systemDefaults } = useAllModels();
  const accountDefault = useDefaultModel();
  const { setup, isLoading, unread } = useClaudeCodeSetup({ enabled: home });
  const claudeCode = home && !!accountDefault && isClaudeCodeModel(metadata[accountDefault]);
  const runs = !!setup?.on && !!setup.chief_of_staff?.default_runs;
  const { data: thread } = useQuery({
    queryKey: queryKeys.features.claudeCodeChiefOfStaff(threadId ?? ''),
    queryFn: () => getClaudeCodeChiefOfStaff(threadId ?? ''),
    enabled: claudeCode && runs && !!threadId,
    retry: 1,
  });
  if (!claudeCode) return accountDefault;
  if (isLoading || unread) return null;
  const deployment = systemDefaults?.default_model ?? null;
  if (!runs) return deployment;
  if (!threadId) return accountDefault;
  if (!thread) return null;
  return thread.default_runs ? accountDefault : deployment;
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
