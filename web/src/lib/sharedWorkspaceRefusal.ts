/**
 * What a shared workspace's routes refuse, in the app's words. Each refusal a
 * create or a members change can meet maps to one reading here, so the
 * dialog and the members panel word them the same, and a refusal by rule
 * goes to the shared rule row instead.
 */
import { apiErrorDetail, apiErrorObject, apiErrorStatus } from '@/api/errors';
import { readRuleRefusal, type RuleRefusal } from './ruleRefusal';

export type SharedRefusal =
  | { kind: 'rule'; refusal: RuleRefusal }
  | { kind: 'text'; key: string; values?: Record<string, unknown> };

/** The refusal `err` carries for a shared workspace's create or members
 *  change. `email` is the address the request named, for a refusal about it. */
export function sharedRefusal(err: unknown, email?: string): SharedRefusal {
  const rule = readRuleRefusal(apiErrorDetail(err));
  if (rule) return { kind: 'rule', refusal: rule };
  const detail = apiErrorObject(err);
  const type = typeof detail?.type === 'string' ? detail.type : null;
  const status = apiErrorStatus(err);
  switch (type) {
    case 'member_not_found':
      return { kind: 'text', key: 'sharedWorkspace.refusal.notInOrganization', values: { email: (detail?.email as string) ?? email ?? '' } };
    case 'already_member':
      return { kind: 'text', key: 'sharedWorkspace.refusal.alreadyMember' };
    case 'last_manager':
      return { kind: 'text', key: 'sharedWorkspace.refusal.lastManager' };
    case 'shared_workspace_manager_only':
      return { kind: 'text', key: 'sharedWorkspace.refusal.managersOnly' };
    case 'shared_workspace_limit':
      return { kind: 'text', key: 'sharedWorkspace.refusal.limit', values: { limit: detail?.limit ?? '' } };
    case 'no_cloud_computer':
      return { kind: 'text', key: 'sharedWorkspace.refusal.noCloudComputer' };
    case 'too_many_members':
      return { kind: 'text', key: 'sharedWorkspace.refusal.tooMany' };
    case 'team_not_yours':
      return { kind: 'text', key: 'sharedWorkspace.refusal.teamNotYours' };
    case 'team_not_found':
      return { kind: 'text', key: 'sharedWorkspace.refusal.teamNotFound' };
  }
  if (status === 503) return { kind: 'text', key: 'sharedWorkspace.refusal.unavailable' };
  // Not listed any more, or the feature went off: the workspace is gone for them.
  if (status === 403 || status === 404) return { kind: 'text', key: 'sharedWorkspace.refusal.noAccess' };
  if (status === 400 && typeof apiErrorDetail(err) === 'string') {
    return { kind: 'text', key: 'sharedWorkspace.refusal.badName' };
  }
  return { kind: 'text', key: 'sharedWorkspace.refusal.generic' };
}
