// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { sharedRefusal } from '../sharedWorkspaceRefusal';

const refused = (status: number, detail: unknown) => ({ response: { status, data: { detail } } });

describe('sharedRefusal', () => {
  it('sends a refusal by rule to the rule row', () => {
    const got = sharedRefusal(refused(403, {
      type: 'policy_denied', rule: 'shared_workspaces.create', set_by: 'organization', message: 'No.',
    }));
    expect(got.kind).toBe('rule');
    expect(got.kind === 'rule' && got.refusal.rule).toBe('shared_workspaces.create');
  });

  it.each([
    ['already_member', 409, 'sharedWorkspace.refusal.alreadyMember'],
    ['last_manager', 409, 'sharedWorkspace.refusal.lastManager'],
    ['shared_workspace_manager_only', 403, 'sharedWorkspace.refusal.managersOnly'],
    ['no_cloud_computer', 409, 'sharedWorkspace.refusal.noCloudComputer'],
    ['too_many_members', 400, 'sharedWorkspace.refusal.tooMany'],
    ['team_not_yours', 403, 'sharedWorkspace.refusal.teamNotYours'],
    ['team_not_found', 400, 'sharedWorkspace.refusal.teamNotFound'],
  ])('words %s', (type, status, key) => {
    expect(sharedRefusal(refused(status, { type, message: 'server words' }))).toEqual({ kind: 'text', key });
  });

  it('names the address an organization does not have, from the refusal or the request', () => {
    expect(sharedRefusal(refused(400, { type: 'member_not_found', email: 'x@other.test' }), 'typed@other.test'))
      .toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.notInOrganization', values: { email: 'x@other.test' } });
    expect(sharedRefusal(refused(400, { type: 'member_not_found' }), 'typed@other.test'))
      .toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.notInOrganization', values: { email: 'typed@other.test' } });
  });

  it("says the organization's cap", () => {
    expect(sharedRefusal(refused(409, { type: 'shared_workspace_limit', limit: 3 })))
      .toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.limit', values: { limit: 3 } });
  });

  it('reads an absent route or a lost listing as no access, and an unreadable account as unavailable', () => {
    expect(sharedRefusal(refused(404, 'Not Found'))).toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.noAccess' });
    expect(sharedRefusal(refused(403, 'Forbidden'))).toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.noAccess' });
    expect(sharedRefusal(refused(503, 'Unavailable'))).toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.unavailable' });
  });

  it("words a refused name, and anything else as the generic failure", () => {
    expect(sharedRefusal(refused(400, 'A workspace with that name already exists')))
      .toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.badName' });
    expect(sharedRefusal(new Error('Network Error'))).toEqual({ kind: 'text', key: 'sharedWorkspace.refusal.generic' });
  });
});
