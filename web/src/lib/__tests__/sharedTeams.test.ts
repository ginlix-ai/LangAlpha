// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';
import { managesIt } from '../sharedTeams';

const entry = (shared: Partial<SharedWorkspaceEntry['shared']>) => ({ shared }) as SharedWorkspaceEntry;

describe('managesIt', () => {
  it('reads a listed manager or an admin of its team as running it, not an acting member', () => {
    expect(managesIt(entry({ role: 'manager' }))).toBe(true);
    expect(managesIt(entry({ role: 'member', team_admin: true, listed: false }))).toBe(true);
    expect(managesIt(entry({ role: 'member', can_manage: true }))).toBe(false);
    expect(managesIt(entry({ role: 'member' }))).toBe(false);
  });
});
