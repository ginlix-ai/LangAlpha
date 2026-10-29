// @vitest-environment node
/**
 * When a move that could not copy off a machine is offered again from the
 * workspace's backup: only off a machine the user brought that did not
 * answer, onto a target that can take the workspace now.
 */
import { describe, it, expect } from 'vitest';

import type { Computer } from '@/types/api';

import { backupRestoreOffer, type MoveAttempt } from '../moveFromBackup';

const BOX = 'a3c1e8f2-4b6d-4e0a-9c7b-1d2e3f4a5b6c';
const CLOUD = '7e9f1a2b-3c4d-4e5f-8a9b-0c1d2e3f4a5b';
const OTHER_CLOUD = 'c2d3e4f5-a6b7-4c8d-9e0f-1a2b3c4d5e6f';

type Row = Pick<Computer, 'computer_id' | 'kind' | 'status' | 'provider_ref'>;

const box: Row = { computer_id: BOX, kind: 'ssh', status: 'error', provider_ref: 'me@198.51.100.7' };
const cloud: Row = { computer_id: CLOUD, kind: 'daytona', status: 'running', provider_ref: 'sbx-cloud' };
const otherCloud: Row = { computer_id: OTHER_CLOUD, kind: 'daytona', status: 'stopped', provider_ref: 'sbx-other' };

const failed: MoveAttempt = { code: 'source_unavailable', sourceId: BOX, targetId: CLOUD, fromBackup: false };

describe('backupRestoreOffer', () => {
  it('offers a restore onto the same target when the brought machine did not answer', () => {
    expect(backupRestoreOffer(failed, [box, cloud], BOX)).toEqual({ sourceId: BOX, targetId: CLOUD });
  });

  it.each([
    ['another failure', { ...failed, code: 'target_unreachable' }],
    ['a failure with no code', { ...failed, code: null }],
    ['a restore from the backup that failed itself', { ...failed, fromBackup: true }],
  ])('offers nothing after %s', (_what, attempt) => {
    expect(backupRestoreOffer(attempt, [box, cloud], BOX)).toBeNull();
  });

  it('offers nothing off a hosted machine, which is the service\'s to bring back', () => {
    const attempt = { ...failed, sourceId: OTHER_CLOUD };
    expect(backupRestoreOffer(attempt, [otherCloud, cloud], OTHER_CLOUD)).toBeNull();
  });

  it('offers nothing once the workspace is no longer on the machine that failed', () => {
    expect(backupRestoreOffer(failed, [box, cloud], CLOUD)).toBeNull();
  });

  it('offers nothing onto a target that cannot take a workspace now', () => {
    expect(backupRestoreOffer(failed, [box, { ...cloud, status: 'error' }], BOX)).toBeNull();
    expect(backupRestoreOffer(failed, [box], BOX)).toBeNull();
  });

  it('offers an earlier failure only while the machine still is not running', () => {
    const earlier = { ...failed, earlier: true };
    expect(backupRestoreOffer(earlier, [box, cloud], BOX)).toEqual({ sourceId: BOX, targetId: CLOUD });
    expect(backupRestoreOffer(earlier, [{ ...box, status: 'running' }, cloud], BOX)).toBeNull();
    // A failure this dialog just saw is offered whatever the status says.
    expect(backupRestoreOffer(failed, [{ ...box, status: 'running' }, cloud], BOX)).not.toBeNull();
  });
});
