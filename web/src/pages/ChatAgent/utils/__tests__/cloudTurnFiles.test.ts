// @vitest-environment node
/**
 * A file card of a Home turn that ran on the cloud opens the copy the first
 * hand-back after the turn carried, in its folder in Home, or the stand-in's
 * own copy while none has.
 */
import { describe, expect, it } from 'vitest';

import type { HomeFallback } from '@/types/sse';

import { placeCloudTurnFiles, placeCloudTurns } from '../cloudTurnFiles';
import type { HomeHandbacks } from '../api/workspaces';

const FALLBACK: HomeFallback = {
  computer_id: 'c-mac',
  computer_name: 'Office Mac',
  cloud_computer_id: 'c-cloud',
  cloud_name: 'Cloud One',
  at: '2026-11-15T09:00:00Z',
};

const HANDBACKS: HomeHandbacks = {
  stand_in_workspace_id: 'ws-stand-in',
  handbacks: [
    // Before the turn: an older report.md of the same name, not this turn's.
    { through: '2026-11-14T20:00:00Z', folder: 'From the cloud/2026-11-14', files: ['report.md'] },
    { through: '2026-11-15T10:00:00Z', folder: 'From the cloud/2026-11-15', files: ['report.md', 'data/prices.csv'] },
    { through: '2026-11-15T12:00:00Z', folder: 'From the cloud/2026-11-15 2', files: ['report.md', 'later.md'] },
  ],
};

describe('placeCloudTurnFiles', () => {
  it('opens the copy the first hand-back after the turn carried', () => {
    const placed = placeCloudTurnFiles([{ path: './report.md' }, { path: 'data/prices.csv' }], FALLBACK, HANDBACKS);
    expect(placed).toEqual([
      { path: 'From the cloud/2026-11-15/report.md' },
      { path: 'From the cloud/2026-11-15/data/prices.csv' },
    ]);
  });

  it("opens the stand-in's own copy while no hand-back has carried it", () => {
    expect(placeCloudTurnFiles([{ path: 'draft.md' }], FALLBACK, HANDBACKS)).toEqual([
      { path: 'draft.md', workspaceId: 'ws-stand-in' },
    ]);
  });

  it('leaves a file of another workspace where it is', () => {
    const file = { path: 'notes.md', workspaceId: 'ws-semis' };
    expect(placeCloudTurnFiles([file], FALLBACK, HANDBACKS)).toEqual([file]);
  });
});

describe('placeCloudTurns', () => {
  it('places only the cloud turns, and says when one of their files is still on the cloud', () => {
    const files = new Map([
      [0, [{ path: 'report.md' }]],
      [1, [{ path: 'report.md' }]],
      [2, [{ path: 'draft.md' }]],
    ]);
    const cloudTurns = new Map([[1, FALLBACK], [2, FALLBACK]]);
    const { placed, unplaced } = placeCloudTurns(files, cloudTurns, HANDBACKS);
    expect(placed.get(0)).toEqual([{ path: 'report.md' }]);
    expect(placed.get(1)).toEqual([{ path: 'From the cloud/2026-11-15/report.md' }]);
    expect(placed.get(2)).toEqual([{ path: 'draft.md', workspaceId: 'ws-stand-in' }]);
    expect(unplaced).toBe(true);
    expect(placeCloudTurns(files, new Map([[1, FALLBACK]]), HANDBACKS).unplaced).toBe(false);
  });
});
