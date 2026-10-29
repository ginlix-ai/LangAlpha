/**
 * Which size a low disk points to. Medium and Large share a 10 GB disk, so the
 * next size up is not always more disk, and the plan may leave a size out.
 *
 * `@/config/hostMode` is mocked to platform mode: a size the plan leaves out
 * only exists there, and `web/.env` would otherwise decide the mode for the run.
 */
import { describe, it, expect, vi } from 'vitest';

import type { ComputerProviderOption, ComputerTierSpec, WorkspaceQuota } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

import { diskUpgradeTier, largerDiskTiers } from '../computerDiskUi';
import { offeredTiers, quotaForSpecChange } from '../tierUi';

// As `GET /computers/providers` lists them: Daytona runs three sizes, E2B four.
const BASE_SPECS: ComputerTierSpec[] = [
  { tier: 'standard', cpu: 1, memory_gib: 2, disk_gib: 5 },
  { tier: 'performance', cpu: 2, memory_gib: 4, disk_gib: 10 },
  { tier: 'max', cpu: 2, memory_gib: 8, disk_gib: 10 },
];
const DAYTONA: ComputerProviderOption = { kind: 'daytona', is_default: true, allowed: null, tiers: BASE_SPECS };
const E2B: ComputerProviderOption = {
  kind: 'e2b',
  is_default: false,
  allowed: true,
  tiers: [...BASE_SPECS, { tier: 'xlarge', cpu: 4, memory_gib: 8, disk_gib: 20 }],
};

// A plan has one slot for a computer bigger than Small, open at every size up
// to its largest, and each size counts every computer held in it.
const PRO: WorkspaceQuota = {
  performance: { used: 0, limit: 1 },
  max: { used: 0, limit: 1 },
  xlarge: { used: 0, limit: 0 },
  always_on: null,
};
const MAX_HOLDING_ONE: WorkspaceQuota = {
  performance: { used: 1, limit: 1 },
  max: { used: 1, limit: 1 },
  xlarge: { used: 1, limit: 1 },
  always_on: null,
};
const FREE: WorkspaceQuota = {
  performance: { used: 0, limit: 0 },
  max: { used: 0, limit: 0 },
  xlarge: { used: 0, limit: 0 },
  always_on: null,
};

describe('largerDiskTiers', () => {
  it('passes over a size up that adds no disk', () => {
    expect(largerDiskTiers('performance', offeredTiers(DAYTONA))).toEqual([]);
    expect(largerDiskTiers('performance', offeredTiers(E2B))).toEqual(['xlarge']);
    expect(largerDiskTiers('standard', offeredTiers(E2B))).toEqual(['performance', 'max', 'xlarge']);
  });

  it('falls back to the next size up when the server lists no specs', () => {
    expect(largerDiskTiers('performance', offeredTiers({ ...DAYTONA, tiers: undefined }))).toEqual(['max']);
    expect(largerDiskTiers('max', offeredTiers(undefined))).toEqual([]);
  });
});

describe('diskUpgradeTier', () => {
  it('points Medium on E2B to Extra Large, and Medium on Daytona nowhere', () => {
    expect(diskUpgradeTier('performance', offeredTiers(E2B), undefined)).toBe('xlarge');
    expect(diskUpgradeTier('performance', offeredTiers(DAYTONA), undefined)).toBeNull();
  });

  it('points a Pro plan on Small to Medium, the smallest size with more disk', () => {
    expect(diskUpgradeTier('standard', offeredTiers(DAYTONA), PRO)).toBe('performance');
  });

  it('points a Medium to Extra Large in the slot it holds', () => {
    const offer = offeredTiers(E2B);
    expect(diskUpgradeTier('performance', offer, MAX_HOLDING_ONE)).toBeNull();
    expect(diskUpgradeTier('performance', offer, quotaForSpecChange(MAX_HOLDING_ONE, 'performance'))).toBe('xlarge');
  });

  it('points nowhere when the plan allows no size with more disk', () => {
    expect(diskUpgradeTier('standard', offeredTiers(E2B), FREE)).toBeNull();
  });
});

describe('quotaForSpecChange', () => {
  it('hands the slot a larger computer holds back to every limited size', () => {
    const quota: WorkspaceQuota = { ...MAX_HOLDING_ONE, xlarge: { used: 1, limit: 0 } };
    expect(quotaForSpecChange(quota, 'max')).toEqual({
      performance: { used: 0, limit: 1 },
      max: { used: 0, limit: 1 },
      // A size the plan leaves out stays out.
      xlarge: { used: 1, limit: 0 },
      always_on: null,
    });
  });

  it('leaves the quota as it is for a Small, which holds no slot', () => {
    expect(quotaForSpecChange(MAX_HOLDING_ONE, 'standard')).toBe(MAX_HOLDING_ONE);
    expect(quotaForSpecChange(undefined, 'max')).toBeUndefined();
  });

  it('keeps unlimited and unknown sizes as they are', () => {
    const quota: WorkspaceQuota = { performance: { used: 3, limit: -1 }, max: null, always_on: null };
    expect(quotaForSpecChange(quota, 'performance')).toEqual({ ...quota, xlarge: undefined });
  });
});
