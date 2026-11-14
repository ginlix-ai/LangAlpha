/**
 * What gets a cloud computer too small for Claude Code to Medium: the dialog
 * on that size, the plan page where the plan keeps it closed, or the change
 * already running. A self-hosted server reports no quota and has no plan page.
 *
 * `@/config/hostMode` is mocked to platform mode, the one place a plan page
 * and a quota exist. Its plan page is a fragment of this page, the only
 * navigation jsdom performs, so what `go` assigns reads back off the address.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { useEffect, type ReactNode } from 'react';

import type { Computer, ComputerProviderOption, ComputerSpecChange, WorkspaceQuota } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('@/config/supportLinks', async (original) => ({
  ...(await original<typeof import('@/config/supportLinks')>()),
  PLAN_URL: '#plans',
}));

const h = vi.hoisted(() => ({
  computers: [] as unknown[],
  providers: [] as unknown[],
  quota: undefined as unknown,
  openComputerSpec: vi.fn(),
  hostMounted: true,
  openConsolePage: vi.fn(),
}));

vi.mock('../useComputers', () => ({
  useComputers: () => ({ data: { computers: h.computers, total: h.computers.length } }),
  useComputerProviders: () => ({ data: { providers: h.providers } }),
}));
vi.mock('../useTierQuota', () => ({ useTierQuota: () => ({ data: h.quota }) }));
vi.mock('../computerPanelStore', async (original) => ({
  ...(await original<typeof import('../computerPanelStore')>()),
  openComputerSpec: h.openComputerSpec,
  computersHostMounted: () => h.hostMounted,
}));
vi.mock('@/lib/desktop', () => ({ openConsolePage: h.openConsolePage }));

const at = { location: '' };
const Probe = () => {
  const l = useLocation();
  useEffect(() => {
    at.location = l.pathname + l.search;
  });
  return null;
};
const wrapper = ({ children }: { children: ReactNode }) => (
  <MemoryRouter initialEntries={['/chat']}>
    <Probe />
    {children}
  </MemoryRouter>
);

import { useComputersHost } from '../computerPanelStore';
import { claudeCodeSpecRequest, planUrlFor, useMediumAction, useMediumHold } from '../useMediumAction';

const SMALL = { computer_id: 'c-1', kind: 'daytona', name: 'Cloud computer', resource_tier: 'standard', spec_change: null } as unknown as Computer;
const DAYTONA = {
  kind: 'daytona',
  is_default: true,
  allowed: null,
  tiers: [
    { tier: 'standard', cpu: 1, memory_gib: 2, disk_gib: 5 },
    { tier: 'performance', cpu: 2, memory_gib: 4, disk_gib: 10 },
    { tier: 'max', cpu: 2, memory_gib: 8, disk_gib: 10 },
  ],
} as ComputerProviderOption;
const NOT_ON_PLAN: WorkspaceQuota = { performance: { used: 0, limit: 0 }, max: { used: 0, limit: 0 }, xlarge: { used: 0, limit: 0 }, always_on: null };
const ON_PLAN: WorkspaceQuota = { performance: { used: 0, limit: 1 }, max: { used: 0, limit: 1 }, xlarge: { used: 0, limit: 0 }, always_on: null };
const RESIZING = { target_tier: 'performance', from_tier: 'standard', state: 'in_progress', started_at: '2026-11-13T18:00:00Z' } as ComputerSpecChange;

beforeEach(() => {
  vi.clearAllMocks();
  at.location = '';
  h.hostMounted = true;
  h.computers = [SMALL];
  h.providers = [DAYTONA];
  h.quota = undefined;
});

describe('useMediumAction, where nothing applies', () => {
  it('is null without a computer id', () => {
    expect(renderHook(() => useMediumAction(null), { wrapper }).result.current).toBeNull();
  });

  it('is null for a computer not on the list', () => {
    expect(renderHook(() => useMediumAction('c-9'), { wrapper }).result.current).toBeNull();
  });

  it.each(['performance', 'max'])('is null once the computer is %s, whatever the thread still holds', (tier) => {
    h.computers = [{ ...SMALL, resource_tier: tier }];
    expect(renderHook(() => useMediumAction('c-1'), { wrapper }).result.current).toBeNull();
  });

  it("is null where the provider's sizes omit Medium", () => {
    h.providers = [{ ...DAYTONA, tiers: DAYTONA.tiers?.filter((s) => s.tier !== 'performance') }];
    expect(renderHook(() => useMediumAction('c-1'), { wrapper }).result.current).toBeNull();
  });
});

describe('useMediumAction, what gets it to Medium', () => {
  it('shows the change already running, and opens the spec dialog on that computer', () => {
    h.computers = [{ ...SMALL, spec_change: RESIZING }];
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper });
    expect(result.current).toMatchObject({ kind: 'resizing', change: RESIZING });
    act(() => result.current?.go());
    expect(h.openComputerSpec).toHaveBeenCalledWith('c-1', null);
  });

  it('offers nothing while a change takes the computer down to Small, which is no way to Medium', () => {
    const down = { ...RESIZING, target_tier: 'standard', from_tier: 'performance' } as ComputerSpecChange;
    h.computers = [{ ...SMALL, spec_change: down }];
    h.providers = [DAYTONA];
    h.quota = ON_PLAN;
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper });
    expect(result.current).toBeNull();
  });

  it('opens the spec dialog on Medium where the plan allows it', () => {
    h.quota = ON_PLAN;
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper });
    expect(result.current?.kind).toBe('resize');
    act(() => result.current?.go());
    expect(h.openComputerSpec).toHaveBeenCalledWith('c-1', claudeCodeSpecRequest('c-1'));
    expect(claudeCodeSpecRequest('c-1')).toEqual({
      tier: 'performance', planUrl: planUrlFor('c-1'), leadKey: 'modelAccess.claudeCode.medium.rowPlan',
    });
  });

  it('opens the spec dialog where the server reports no quota, as a self-hosted one does', () => {
    h.quota = undefined;
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper });
    expect(result.current?.kind).toBe('resize');
  });

  it('sends the user to the plan page, asking to come back to Claude Code with the dialog open, where Medium is off the plan', () => {
    h.quota = NOT_ON_PLAN;
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper });
    expect(result.current?.kind).toBe('plan');
    act(() => result.current?.go());
    expect(h.openConsolePage).toHaveBeenCalledWith(planUrlFor('c-1'), { inPlace: false });
    expect(h.openConsolePage).toHaveBeenCalledWith(
      '#plans?return=' + encodeURIComponent('/settings?tab=model&connect=claude-code-cloud&resize=c-1&plan_changed=1'),
      { inPlace: false },
    );
    expect(h.openComputerSpec).not.toHaveBeenCalled();
  });

  it('opens the plan page in its own tab where the page on screen has nothing to lose', () => {
    h.quota = NOT_ON_PLAN;
    const { result } = renderHook(() => {
      useComputersHost(true);
      return useMediumAction('c-1');
    }, { wrapper });
    act(() => result.current?.go());
    expect(h.openConsolePage).toHaveBeenCalledWith(planUrlFor('c-1'), { inPlace: true });
  });

  it('with no dialog host mounted, sends the user to Claude Code with the resize open instead of opening the dialog', () => {
    h.hostMounted = false;
    h.quota = ON_PLAN;
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper });
    act(() => result.current?.go());
    expect(at.location).toBe('/settings?tab=model&connect=claude-code-cloud&resize=c-1');
    expect(h.openComputerSpec).not.toHaveBeenCalled();
  });
});

describe('useMediumAction, from the setup wizard', () => {
  it('asks the plan page to come back to the setup step, where onboarding left off', () => {
    h.computers = [SMALL];
    h.providers = [DAYTONA];
    h.quota = NOT_ON_PLAN;
    const setup = ({ children }: { children: ReactNode }) => (
      <MemoryRouter initialEntries={['/setup/claude-code']}>{children}</MemoryRouter>
    );
    const { result } = renderHook(() => useMediumAction('c-1'), { wrapper: setup });
    act(() => result.current?.go());
    expect(h.openConsolePage).toHaveBeenCalledWith(
      '#plans?return=' + encodeURIComponent('/setup/claude-code?resize=c-1&plan_changed=1'),
      { inPlace: false },
    );
  });
});

describe('planUrlFor', () => {
  it('encodes the way back, with the computer in it and the plan marked as changed', () => {
    expect(planUrlFor('c 1')).toBe(
      '#plans?return=' + encodeURIComponent('/settings?tab=model&connect=claude-code-cloud&resize=c%201&plan_changed=1'),
    );
  });
});

describe('useMediumHold, a picker row', () => {
  it.each([
    [ON_PLAN, 'resize', 'Needs a Medium or larger cloud computer. Select to change its spec.'],
    [NOT_ON_PLAN, 'plan', "Needs a Medium or larger cloud computer, which isn't on your plan. Select to upgrade."],
  ] as const)('says what picking it does (%#: %s), and picking runs it', (quota, _kind, words) => {
    h.quota = quota;
    const { result } = renderHook(() => useMediumHold('c-1'), { wrapper });
    expect(result.current?.unavailable).toBe(words);
    act(() => result.current?.fix());
    expect(h.openComputerSpec.mock.calls.length + h.openConsolePage.mock.calls.length).toBe(1);
  });

  it('says the change is running while it runs', () => {
    h.computers = [{ ...SMALL, spec_change: RESIZING }];
    expect(renderHook(() => useMediumHold('c-1'), { wrapper }).result.current?.unavailable).toBe('Its computer is changing to Medium.');
  });

  it('holds nothing without a computer', () => {
    expect(renderHook(() => useMediumHold(null), { wrapper }).result.current).toBeNull();
  });
});
