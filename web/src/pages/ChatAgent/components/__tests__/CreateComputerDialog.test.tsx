/**
 * The New computer form's size picker and its plan locks: the sizes are the
 * picked provider's, described from its server config, and the form falls
 * back to what is open whenever a pick is no longer offered or allowed.
 *
 * `@/config/hostMode` is mocked to platform mode: plan locks only exist
 * there, and `web/.env` would otherwise decide the mode for the run.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { renderWithProviders } from '@/test/utils';
import type { ComputerProviderOption, ComputerTierSpec, WorkspaceQuota } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

vi.mock('../../utils/api', () => ({
  getComputerProviders: vi.fn(),
  // Read only where this Mac is offered, which it never is here.
  getComputers: vi.fn(),
  getWorkspaceQuota: vi.fn(),
  // SSH setup mints a pairing once SSH is the kind; none of it lands here.
  createSshPairing: vi.fn(() => new Promise(() => {})),
  getSshPairing: vi.fn(() => new Promise(() => {})),
  connectSshPairing: vi.fn(),
  probeSsh: vi.fn(),
  getSshKey: vi.fn(() => new Promise(() => {})),
}));

import { getComputerProviders, getWorkspaceQuota } from '../../utils/api';
import CreateComputerDialog from '../CreateComputerDialog';

const mockProviders = getComputerProviders as Mock;
const mockQuota = getWorkspaceQuota as Mock;

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
const SSH: ComputerProviderOption = { kind: 'ssh', is_default: false, allowed: true, tiers: [] };

const OPEN_QUOTA: WorkspaceQuota = {
  performance: { used: 0, limit: -1 },
  max: { used: 0, limit: -1 },
  xlarge: { used: 0, limit: -1 },
  always_on: null,
  computers: { used: 1, limit: 5 },
};

function open(onSubmit = vi.fn()) {
  renderWithProviders(<CreateComputerDialog onClose={() => {}} onSubmit={onSubmit} busy={false} error={null} />);
  return onSubmit;
}

function radio(name: string) {
  return screen.getByRole('radio', { name: new RegExp(`^${name}`) });
}

function tierValues() {
  const group = screen.getByRole('radiogroup', { name: 'Computer spec' });
  return Array.from(group.querySelectorAll('input[type="radio"]')).map((r) => r.getAttribute('value'));
}

beforeEach(() => {
  vi.clearAllMocks();
  mockProviders.mockResolvedValue({ providers: [DAYTONA, E2B] });
  mockQuota.mockResolvedValue(OPEN_QUOTA);
});

describe('CreateComputerDialog, sizes', () => {
  it("names each size and describes it with the provider's machine", async () => {
    open();
    await screen.findByRole('radiogroup', { name: 'Provider' });
    expect(tierValues()).toEqual(['standard', 'performance', 'max']);
    expect(radio('Small')).toHaveAccessibleDescription(/^1 vCPU\s*·\s*2 GB RAM\s*·\s*5 GB disk$/);
    expect(radio('Large')).toHaveAccessibleDescription(/2 vCPU\s*·\s*8 GB RAM\s*·\s*10 GB disk$/);
  });

  it('offers Extra Large on E2B alone, and falls back to Small on a switch to Daytona', async () => {
    const user = userEvent.setup();
    const onSubmit = open();
    await screen.findByRole('radiogroup', { name: 'Provider' });
    expect(screen.queryByRole('radio', { name: /^Extra Large/ })).not.toBeInTheDocument();

    await user.click(radio('E2B'));
    expect(tierValues()).toEqual(['standard', 'performance', 'max', 'xlarge']);
    expect(radio('Extra Large')).toHaveAccessibleDescription(/4 vCPU\s*·\s*8 GB RAM\s*·\s*20 GB disk$/);
    await user.click(radio('Extra Large'));
    expect(radio('Extra Large')).toBeChecked();

    await user.click(radio('Daytona'));
    expect(screen.queryByRole('radio', { name: /^Extra Large/ })).not.toBeInTheDocument();
    expect(radio('Small')).toBeChecked();
    await user.click(screen.getByRole('button', { name: 'Create computer' }));
    expect(onSubmit).toHaveBeenCalledWith({ resource_tier: 'standard', kind: 'daytona' });
  });

  it('shows the three base sizes by name alone when the server does not list them', async () => {
    mockProviders.mockResolvedValue({
      providers: [DAYTONA, E2B].map(({ tiers: _tiers, ...older }) => older),
    });
    const user = userEvent.setup();
    open();
    await screen.findByRole('radiogroup', { name: 'Provider' });
    await user.click(radio('E2B'));
    expect(tierValues()).toEqual(['standard', 'performance', 'max']);
    expect(radio('Medium')).not.toHaveAccessibleDescription(/vCPU/);
  });

  it('holds Extra Large when the plan lacks it, and says where to get it', async () => {
    mockQuota.mockResolvedValue({ ...OPEN_QUOTA, xlarge: { used: 0, limit: 0 } });
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('radio', { name: /^E2B/ }));

    await waitFor(() => expect(radio('Extra Large')).toBeDisabled());
    expect(radio('Extra Large')).toHaveAccessibleDescription(/^Not on your plan/);
    expect(screen.getByTestId('create-computer-plan-cta')).toHaveTextContent(
      'Need a bigger spec than your plan includes?',
    );
  });

  it('says nothing of a plan lock Daytona cannot run anyway', async () => {
    mockQuota.mockResolvedValue({ ...MAX_PLAN, xlarge: { used: 0, limit: 0 } });
    open();
    // The quota has landed once the slot line shows.
    await screen.findByTestId('tier-slot');
    expect(screen.queryByTestId('create-computer-plan-cta')).not.toBeInTheDocument();
  });
});

describe('CreateComputerDialog, the size rows', () => {
  it('sets each size on one row with its specs on one line', async () => {
    open();

    const group = await screen.findByRole('radiogroup', { name: 'Computer spec' });
    expect(group).not.toHaveClass('@sm:grid-cols-2', '@md:grid-cols-3');
    await waitFor(() =>
      expect(radio('Small')).toHaveAccessibleDescription(/^1 vCPU\s*·\s*2 GB RAM\s*·\s*5 GB disk$/),
    );
  });
});

describe('CreateComputerDialog, the providers', () => {
  it('puts three providers in one row, the hosted ones first, each named plainly', async () => {
    mockProviders.mockResolvedValue({ providers: [SSH, DAYTONA, E2B] });
    open();

    const group = await screen.findByRole('radiogroup', { name: 'Provider' });
    expect(group).toHaveClass('@md:grid-cols-3');
    expect(Array.from(group.querySelectorAll('input[type="radio"]')).map((r) => r.getAttribute('value'))).toEqual([
      'daytona',
      'e2b',
      'ssh',
    ]);
    expect(radio('Daytona')).toHaveAccessibleDescription('Container-based sandbox');
    expect(radio('E2B')).toHaveAccessibleDescription('MicroVM');
    expect(radio('Your machine \\(SSH\\)')).toHaveAccessibleDescription(/^Preview\s*Any Linux server or Mac$/);
  });

  it('keeps two providers side by side', async () => {
    open();

    const group = await screen.findByRole('radiogroup', { name: 'Provider' });
    expect(group).toHaveClass('@sm:grid-cols-2');
    expect(group).not.toHaveClass('@md:grid-cols-3');
  });
});

// A plan has one slot for a computer bigger than Small, open at every size up
// to its largest, and each size counts every computer held in it.
const MAX_PLAN: WorkspaceQuota = {
  performance: { used: 0, limit: 1 },
  max: { used: 0, limit: 1 },
  xlarge: { used: 0, limit: 1 },
  always_on: null,
  computers: { used: 1, limit: 2 },
};

describe('CreateComputerDialog, the larger slot', () => {
  it('opens every size up to the top of a Max plan, and names the one slot once', async () => {
    mockQuota.mockResolvedValue(MAX_PLAN);
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('radio', { name: /^E2B/ }));

    expect(await screen.findByTestId('tier-slot')).toHaveTextContent(
      'Your plan includes 1 computer larger than Small.',
    );
    for (const name of ['Medium', 'Large', 'Extra Large']) {
      expect(radio(name)).toBeEnabled();
      // The slot is said once, under the sizes, rather than on each.
      expect(radio(name)).toHaveAccessibleDescription(/^\d+ vCPU/);
    }
    expect(screen.queryByTestId('create-computer-plan-cta')).not.toBeInTheDocument();
  });

  it('reads none left at every larger size once one is held', async () => {
    const held = { used: 1, limit: 1 };
    mockQuota.mockResolvedValue({ ...MAX_PLAN, performance: held, max: held, xlarge: held });
    const user = userEvent.setup();
    open();
    await user.click(await screen.findByRole('radio', { name: /^E2B/ }));

    await waitFor(() => expect(radio('Medium')).toBeDisabled());
    for (const name of ['Medium', 'Large', 'Extra Large']) {
      expect(radio(name)).toBeDisabled();
      expect(radio(name)).toHaveAccessibleDescription(/^None left/);
    }
    expect(screen.getByTestId('tier-slot')).toHaveTextContent("Your plan's computer larger than Small is in use.");
    expect(radio('Small')).toBeChecked();
  });

  it('names the slot on Daytona too, which runs no Extra Large', async () => {
    mockQuota.mockResolvedValue(MAX_PLAN);
    open();

    expect(await screen.findByTestId('tier-slot')).toHaveTextContent(
      'Your plan includes 1 computer larger than Small.',
    );
    expect(tierValues()).toEqual(['standard', 'performance', 'max']);
  });

  it('has no slot line on a plan with no larger size, only the way to one', async () => {
    const none = { used: 0, limit: 0 };
    mockQuota.mockResolvedValue({ ...MAX_PLAN, performance: none, max: none, xlarge: none });
    open();

    await waitFor(() => expect(radio('Medium')).toBeDisabled());
    expect(radio('Medium')).toHaveAccessibleDescription(/^Not on your plan/);
    expect(screen.queryByTestId('tier-slot')).not.toBeInTheDocument();
    expect(screen.getByTestId('create-computer-plan-cta')).toBeInTheDocument();
  });

  it('says nothing of the slot, on the line or the sizes, on an unlimited plan', async () => {
    open();
    await waitFor(() => expect(mockQuota).toHaveBeenCalled());
    await act(() => new Promise((resolve) => setTimeout(resolve, 0)));

    expect(screen.queryByTestId('tier-slot')).not.toBeInTheDocument();
    expect(radio('Medium')).toHaveAccessibleDescription(/^2 vCPU/);
  });
});

describe('CreateComputerDialog, a size on another provider', () => {
  it('names Extra Large on E2B to a Max plan on Daytona, not an upgrade', async () => {
    mockQuota.mockResolvedValue(MAX_PLAN);
    const user = userEvent.setup();
    const onSubmit = open();

    const hint = await screen.findByTestId('create-computer-tier-elsewhere');
    expect(hint).toHaveTextContent('Extra Large is available on E2B.');
    expect(radio('Large')).toBeEnabled();
    expect(screen.queryByTestId('create-computer-plan-cta')).not.toBeInTheDocument();

    await user.click(screen.getByRole('button', { name: 'Switch to E2B' }));
    expect(radio('E2B')).toBeChecked();
    expect(radio('Extra Large')).toBeChecked();
    expect(screen.queryByTestId('create-computer-tier-elsewhere')).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Create computer' }));
    expect(onSubmit).toHaveBeenCalledWith({ resource_tier: 'xlarge', kind: 'e2b' });
  });

  it('keeps the upgrade line when no other provider runs a bigger size the plan allows', async () => {
    // Plus: Medium is its largest.
    mockQuota.mockResolvedValue({ ...MAX_PLAN, max: { used: 0, limit: 0 }, xlarge: { used: 0, limit: 0 } });
    open();

    await waitFor(() => expect(radio('Large')).toBeDisabled());
    expect(screen.getByTestId('create-computer-plan-cta')).toHaveTextContent(
      'Need a bigger spec than your plan includes?',
    );
    expect(screen.queryByTestId('create-computer-tier-elsewhere')).not.toBeInTheDocument();
  });
});

describe('CreateComputerDialog, at the computer cap', () => {
  const AT_CAP: WorkspaceQuota = { ...OPEN_QUOTA, computers: { used: 1, limit: 1 } };

  it('locks every provider the cap counts and leaves the user\'s own machine', async () => {
    mockProviders.mockResolvedValue({ providers: [DAYTONA, E2B, SSH] });
    mockQuota.mockResolvedValue(AT_CAP);
    open();

    await waitFor(() => expect(radio('Daytona')).toBeDisabled());
    expect(radio('Daytona')).toHaveAccessibleDescription(/^Limit reached/);
    expect(radio('E2B')).toBeDisabled();
    expect(radio('Your machine \\(SSH\\)')).toBeChecked();
    expect(screen.queryByRole('radiogroup', { name: 'Computer spec' })).not.toBeInTheDocument();
    expect(screen.getByTestId('create-computer-plan-cta')).toHaveTextContent(
      'Need more computers than your plan includes?',
    );
  });

  it('names a plan lock over the cap', async () => {
    mockProviders.mockResolvedValue({ providers: [DAYTONA, { ...E2B, allowed: false }, SSH] });
    mockQuota.mockResolvedValue(AT_CAP);
    open();

    await waitFor(() => expect(radio('Daytona')).toBeDisabled());
    expect(radio('E2B')).toHaveAccessibleDescription(/^Not on your plan/);
  });

  it('fails open when the count is unknown', async () => {
    mockProviders.mockResolvedValue({ providers: [DAYTONA, E2B, SSH] });
    mockQuota.mockResolvedValue({ ...MAX_PLAN, computers: null });
    open();

    await screen.findByTestId('tier-slot');
    expect(radio('Daytona')).toBeEnabled();
    expect(radio('Daytona')).toBeChecked();
    expect(screen.getByRole('button', { name: 'Create computer' })).toBeEnabled();
  });
});
