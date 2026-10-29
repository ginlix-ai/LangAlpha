/**
 * What the gallery says about the machines under its workspaces, and how it
 * narrows, sorts and greets.
 *
 * The rows are the live wt3 ones: two workspaces on computer 5319ad0e, each
 * with its own `dir_name`. A card's state has to come from the computer,
 * because that is what starts and stops.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { relativeTime } from '@/lib/format';
import { queryKeys } from '@/lib/queryKeys';
import { renderWithProviders } from '@/test/utils';
import type { Computer, Workspace } from '@/types/api';

const COMPUTER_ID = '5319ad0e-7835-4ca6-9541-b7c2961a7bf6';
const MAC_ID = 'mac-0001';

const HOME: Workspace = {
  workspace_id: 'home-0001',
  name: 'Home',
  status: 'running',
  is_home: true,
  computer_id: COMPUTER_ID,
  updated_at: '2026-09-16T02:23:43.210835Z',
};

const WORKSPACES: Workspace[] = [
  {
    workspace_id: 'aecc944a-61b4-4e68-8a1d-28e1b1823001',
    name: 'Live Alpha',
    status: 'running',
    is_home: false,
    computer_id: COMPUTER_ID,
    dir_name: 'live-alpha-16ad',
    resource_tier: 'standard',
    updated_at: '2026-09-16T02:23:43.210835Z',
  },
  {
    workspace_id: '01dae5b9-edb4-4afb-966e-920039c6d0ac',
    name: 'Live Beta',
    status: 'running',
    is_home: false,
    computer_id: COMPUTER_ID,
    dir_name: 'live-beta-1d6d',
    resource_tier: 'standard',
    updated_at: '2026-09-16T02:23:43.210835Z',
  },
];

const listed = (workspaces: Workspace[]) => ({
  data: { workspaces, total: workspaces.length },
  isLoading: false,
  error: null,
});

vi.mock('@/hooks/useAllWorkspaces', () => ({
  useAllWorkspaces: vi.fn(),
  allWorkspacesKey: (sortBy: string, includeHome: boolean) => ['workspaces', 'list', { view: 'all', sortBy, includeHome }],
}));

vi.mock('../../utils/api', () => ({
  createWorkspace: vi.fn(),
  getHomeWorkspace: vi.fn(async () => null),
  reorderWorkspaces: vi.fn(),
  renameWorkspace: vi.fn(),
  getComputers: vi.fn(),
  getComputerProviders: vi.fn(async () => ({ providers: [] })),
  streamComputerEvents: vi.fn(async () => {}),
  streamWorkspaceEvents: vi.fn(async () => {}),
  createComputer: vi.fn(),
  startComputer: vi.fn(),
  stopComputer: vi.fn(),
  getWorkspaceThreads: vi.fn(async () => ({ threads: [], total: 0 })),
  getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
  formatApiErrorDetail: (err: unknown) => (err as { message?: string })?.message ?? 'Request failed',
  uploadWorkspaceFile: vi.fn(),
}));

vi.mock('../workspaceActions', () => ({
  WorkspaceMenuItems: () => null,
  useWorkspaceActions: () => ({
    openDuplicate: vi.fn(),
    openMove: vi.fn(),
    openDelete: vi.fn(),
    dialogs: null,
  }),
}));

// The desktop shell is the only one that can say which row is this Mac.
vi.mock('../../hooks/useRemoteControl', () => ({
  useThisMac: () => ({
    status: null,
    of: (c: Pick<Computer, 'ssh'>) => (c.ssh?.fingerprint === 'SHA256:thismac' ? { paused: false } : null),
  }),
}));

vi.mock('../../hooks/workspaceRowActions', () => ({ pinWorkspaceRow: vi.fn() }));
vi.mock('../../hooks/useNavigationData', () => ({
  isEffectivelyPinned: (ws: { is_home?: boolean; is_pinned?: boolean }) => !!ws.is_pinned || !!ws.is_home,
}));
vi.mock('../../hooks/utils/chatSessionRestore', () => ({ clearChatSession: vi.fn() }));
vi.mock('../../hooks/computerPanelStore', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../hooks/computerPanelStore')>()),
  openComputersPanel: vi.fn(),
}));

import { getComputerProviders, getComputers } from '../../utils/api';
import { openComputersPanel } from '../../hooks/computerPanelStore';
import { useAllWorkspaces } from '@/hooks/useAllWorkspaces';
import WorkspaceGallery from '../WorkspaceGallery';

const mockGetComputers = getComputers as Mock;
const mockUseAllWorkspaces = useAllWorkspaces as Mock;

const PRIMARY: Computer = {
  computer_id: COMPUTER_ID,
  user_id: 'wp17-live-user',
  kind: 'daytona',
  name: 'Alpha Research',
  status: 'running',
  resource_tier: 'standard',
  is_always_on: false,
  is_primary: true,
  root_dir: '/home/workspace',
  ssh: null,
};

const MAC: Computer = {
  ...PRIMARY,
  computer_id: MAC_ID,
  kind: 'ssh',
  name: "Alan's MacBook Pro",
  is_primary: false,
  ssh: {
    host: 'alans-mbp',
    port: 22,
    username: 'alan',
    fingerprint: 'SHA256:thismac',
    tunnel: true,
    tunnel_connected: true,
    helper: true,
  },
};

function computers(...list: Computer[]) {
  return { computers: list, total: list.length };
}

function computerList(status: string) {
  return computers({ ...PRIMARY, status });
}

const cards = () => screen.findAllByTestId('workspace-card');
const chip = () => screen.findByTestId('computer-chip');

const LOW_DISK = {
  used_bytes: 9_100_000_000,
  total_bytes: 10_000_000_000,
  free_bytes: 900_000_000,
  measured_at: '2026-09-16T02:23:43.210835Z',
  level: 'warning',
} as const;

/** Two machines, so each card says which it is on: Alpha on PRIMARY, Beta on the Mac. */
/** Live Alpha on the primary, Live Beta on the Mac. */
function onTwoMachines(primary: Partial<Computer> = {}, mac: Partial<Computer> = {}) {
  mockGetComputers.mockResolvedValue(computers({ ...PRIMARY, ...primary }, { ...MAC, ...mac }));
  mockUseAllWorkspaces.mockImplementation(() => listed([HOME, WORKSPACES[0], { ...WORKSPACES[1], computer_id: MAC_ID }]));
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  mockGetComputers.mockResolvedValue(computerList('running'));
  mockUseAllWorkspaces.mockImplementation(() => listed([HOME, ...WORKSPACES]));
});

describe('WorkspaceGallery, machine line', () => {
  it('reads every workspace in one list, Home included', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await cards();
    expect(mockUseAllWorkspaces).toHaveBeenCalledWith(expect.objectContaining({ includeHome: true, sortBy: 'activity' }));
  });

  it('draws Home as the Chief of Staff card above the cards, never as one of them', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [first] = await cards();
    expect(await cards()).toHaveLength(2);
    const chief = screen.getByTestId('chief-of-staff');
    expect(within(screen.getByTestId('workspace-grid')).getByTestId('chief-of-staff')).toBe(chief);
    expect(chief.compareDocumentPosition(first) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    // The workspaces under it are a section of their own, counted.
    expect(screen.getByText('Workspaces · 2')).toBeInTheDocument();
  });

  it('draws no machine line on a card when every workspace is on the one computer', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await chip();
    for (const card of await cards()) {
      expect(within(card).queryByText('Alpha Research')).toBeNull();
      expect(within(card).queryByTestId('computer-badge')).toBeNull();
    }
  });

  it('names the one computer once, in the toolbar, with its badge', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const named = await chip();
    expect(within(screen.getByTestId('gallery-toolbar')).getByTestId('computer-chip')).toBe(named);
    expect(named).toHaveTextContent('Alpha Research');
    expect(within(named).getByTestId('computer-badge')).toHaveTextContent('Daytona·Small');
    // Running is its dot alone; the glyph still names it for a screen reader.
    expect(await within(named).findByText('Running')).toHaveClass('sr-only');
    expect(within(named).queryByTestId('machine-state')).toBeNull();
  });

  it('opens a menu from the chip that names the machine, manages it and adds another', async () => {
    const user = userEvent.setup();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const named = await chip();
    // Still named for the machine it opens onto.
    expect(named).toHaveAccessibleName(/Alpha Research/);
    await user.click(named);
    expect(openComputersPanel).not.toHaveBeenCalled();

    const menu = screen.getByRole('menu');
    // The machine is shown, not offered: nothing to pick with one.
    const machine = within(menu).getByTestId('computer-menu-machine');
    expect(machine).toHaveTextContent('Alpha Research');
    expect(within(machine).getByTestId('computer-badge')).toHaveTextContent('Daytona·Small');
    expect(within(menu).queryAllByRole('menuitemradio')).toHaveLength(0);
    expect(within(menu).getAllByRole('menuitem').map((i) => i.textContent)).toEqual(['Manage computer…', 'New computer…']);

    await user.click(within(menu).getByRole('menuitem', { name: 'Manage computer…' }));
    expect(openComputersPanel).toHaveBeenCalledWith({ view: 'list', computerId: COMPUTER_ID });

    await user.click(named);
    await user.click(screen.getByRole('menuitem', { name: 'New computer…' }));
    expect(openComputersPanel).toHaveBeenLastCalledWith({ view: 'create' });
  });

  it('spells out a stopped computer on the chip, reading the state from the computer, not the row', async () => {
    // The rows still say 'running'; the machine has since been stopped.
    mockGetComputers.mockResolvedValue(computerList('stopped'));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await within(await chip()).findByTestId('machine-state')).toHaveTextContent('Stopped');
    expect(screen.queryByText('Running')).not.toBeInTheDocument();
  });

  it('shows a filling disk on the chip and in the banner, never on a card', async () => {
    mockGetComputers.mockResolvedValue(computers({ ...PRIMARY, disk: LOW_DISK }));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await within(await chip()).findByText('Disk 91%')).toHaveAttribute('title', 'Alpha Research is running low on disk space');
    expect(screen.getByTestId('disk-warning')).toBeInTheDocument();
    for (const card of await cards()) expect(within(card).queryByText(/Disk/)).toBeNull();
  });

  it('offers New computer in the chip\'s place when there is no computer', async () => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue(computers());
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const add = await screen.findByTestId('new-computer');
    expect(within(screen.getByTestId('gallery-toolbar')).getByRole('button', { name: 'New computer' })).toBe(add);
    expect(screen.queryByTestId('computer-chip')).not.toBeInTheDocument();
    expect(screen.queryByTestId('computer-filter')).not.toBeInTheDocument();

    await user.click(add);
    expect(openComputersPanel).toHaveBeenCalledWith({ view: 'create' });
  });

  it('keeps the computer reachable before the first workspace, with nothing to search or sort', async () => {
    const user = userEvent.setup();
    mockUseAllWorkspaces.mockImplementation(() => listed([HOME]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const named = await chip();
    expect(screen.queryByTestId('gallery-toolbar')).not.toBeInTheDocument();
    expect(screen.queryByRole('searchbox')).not.toBeInTheDocument();
    expect(screen.queryByTestId('gallery-sort')).not.toBeInTheDocument();

    await user.click(named);
    await user.click(screen.getByRole('menuitem', { name: 'New computer…' }));
    expect(openComputersPanel).toHaveBeenCalledWith({ view: 'create' });
  });

  it('leaves the computer slot empty while the list is still being read', async () => {
    mockGetComputers.mockImplementation(() => new Promise(() => {}));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await cards();
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(screen.getByTestId('gallery-toolbar')).toBeInTheDocument();
    expect(screen.queryByTestId('new-computer')).not.toBeInTheDocument();
    expect(screen.queryByTestId('computer-chip')).not.toBeInTheDocument();
    expect(screen.queryByTestId('computer-filter')).not.toBeInTheDocument();
  });

  it('leaves out the badge words a computer\'s own name already says', async () => {
    mockGetComputers.mockResolvedValue(computers({ ...PRIMARY, name: 'Daytona Small' }));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const named = await chip();
    expect(named).toHaveTextContent('Daytona Small');
    expect(within(named).queryByTestId('computer-badge')).toBeNull();
  });

  it('leaves the line off every card when a second computer holds none of them', async () => {
    // The live case: a second machine with no workspaces on it.
    mockGetComputers.mockResolvedValue(computers(PRIMARY, MAC));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    // The toolbar still offers both, as a filter.
    expect(await screen.findByTestId('computer-filter')).toBeInTheDocument();
    for (const card of await cards()) {
      expect(within(card).queryByText('Alpha Research')).toBeNull();
      expect(within(card).queryByTestId('computer-badge')).toBeNull();
    }
    expect(screen.queryByTestId('computer-chip')).not.toBeInTheDocument();
  });

  it('counts the workspaces\' machines, not Home\'s, toward the line', async () => {
    mockGetComputers.mockResolvedValue(computers(PRIMARY, MAC));
    mockUseAllWorkspaces.mockImplementation(() => listed([{ ...HOME, computer_id: MAC_ID }, ...WORKSPACES]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await screen.findByTestId('computer-filter');
    for (const card of await cards()) expect(within(card).queryByTestId('computer-badge')).toBeNull();
  });

  it('labels only the cards off the primary computer', async () => {
    onTwoMachines();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [alpha, beta] = await cards();
    expect(await within(beta).findByText("Alan's MacBook Pro")).toBeInTheDocument();
    // The primary is where a workspace goes unless told otherwise: no line.
    expect(within(alpha).queryByText('Alpha Research')).toBeNull();
    expect(within(alpha).queryByTestId('computer-badge')).toBeNull();
    expect(screen.queryByTestId('computer-chip')).not.toBeInTheDocument();
  });

  it('names the machine and its size as plain words after the name', async () => {
    // The Mac primary, so the Daytona card is the one off it.
    onTwoMachines({ is_primary: false }, { is_primary: true });
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [alpha, beta] = await cards();
    // The name, not the folder the title already spells.
    expect(await within(alpha).findByText('Alpha Research')).toBeInTheDocument();
    expect(within(alpha).queryByText('live-alpha-16ad')).toBeNull();
    const badge = within(alpha).getByTestId('computer-badge');
    expect(badge).toHaveTextContent('·Daytona·Small');
    expect(badge).not.toHaveClass('rounded-full');
    expect(within(beta).queryByText("Alan's MacBook Pro")).toBeNull();
  });

  it('breaks a tie for the usual machine by id, so the order of the list never moves it', async () => {
    const box = { ...MAC, computer_id: 'box-0001', kind: 'e2b', name: 'Backtests', ssh: null };
    mockGetComputers.mockResolvedValue(computers(PRIMARY, MAC, box));
    const onMac = { ...WORKSPACES[0], computer_id: MAC_ID };
    const onBox = { ...WORKSPACES[1], computer_id: 'box-0001' };
    for (const order of [[onMac, onBox], [onBox, onMac]]) {
      mockUseAllWorkspaces.mockImplementation(() => listed([HOME, ...order]));
      const { unmount } = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
      await cards();
      // box-0001 sorts before mac-0001, so the Box is usual and the Mac's card says so.
      expect(await screen.findByText("Alan's MacBook Pro")).toBeInTheDocument();
      expect(screen.queryByText('Backtests')).not.toBeInTheDocument();
      unmount();
    }
  });

  it('takes the machine holding the most as usual when the primary holds none', async () => {
    mockGetComputers.mockResolvedValue(computers(PRIMARY, MAC, { ...MAC, computer_id: 'box-0001', kind: 'e2b', name: 'Backtests', ssh: null }));
    mockUseAllWorkspaces.mockImplementation(() => listed([
      HOME,
      { ...WORKSPACES[0], computer_id: MAC_ID },
      { ...WORKSPACES[1], computer_id: MAC_ID },
      { ...WORKSPACES[1], workspace_id: 'gamma', name: 'Live Gamma', computer_id: 'box-0001' },
    ]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [alpha, beta, gamma] = await cards();
    expect(await within(gamma).findByText('Backtests')).toBeInTheDocument();
    for (const card of [alpha, beta]) expect(within(card).queryByTestId('computer-badge')).toBeNull();
  });

  it('leaves the line off the cards a filter narrows to one machine, and keeps it through a search', async () => {
    const user = userEvent.setup();
    onTwoMachines();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [, beta] = await cards();
    expect(await within(beta).findByText("Alan's MacBook Pro")).toBeInTheDocument();

    // Every card listed sits on the one machine the filter names.
    await user.click(screen.getByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitemradio', { name: /MacBook/ }));
    const filtered = await cards();
    expect(filtered).toHaveLength(1);
    expect(within(filtered[0]).queryByText("Alan's MacBook Pro")).toBeNull();

    // A search narrows the cards, not what counts as the usual machine.
    await user.click(screen.getByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitemradio', { name: 'All computers' }));
    await user.type(screen.getByRole('searchbox'), 'Beta');
    const searched = await cards();
    expect(searched).toHaveLength(1);
    expect(within(searched[0]).getByText("Alan's MacBook Pro")).toBeInTheDocument();
  });

  it('calls the desktop app\'s own Mac "This Mac"', async () => {
    onTwoMachines();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [, beta] = await cards();
    expect(await within(beta).findByTestId('computer-badge')).toHaveTextContent(/^·This Mac$/);
  });

  it('gives a running machine its dot and no word to read past', async () => {
    onTwoMachines();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    const [, beta] = await cards();
    expect(await within(beta).findByText('Running')).toHaveClass('sr-only');
    expect(within(beta).queryByTestId('machine-state')).toBeNull();
  });

  it('spells out a stopped machine on its cards', async () => {
    onTwoMachines({}, { status: 'stopped' });
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [alpha, beta] = await cards();
    expect(await within(beta).findByTestId('machine-state')).toHaveTextContent('Stopped');
    expect(within(alpha).queryByTestId('machine-state')).toBeNull();
  });

  it('keeps the disk off the cards with two computers, and says it in the filter menu', async () => {
    const user = userEvent.setup();
    onTwoMachines({}, { disk: LOW_DISK });
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [, beta] = await cards();
    await within(beta).findByText("Alan's MacBook Pro");
    expect(within(beta).queryByText(/Disk/)).toBeNull();
    expect(screen.getByTestId('disk-warning')).toBeInTheDocument();

    await user.click(screen.getByTestId('computer-filter'));
    const [, alphaRow, macRow] = screen.getAllByRole('menuitemradio');
    expect(within(macRow).getByText('Disk 91%')).toBeInTheDocument();
    expect(within(alphaRow).queryByText(/Disk/)).toBeNull();
  });

  it('leaves out a time it cannot read instead of failing the card', async () => {
    mockUseAllWorkspaces.mockImplementation(() => listed([{ ...WORKSPACES[0], updated_at: 'not a date' }, WORKSPACES[1]]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const [card] = await cards();
    expect(within(card).getByText('Live Alpha')).toBeInTheDocument();
    expect(within(card).queryByText(/ago|now|yesterday/)).toBeNull();
  });

  it('dates a card by last use, not by the last edit, in every order', async () => {
    // A rename, pin or reorder bumps `updated_at`; the Recent order sorts on use.
    const user = userEvent.setup();
    const edited = new Date().toISOString();
    const used = new Date(Date.now() - 3 * 86_400_000).toISOString();
    mockUseAllWorkspaces.mockImplementation(() => listed([
      { ...HOME, updated_at: edited },
      { ...WORKSPACES[0], updated_at: edited, last_activity_at: used },
      // Never used: the edit is all there is to go on.
      { ...WORKSPACES[1], updated_at: used, last_activity_at: null },
    ]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const usedText = relativeTime(used, 'en-US', Date.now());
    const editedText = relativeTime(edited, 'en-US', Date.now());
    const expectTimes = async () => {
      const [alpha, beta] = await cards();
      expect(within(alpha).getByText(usedText)).toBeInTheDocument();
      expect(within(alpha).queryByText(editedText)).toBeNull();
      expect(within(beta).getByText(usedText)).toBeInTheDocument();
    };
    await expectTimes();

    for (const order of ['Name', 'Custom']) {
      await user.click(screen.getByTestId('gallery-sort'));
      await user.click(screen.getByRole('menuitemradio', { name: order }));
      await expectTimes();
    }
  });

  it('hands the banner to the next full machine when one is dismissed', async () => {
    // Two machines equally low: dismissing the one shown must not silence the other.
    const disk = {
      used_bytes: 9_000_000_000,
      total_bytes: 10_000_000_000,
      free_bytes: 1_000_000_000,
      measured_at: '2026-09-16T02:23:43.210835Z',
      level: 'warning',
    };
    const [machine] = computerList('running').computers;
    mockGetComputers.mockResolvedValue({
      computers: [
        { ...machine, computer_id: 'disk-a', name: 'Disk A', disk },
        { ...machine, computer_id: 'disk-b', name: 'Disk B', is_primary: false, disk },
      ],
      total: 2,
    });
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await screen.findByText('Disk A is running low on disk space')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));

    expect(await screen.findByText('Disk B is running low on disk space')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }));

    expect(screen.queryByTestId('disk-warning')).not.toBeInTheDocument();
  });

  it.each([
    ['daytona', false],
    ['e2b', true],
  ])('offers a bigger size to a full Large machine on %s only where one runs', async (kind, offered) => {
    const disk: Record<string, number> = { standard: 5, performance: 10, max: 10, xlarge: 20 };
    const spec = (tier: string) => ({ tier, cpu: 2, memory_gib: 8, disk_gib: disk[tier] });
    (getComputerProviders as Mock).mockResolvedValue({
      providers: [
        { kind: 'daytona', is_default: true, allowed: null, tiers: ['standard', 'performance', 'max'].map(spec) },
        { kind: 'e2b', is_default: false, allowed: true, tiers: ['standard', 'performance', 'max', 'xlarge'].map(spec) },
      ],
    });
    const [machine] = computerList('running').computers;
    mockGetComputers.mockResolvedValue({
      computers: [{
        ...machine,
        kind,
        resource_tier: 'max',
        disk: {
          used_bytes: 9_000_000_000,
          total_bytes: 10_000_000_000,
          free_bytes: 1_000_000_000,
          measured_at: '2026-09-16T02:23:43.210835Z',
          level: 'warning',
        },
      }],
      total: 1,
    });
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    const warning = await screen.findByTestId('disk-warning');
    await waitFor(() => expect(getComputerProviders).toHaveBeenCalled());
    if (offered) expect(await within(warning).findByRole('button', { name: 'Change spec' })).toBeInTheDocument();
    else {
      // Let the list land before reading its absence.
      await act(async () => {});
      expect(within(warning).queryByRole('button', { name: 'Change spec' })).not.toBeInTheDocument();
    }
  });

  it('no longer shows the Always-on pill a legacy machine still carries', async () => {
    mockGetComputers.mockResolvedValue(computers({ ...PRIMARY, is_always_on: true }));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await within(await chip()).findByTestId('computer-badge');
    expect(screen.queryByText('Always-on')).not.toBeInTheDocument();
  });

  it('leaves the computers to the toolbar, with no Computers button in the header', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await chip();
    expect(screen.queryByRole('button', { name: /^computers$/i })).not.toBeInTheDocument();
    expect(screen.getAllByRole('button', { name: 'New workspace' }).length).toBeGreaterThan(0);
  });
});

describe('WorkspaceGallery, toolbar', () => {
  it('has no computer filter with one computer', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await cards();
    await waitFor(() => expect(mockGetComputers).toHaveBeenCalled());
    expect(screen.getByTestId('gallery-toolbar')).toBeInTheDocument();
    expect(screen.queryByTestId('computer-filter')).not.toBeInTheDocument();
  });

  it('narrows the grid to one machine once there are two to choose from', async () => {
    const user = userEvent.setup();
    mockGetComputers.mockResolvedValue(computers(PRIMARY, MAC));
    mockUseAllWorkspaces.mockImplementation(() => listed([HOME, WORKSPACES[0], { ...WORKSPACES[1], computer_id: MAC_ID }]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await cards()).toHaveLength(2);
    const filter = await screen.findByTestId('computer-filter');
    expect(filter).toHaveTextContent('All computers');
    // The label names the control, and the value still reads out with it.
    expect(filter).toHaveAccessibleName('Filter by computer: All computers');

    await user.click(filter);
    const options = screen.getAllByRole('menuitemradio');
    expect(options.map((o) => o.textContent)).toEqual([
      'All computers',
      expect.stringContaining('Alpha Research'),
      expect.stringContaining("Alan's MacBook Pro"),
    ]);
    // Each machine is offered with its badge.
    expect(within(options[2]).getByTestId('computer-badge')).toHaveTextContent('This Mac');

    await user.click(options[2]);
    const narrowed = await cards();
    expect(narrowed).toHaveLength(1);
    expect(within(narrowed[0]).getByText('Live Beta')).toBeInTheDocument();
    expect(screen.getByTestId('computer-filter')).toHaveTextContent("Alan's MacBook Pro");
    expect(screen.getByTestId('computer-filter')).toHaveAccessibleName("Filter by computer: Alan's MacBook Pro");
  });

  it('manages the machine the grid is narrowed to, or all of them, and adds one, from the filter menu', async () => {
    const user = userEvent.setup();
    onTwoMachines();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    await user.click(await screen.findByTestId('computer-filter'));
    // The filter rows stay as they were; the actions follow them, outside the choice.
    expect(screen.getAllByRole('menuitemradio')).toHaveLength(3);
    expect(screen.getAllByRole('menuitem').map((i) => i.textContent)).toEqual(['Manage computers…', 'New computer…']);
    expect(within(screen.getByRole('group')).queryAllByRole('menuitem')).toHaveLength(0);

    await user.click(screen.getByRole('menuitem', { name: 'Manage computers…' }));
    expect(openComputersPanel).toHaveBeenLastCalledWith({ view: 'list', computerId: null });

    await user.click(screen.getByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitemradio', { name: /MacBook/ }));
    await user.click(screen.getByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitem', { name: 'Manage computers…' }));
    expect(openComputersPanel).toHaveBeenLastCalledWith({ view: 'list', computerId: MAC_ID });

    await user.click(screen.getByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitem', { name: 'New computer…' }));
    expect(openComputersPanel).toHaveBeenLastCalledWith({ view: 'create' });
  });

  describe('a filter on a machine that leaves the list', () => {
    const OTHER: Computer = { ...PRIMARY, computer_id: 'other-0001', name: 'Beta Box', is_primary: false };
    const setComputers = (client: ReturnType<typeof renderWithProviders>['queryClient'], list: Computer[]) =>
      act(() => { client.setQueryData(queryKeys.computers.lists(), computers(...list)); });

    beforeEach(() => {
      mockGetComputers.mockResolvedValue(computers(PRIMARY, MAC, OTHER));
      mockUseAllWorkspaces.mockImplementation(() => listed([
        HOME,
        WORKSPACES[0],
        { ...WORKSPACES[1], computer_id: MAC_ID },
        { ...WORKSPACES[1], workspace_id: 'ws-other', name: 'Live Gamma', computer_id: OTHER.computer_id },
      ]));
    });

    async function filterToMac() {
      const user = userEvent.setup();
      await user.click(await screen.findByTestId('computer-filter'));
      await user.click(screen.getByRole('menuitemradio', { name: /Alan's MacBook Pro/ }));
      expect(await cards()).toHaveLength(1);
    }

    it.each([
      ['is deleted', [PRIMARY, OTHER]],
      ['leaves only one machine', [PRIMARY]],
    ])('drops it when the machine %s, so it does not come back with it', async (_, after) => {
      const { queryClient } = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
      await filterToMac();

      setComputers(queryClient, after);
      await waitFor(() => expect(screen.getAllByTestId('workspace-card')).toHaveLength(3));

      setComputers(queryClient, [PRIMARY, MAC, OTHER]);
      expect(await screen.findByTestId('computer-filter')).toHaveTextContent('All computers');
      expect(screen.getAllByTestId('workspace-card')).toHaveLength(3);
    });

    it('keeps it through a list that comes back empty for a moment', async () => {
      const { queryClient } = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
      await filterToMac();

      setComputers(queryClient, []);
      await waitFor(() => expect(screen.getAllByTestId('workspace-card')).toHaveLength(3));

      setComputers(queryClient, [PRIMARY, MAC, OTHER]);
      await waitFor(() => expect(screen.getAllByTestId('workspace-card')).toHaveLength(1));
      expect(within(screen.getByTestId('workspace-card')).getByText('Live Beta')).toBeInTheDocument();
      expect(screen.getByTestId('computer-filter')).toHaveTextContent("Alan's MacBook Pro");
    });
  });

  it('asks the server for the order picked from the sort menu', async () => {
    const user = userEvent.setup();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();

    const sort = screen.getByTestId('gallery-sort');
    expect(sort).toHaveTextContent('Recent');
    await user.click(sort);
    const options = screen.getAllByRole('menuitemradio');
    expect(options.map((o) => o.getAttribute('aria-label'))).toEqual(['Recent', 'Name', 'Custom']);
    // Custom is the order cards are dragged into, and says so under its name.
    expect(options[2]).toHaveAccessibleName('Custom');
    expect(options[2]).toHaveAccessibleDescription('Drag cards to arrange');

    await user.click(screen.getByRole('menuitemradio', { name: 'Custom' }));
    expect(mockUseAllWorkspaces).toHaveBeenLastCalledWith(expect.objectContaining({ sortBy: 'custom' }));
    expect(screen.getByTestId('gallery-sort')).toHaveTextContent('Custom');
  });

  it('opens on the order picked last time', async () => {
    const user = userEvent.setup();
    const first = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();
    await user.click(screen.getByTestId('gallery-sort'));
    await user.click(screen.getByRole('menuitemradio', { name: 'Name' }));
    first.unmount();

    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();
    expect(screen.getByTestId('gallery-sort')).toHaveTextContent('Name');
    expect(mockUseAllWorkspaces).toHaveBeenLastCalledWith(expect.objectContaining({ sortBy: 'name' }));
  });

  it.each([
    ['nothing is stored', null],
    ['the stored order is not one it knows', 'oldest'],
  ])('opens on Recent when %s', async (_, stored) => {
    if (stored) localStorage.setItem('workspaceGallery.sort', stored);
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();
    expect(screen.getByTestId('gallery-sort')).toHaveTextContent('Recent');
    expect(mockUseAllWorkspaces).toHaveBeenLastCalledWith(expect.objectContaining({ sortBy: 'activity' }));
  });

  it('says so when a search matches nothing', async () => {
    const user = userEvent.setup();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();

    await user.type(screen.getByRole('searchbox'), 'zzz');
    expect(screen.getByText('No workspaces match “zzz”.')).toBeInTheDocument();
    expect(screen.queryAllByTestId('workspace-card')).toHaveLength(0);

    await user.click(screen.getByRole('button', { name: 'Clear search' }));
    expect(screen.getByRole('searchbox')).toHaveValue('');
    expect(await cards()).toHaveLength(2);
  });

  it('stands its frame up while the list is read, and the cards land in the placeholders without an entrance', async () => {
    mockUseAllWorkspaces.mockImplementation(() => ({ data: undefined, isLoading: true, error: null }));
    const { rerender } = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    expect(screen.getAllByRole('heading', { name: 'Workspaces' }).length).toBeGreaterThan(0);
    expect(screen.getByRole('searchbox')).toBeInTheDocument();
    expect(screen.getByTestId('chief-of-staff')).toBeInTheDocument();
    expect(screen.getAllByTestId('workspace-card-skeleton').length).toBeGreaterThan(0);
    expect(screen.queryByTestId('workspace-card')).not.toBeInTheDocument();

    mockUseAllWorkspaces.mockImplementation(() => listed([HOME, ...WORKSPACES]));
    rerender(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    for (const card of await cards()) expect(card).not.toHaveClass('enter-fade-up');
    expect(screen.getByTestId('chief-of-staff')).not.toHaveClass('enter-fade-up');
  });

  it('has no reorder button and no separate reorder mode', async () => {
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();

    expect(screen.queryByRole('button', { name: 'Reorder' })).not.toBeInTheDocument();
    expect(screen.queryByText('Drag to reorder')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Done' })).not.toBeInTheDocument();
  });
});

describe('WorkspaceGallery, the Chief of Staff card', () => {
  it('steps aside while a search runs, and comes back when it clears', async () => {
    const user = userEvent.setup();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();

    await user.type(screen.getByRole('searchbox'), 'alpha');
    expect(screen.queryByTestId('chief-of-staff')).not.toBeInTheDocument();
    expect(screen.queryByText(/^Workspaces · /)).not.toBeInTheDocument();
    await user.clear(screen.getByRole('searchbox'));
    expect(screen.getByTestId('chief-of-staff')).toBeInTheDocument();
  });

  it('comes back when the last workspace goes under a search, with no search box left to clear', async () => {
    const user = userEvent.setup();
    mockUseAllWorkspaces.mockImplementation(() => listed([HOME, WORKSPACES[0]]));
    const { rerender } = renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();

    await user.type(screen.getByRole('searchbox'), 'alpha');
    expect(screen.queryByTestId('chief-of-staff')).not.toBeInTheDocument();

    // Deleted from its card's menu: the list comes back with Home alone.
    mockUseAllWorkspaces.mockImplementation(() => listed([HOME]));
    rerender(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    expect(await screen.findByText('No workspaces yet')).toBeInTheDocument();
    expect(screen.getByTestId('chief-of-staff')).toBeInTheDocument();
  });

  it('keeps its place under a computer filter, which narrows only the workspaces', async () => {
    const user = userEvent.setup();
    onTwoMachines();
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);
    await cards();

    await user.click(await screen.findByTestId('computer-filter'));
    await user.click(screen.getByRole('menuitemradio', { name: /Alpha Research/ }));
    expect(await cards()).toHaveLength(1);
    expect(screen.getByTestId('chief-of-staff')).toBeInTheDocument();
    expect(screen.getByText('Workspaces · 1')).toBeInTheDocument();
  });
});

describe('WorkspaceGallery, a failed read', () => {
  it('keeps the gallery it has when a refetch fails', async () => {
    mockUseAllWorkspaces.mockImplementation(() => ({ ...listed([HOME, ...WORKSPACES]), error: new Error('offline') }));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await cards()).toHaveLength(2);
    expect(screen.queryByText('Failed to load workspaces. Please refresh the page.')).not.toBeInTheDocument();
  });

  it('says so, with a retry, when there is nothing to show', async () => {
    mockUseAllWorkspaces.mockImplementation(() => ({ data: undefined, isLoading: false, error: new Error('offline') }));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await screen.findByText('Failed to load workspaces. Please refresh the page.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();
    expect(screen.queryAllByTestId('workspace-card')).toHaveLength(0);
  });
});

describe('WorkspaceGallery, a user with only Home', () => {
  it('shows the Chief of Staff and an empty workspaces panel, with no toolbar to search nothing', async () => {
    mockUseAllWorkspaces.mockImplementation(() => listed([HOME]));
    renderWithProviders(<WorkspaceGallery onWorkspaceSelect={vi.fn()} />);

    expect(await screen.findByText('No workspaces yet')).toBeInTheDocument();
    expect(screen.getByTestId('chief-of-staff')).toBeInTheDocument();
    // The computer stays reachable; there is just nothing to search.
    expect(screen.queryByRole('searchbox')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Create Workspace' })).toBeInTheDocument();
  });
});
