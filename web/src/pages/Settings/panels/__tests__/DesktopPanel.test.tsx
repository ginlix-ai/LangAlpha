/**
 * DesktopPanel's menu bar and panel rows: each list is written back whole,
 * the shell's later views redraw it, and a platform without text beside the
 * icon says where the items go instead.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, fireEvent, screen, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import type { DesktopSettingsPatch, DesktopSettingsView } from '@/lib/desktop';

const h = vi.hoisted(() => ({
  platform: 'darwin',
  view: null as DesktopSettingsView | null,
  listener: null as ((view: DesktopSettingsView) => void) | null,
  patches: [] as DesktopSettingsPatch[],
  saveFails: false,
}));

vi.mock('@/lib/desktop', () => ({
  get desktop() {
    return { platform: h.platform };
  },
  getDesktopSettings: async () => h.view,
  // Takes the patch the way the shell does (one level deep), unless told the save fails.
  setDesktopSettings: async (patch: DesktopSettingsPatch) => {
    h.patches.push(patch);
    if (h.saveFails || !h.view) return null;
    const next: Record<string, unknown> = { ...h.view };
    for (const [key, value] of Object.entries(patch)) {
      next[key] = value && typeof value === 'object' && !Array.isArray(value) ? { ...(next[key] as object), ...value } : value;
    }
    h.view = next as unknown as DesktopSettingsView;
    return h.view;
  },
  onDesktopSettings: (cb: (view: DesktopSettingsView) => void) => {
    h.listener = cb;
    return () => { h.listener = null; };
  },
}));

vi.mock('@/pages/Dashboard/utils/watchlist', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/Dashboard/utils/watchlist')>()),
  listWatchlists: async () => ({
    watchlists: [
      { watchlist_id: 'w1', name: 'Main', is_default: true },
      { watchlist_id: 'w2', name: 'Semis', is_default: false },
    ],
  }),
}));

import { DesktopPanel } from '../DesktopPanel';

function view(overrides: Partial<DesktopSettingsView> = {}): DesktopSettingsView {
  return {
    notifications: { turn: true, needs_input: true, wakeup: true, automation: true, started: true },
    menubar: {
      enabled: true,
      items: [{ kind: 'portfolio' }, { symbol: 'NVDA', kind: 'stock' }, { symbol: 'GSPC', kind: 'index' }],
      style: 'rotate',
      rotateSeconds: 4,
      show: 'change',
      extendedHours: true,
    },
    panel: {
      sections: [
        { id: 'market', visible: true },
        { id: 'portfolio', visible: true },
        { id: 'watchlist', visible: true },
        { id: 'agent', visible: true },
      ],
      tiles: [{ symbol: 'GSPC', kind: 'index' }, { symbol: 'VIX', kind: 'index' }],
      watchlistId: null,
      watchlistRows: 5,
    },
    openAtLogin: false,
    shortcut: '',
    keepRunningInTray: true,
    support: {
      menubarPanel: true,
      trayTitle: true,
      openAtLogin: true,
      shortcut: true,
      keepRunningInTray: false,
      notificationActions: true,
    },
    ...overrides,
  };
}

function itemsRow() {
  return screen.getByText('In the menu bar').closest('.settings-row') as HTMLElement;
}

describe('DesktopPanel menu bar and panel', () => {
  beforeEach(() => {
    h.platform = 'darwin';
    h.view = view();
    h.listener = null;
    h.patches = [];
    h.saveFails = false;
  });

  it('draws the items beside the icon in order, with the rotate controls on a Mac', async () => {
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('In the menu bar');
    const row = within(itemsRow());
    expect(row.getAllByRole('listitem').map((li) => li.textContent?.trim())).toEqual(
      expect.arrayContaining(['Portfolio', 'NVDA', 'S&P 500']),
    );
    expect(screen.getByText('Shown next to the icon. Drag to reorder.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Rotate' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.getByLabelText('Rotate every')).toHaveValue('4');
  });

  it('writes the whole list back when an item is removed', async () => {
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('In the menu bar');
    fireEvent.click(screen.getByRole('button', { name: 'Remove NVDA' }));
    expect(h.patches.at(-1)).toEqual({
      menubar: { items: [{ kind: 'portfolio' }, { symbol: 'GSPC', kind: 'index' }] },
    });
    expect(within(itemsRow()).queryByText('NVDA')).not.toBeInTheDocument();
  });

  it('draws what the shell holds again when a save is not taken', async () => {
    h.saveFails = true;
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('In the menu bar');
    fireEvent.click(screen.getByRole('button', { name: 'Remove NVDA' }));
    expect(within(itemsRow()).queryByText('NVDA')).not.toBeInTheDocument();
    expect(await within(itemsRow()).findByText('NVDA')).toBeInTheDocument();
  });

  it('redraws from a view the shell sends later', async () => {
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('In the menu bar');
    const next = view();
    next.menubar = { ...next.menubar, items: [{ symbol: 'TSLA', kind: 'stock' }] };
    act(() => h.listener?.(next));
    expect(within(itemsRow()).getByText('TSLA')).toBeInTheDocument();
    expect(within(itemsRow()).queryByText('NVDA')).not.toBeInTheDocument();
  });

  it('says a login launch opens the window where there is no Dock and the icon is off', async () => {
    const background = 'Start LangAlpha in the background when you sign in to this computer.';
    const opensWindow = 'Open LangAlpha when you sign in to this computer. With the icon on, it starts in the background instead.';
    h.platform = 'win32';
    const iconOff = view();
    iconOff.menubar = { ...iconOff.menubar, enabled: false };
    h.view = iconOff;
    renderWithProviders(<DesktopPanel />);
    expect(await screen.findByText(opensWindow)).toBeInTheDocument();
    // The icon back on, the launch is a background one again.
    act(() => h.listener?.(view()));
    expect(screen.getByText(background)).toBeInTheDocument();
  });

  it('keeps the background wording on a Mac with the icon off, where the Dock is the way in', async () => {
    const iconOff = view();
    iconOff.menubar = { ...iconOff.menubar, enabled: false };
    h.view = iconOff;
    renderWithProviders(<DesktopPanel />);
    expect(await screen.findByText('Start LangAlpha in the background when you sign in to this computer.')).toBeInTheDocument();
  });

  it('says where the items go on Windows, with no rotate controls', async () => {
    h.platform = 'win32';
    h.view = view({ support: { ...view().support, trayTitle: false } });
    renderWithProviders(<DesktopPanel />);
    await screen.findByText("Shown in the tray icon's tooltip. Drag to reorder.");
    expect(screen.queryByRole('button', { name: 'Rotate' })).not.toBeInTheDocument();
    expect(screen.getByText('Extended hours')).toBeInTheDocument();
  });

  it('says where the items go on Linux', async () => {
    h.platform = 'linux';
    h.view = view({ support: { ...view().support, trayTitle: false, menubarPanel: false } });
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('Shown at the top of the tray menu. Drag to reorder.');
  });

  it('hides a panel section by writing all four back', async () => {
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('Sections');
    fireEvent.click(screen.getByRole('checkbox', { name: 'Agent' }));
    expect(h.patches.at(-1)).toEqual({
      panel: {
        sections: [
          { id: 'market', visible: true },
          { id: 'portfolio', visible: true },
          { id: 'watchlist', visible: true },
          { id: 'agent', visible: false },
        ],
      },
    });
  });

  it('offers the default watchlist once, by name, and saves another by id', async () => {
    renderWithProviders(<DesktopPanel />);
    const which = await screen.findByLabelText('Which watchlist');
    await waitFor(() => expect(within(which).getAllByRole('option').map((o) => o.textContent)).toEqual(['Main (default)', 'Semis']));
    fireEvent.change(which, { target: { value: 'w2' } });
    expect(h.patches.at(-1)).toEqual({ panel: { watchlistId: 'w2' } });
    fireEvent.change(screen.getByLabelText('Rows'), { target: { value: '8' } });
    expect(h.patches.at(-1)).toEqual({ panel: { watchlistRows: 8 } });
  });

  it('draws nothing of the panel while the icon is off', async () => {
    h.view = view({ menubar: { ...view().menubar, enabled: false } });
    renderWithProviders(<DesktopPanel />);
    await screen.findByText('Show icon in the menu bar');
    expect(screen.queryByText('In the menu bar')).not.toBeInTheDocument();
    expect(screen.queryByText('Sections')).not.toBeInTheDocument();
  });

  it('opens a search for a row the icon hides on the icon switch, and on the row once it is drawn', async () => {
    h.view = view({ menubar: { ...view().menubar, enabled: false } });
    const { container } = renderWithProviders(<DesktopPanel />, { route: '/settings?tab=desktop#panel-tiles' });
    const icon = (await screen.findByText('Show icon in the menu bar')).closest('.settings-row');
    expect(icon).toHaveAttribute('id', 'panel-tiles');
    act(() => h.listener?.(view()));
    expect(container.querySelectorAll('#panel-tiles')).toHaveLength(1);
    expect(screen.getByText('Show icon in the menu bar').closest('.settings-row')).toHaveAttribute('id', 'menubar-icon');
  });
});
