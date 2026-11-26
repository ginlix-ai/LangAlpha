import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import { TopBarSlotContext } from '@/components/TopBar/topBarSlot';
import DashboardHeader from '../DashboardHeader';

const mobile = vi.hoisted(() => ({ value: false }));
const palette = vi.hoisted(() => ({ open: vi.fn(), close: vi.fn(), preload: vi.fn(), isOpen: false }));

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => mobile.value }));
vi.mock('@/components/CommandPalette/CommandPaletteProvider', () => ({ useCommandPalette: () => palette }));
function Where() {
  const { pathname, search } = useLocation();
  return <output aria-label="Where">{pathname + search}</output>;
}

const claim = vi.fn(() => () => {});

function renderHeader(slot: HTMLElement | null = null) {
  return render(
    <MemoryRouter initialEntries={['/dashboard']}>
      <TopBarSlotContext.Provider value={{ slot, claim }}>
        <DashboardHeader
          actions={<button type="button">Built-in dashboard</button>}
          edit={{ editMode: false, onEditModeChange: () => {} }}
        />
      </TopBarSlotContext.Provider>
      <Where />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('DashboardHeader search', () => {
  it('on a phone, opens the command palette at tickers instead of a field of its own', () => {
    mobile.value = true;
    renderHeader();
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    const field = screen.getByRole('button', { name: 'Search stocks by symbol or company name' });
    expect(field).toHaveTextContent('Search by symbol or company name...');

    fireEvent.pointerDown(field);
    expect(palette.preload).toHaveBeenCalled();
    fireEvent.click(field);
    expect(palette.open).toHaveBeenCalledWith({ kind: 'tickers' });
    expect(document.querySelector('.dashboard-search-dropdown')).toBeNull();

    // A hardware keyboard's / opens it too.
    palette.open.mockClear();
    fireEvent.keyDown(document.body, { key: '/' });
    expect(palette.open).toHaveBeenCalledWith({ kind: 'tickers' });
  });

  it('on a desktop, draws no header: it puts the top bar up, its controls go there and / opens the palette', () => {
    mobile.value = false;
    const slot = document.createElement('div');
    document.body.appendChild(slot);
    renderHeader(slot);

    expect(claim).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole('textbox')).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Search stocks by symbol or company name' })).not.toBeInTheDocument();
    expect(slot).toContainElement(screen.getByRole('button', { name: 'Built-in dashboard' }));
    expect(slot).toContainElement(screen.getByRole('button', { name: 'Edit layout' }));

    fireEvent.keyDown(document.body, { key: '/' });
    expect(palette.open).toHaveBeenCalledWith({ kind: 'tickers' });
    slot.remove();
  });
});
