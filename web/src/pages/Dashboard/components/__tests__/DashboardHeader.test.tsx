import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router';
import DashboardHeader from '../DashboardHeader';

const mobile = vi.hoisted(() => ({ value: false }));
const palette = vi.hoisted(() => ({ open: vi.fn(), close: vi.fn(), preload: vi.fn(), isOpen: false }));

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => mobile.value }));
vi.mock('@/components/CommandPalette/CommandPaletteProvider', () => ({ useCommandPalette: () => palette }));
vi.mock('@/hooks/useSymbolSearch', () => ({
  useSymbolSearch: (query: string) => ({
    hits: query ? [{ symbol: 'NVDA', name: 'NVIDIA Corporation' }] : [],
    loading: false,
  }),
}));

function Where() {
  const { pathname, search } = useLocation();
  return <output aria-label="Where">{pathname + search}</output>;
}

function renderHeader() {
  return render(
    <MemoryRouter initialEntries={['/dashboard']}>
      <DashboardHeader />
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

  it('on a desktop, keeps its field and dropdown, and Enter opens the first hit in Market', () => {
    mobile.value = false;
    renderHeader();
    const input = screen.getByRole('textbox', { name: 'Search stocks by symbol or company name' });

    fireEvent.keyDown(document.body, { key: '/' });
    expect(input).toHaveFocus();
    fireEvent.change(input, { target: { value: 'nvd' } });
    expect(screen.getByRole('button', { name: /NVDA/ })).toBeInTheDocument();
    expect(palette.open).not.toHaveBeenCalled();

    fireEvent.submit(input.closest('form')!);
    expect(screen.getByLabelText('Where')).toHaveTextContent('/market?symbol=NVDA');
  });
});
