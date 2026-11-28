/**
 * Closing a page tab from the keyboard hands focus to the tab in its place
 * rather than dropping it on <body>; a click leaves focus alone.
 */
import { useState } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { fireEvent, screen } from '@testing-library/react';
import i18n from '@/i18n';
import { renderWithProviders } from '@/test/utils';
import {
  PageTabActionsContext,
  PageTabsContext,
  type PageTabActions,
  type PageTabsState,
} from '@/components/PageTabs/pageTabsContext';

vi.mock('@/components/Sidebar/SidebarCorner', () => ({ SidebarCorner: () => null }));

import AppTopBar from '../AppTopBar';

const TABS: PageTabsState = {
  tabs: [
    { id: 'a', path: '/dashboard', title: 'Alpha' },
    { id: 'b', path: '/dashboard', title: 'Beta' },
    { id: 'c', path: '/dashboard', title: 'Gamma' },
  ],
  activeId: 'a',
};

function Strip() {
  const [state, setState] = useState(TABS);
  const actions: PageTabActions = {
    select: vi.fn(),
    open: vi.fn(),
    publishTitle: vi.fn(),
    guardLeave: () => () => {},
    close: (id) => setState((s) => ({ ...s, tabs: s.tabs.filter((tab) => tab.id !== id) })),
  };
  return (
    <PageTabActionsContext.Provider value={actions}>
      <PageTabsContext.Provider value={state}>
        <AppTopBar sidebarHidden={false} onToggleSidebar={() => {}} peek={{} as never} cornerRef={null} spans slotRef={() => {}} />
      </PageTabsContext.Provider>
    </PageTabActionsContext.Provider>
  );
}

const closeButton = (title: string) => screen.getByRole('button', { name: i18n.t('pageTabs.close', { title }) });

describe('AppTopBar page tabs', () => {
  // jsdom lays nothing out and has no scrollIntoView.
  beforeEach(() => { Element.prototype.scrollIntoView = vi.fn(); });

  it('focuses the tab to the right of one closed from the keyboard, else the one to its left', () => {
    renderWithProviders(<Strip />);
    const beta = closeButton('Beta');
    beta.focus();
    fireEvent.keyDown(beta, { key: 'Enter' });
    fireEvent.click(beta);
    expect(screen.getByRole('button', { name: 'Gamma' })).toHaveFocus();

    const gamma = closeButton('Gamma');
    gamma.focus();
    fireEvent.keyDown(gamma, { key: 'Enter' });
    fireEvent.click(gamma);
    expect(screen.getByRole('button', { name: 'Alpha' })).toHaveFocus();
  });

  it('leaves focus alone after a click closes a tab', () => {
    renderWithProviders(<Strip />);
    const beta = closeButton('Beta');
    fireEvent.pointerDown(beta);
    beta.focus();
    fireEvent.click(beta);
    expect(screen.getByRole('button', { name: 'Gamma' })).not.toHaveFocus();
  });
});
