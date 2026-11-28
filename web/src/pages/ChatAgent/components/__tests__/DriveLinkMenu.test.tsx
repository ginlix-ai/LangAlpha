/**
 * A Drive link gets the chat's menu only where the chat opens it beside the
 * conversation; elsewhere (a share, a phone, outside the app's router) the
 * link is left exactly as it came, with the browser's own menu.
 */
import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import '@/i18n';

const mobile = vi.hoisted(() => ({ value: false }));
vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => mobile.value }));

import { DriveLinkMenu } from '../DriveLinkMenu';
import { LinkActionsContext, NO_LINK_ACTIONS } from '../../contexts/LinkActionsContext';

const HREF = '/drive?file=notes%2Fq3.md';

function show({ router = true, opens = true }: { router?: boolean; opens?: boolean } = {}) {
  const tree = (
    <LinkActionsContext value={{ ...NO_LINK_ACTIONS, openAppRoute: opens ? () => true : null }}>
      <DriveLinkMenu href={HREF}><a href={HREF}>q3.md</a></DriveLinkMenu>
    </LinkActionsContext>
  );
  render(router ? <MemoryRouter>{tree}</MemoryRouter> : tree);
  fireEvent.contextMenu(screen.getByRole('link', { name: 'q3.md' }));
}

describe('DriveLinkMenu', () => {
  it('offers the Drive page, a new tab and the link where the chat opens it', () => {
    mobile.value = false;
    show();
    expect(screen.getByRole('menuitem', { name: /Open in Drive/ })).toBeInTheDocument();
  });

  it.each([
    ['in a share, which opens no app route', { opens: false }],
    ['outside the app\'s router', { router: false }],
  ])('leaves the link alone %s', (_where, opts) => {
    mobile.value = false;
    show(opts);
    expect(screen.queryByRole('menuitem')).toBeNull();
    expect(screen.getByRole('link', { name: 'q3.md' })).toHaveAttribute('href', HREF);
  });

  it('leaves the link alone on a phone, which keeps its own long press', () => {
    mobile.value = true;
    show();
    expect(screen.queryByRole('menuitem')).toBeNull();
  });
});
