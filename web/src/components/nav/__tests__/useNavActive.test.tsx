/**
 * Active-state matrix for the shared nav config:
 *  - every item is active at its exact path
 *  - 'exact-or-sub' items are active at sub-paths (key + '/...') only
 *  - the 'prefix' item (/chat) is active at any deeper path
 *  - sibling items never light up for each other's routes
 */
import React from 'react';
import { describe, it, expect } from 'vitest';
import { renderHook } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { NAV_ITEMS, SETTINGS_ITEM } from '../navItems';
import { useNavActive } from '../useNavActive';

const ALL_ITEMS = [...NAV_ITEMS, SETTINGS_ITEM];

function matcherAt(pathname: string) {
  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <MemoryRouter initialEntries={[pathname]}>{children}</MemoryRouter>
  );
  const { result } = renderHook(() => useNavActive(), { wrapper });
  return result.current;
}

describe('useNavActive', () => {
  it.each(ALL_ITEMS.map((item) => [item.key, item] as const))(
    'marks %s active at its exact path',
    (_key, item) => {
      expect(matcherAt(item.key)(item)).toBe(true);
    },
  );

  it.each(
    ALL_ITEMS.filter((item) => item.match === 'exact-or-sub').map(
      (item) => [item.key, item] as const,
    ),
  )('marks exact-or-sub item %s active at a sub-path', (key, item) => {
    expect(matcherAt(`${key}/details`)(item)).toBe(true);
  });

  it('marks the prefix item /chat active at deep paths', () => {
    const chatItem = NAV_ITEMS.find((item) => item.match === 'prefix');
    expect(chatItem).toBeDefined();
    expect(matcherAt('/chat/whatever/deeper')(chatItem!)).toBe(true);
  });

  it.each(ALL_ITEMS.map((item) => [item.key, item] as const))(
    'does not mark siblings active at %s',
    (key, item) => {
      const isActive = matcherAt(key);
      for (const sibling of ALL_ITEMS) {
        if (sibling === item) continue;
        expect(isActive(sibling)).toBe(false);
      }
    },
  );

  it('does not match a route that merely shares the key as a string prefix (exact-or-sub)', () => {
    const dashboard = NAV_ITEMS.find((item) => item.key === '/dashboard')!;
    expect(matcherAt('/dashboard-archive')(dashboard)).toBe(false);
  });
});
