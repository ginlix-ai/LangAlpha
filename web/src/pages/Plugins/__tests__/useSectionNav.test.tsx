import { describe, it, expect } from 'vitest';
import { act } from '@testing-library/react';
import { useLocation } from 'react-router';
import { renderHookWithProviders } from '@/test/utils';
import { useSectionNav } from '../hooks/useSectionNav';

function renderNav(route: string) {
  return renderHookWithProviders(
    () => ({ nav: useSectionNav(), search: new URLSearchParams(useLocation().search) }),
    { route },
  );
}

describe('a link from one section of the Plugins page to another', () => {
  // A skill's plugin link carried the skill search onto the Plugins tab, which
  // filtered the plugin list by a skill's name: usually to nothing, with the
  // plugin's page open over an empty list.
  it('leaves the search behind when it opens a plugin from the Skills tab', () => {
    const { result } = renderNav('/plugins?tab=skills&q=earnings');

    act(() => result.current.nav.openDetail({ kind: 'plugin', name: 'ledger' }));

    expect(result.current.search.get('tab')).toBe('plugins');
    expect(result.current.search.get('detail')).toBe('plugin:ledger');
    expect(result.current.search.has('q')).toBe(false);
  });
});
