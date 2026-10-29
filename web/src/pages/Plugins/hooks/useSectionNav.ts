import { useSearchParams } from 'react-router';
import { readSection } from '../components/shell/sections';
import { SCOPE_TAB, type DiscoverScope } from '../utils/marketplace';
import {
  DETAIL_KIND_TAB,
  VIEW_PARAM,
  readView,
  withDetail,
  type DetailKind,
  type SectionView,
} from '../utils/detailParam';

type Ref = { kind: DetailKind; name: string; workspaceId?: string | null };

/**
 * Every way one place on the Plugins page opens another, so each link keeps
 * the same params.
 *
 * The section moves with the detail, and a move to another section leaves the
 * search and the view behind: the search is words typed about this section's
 * rows, and carried along it filters the other section's list by them, often
 * to nothing. Pushed, like every overlay open, so Back returns to where the
 * link was.
 */
export function useSectionNav() {
  const [searchParams, setSearchParams] = useSearchParams();

  /** `view` null keeps the one showing, within the same section. */
  function go(ref: Ref | null, tab: string, view: SectionView | null, opts?: { replace?: boolean }) {
    const next = withDetail(searchParams, ref);
    const moving = readSection(searchParams) !== tab;
    if (moving) next.delete('q');
    const shown = view ?? (moving ? 'yours' : readView(searchParams));
    next.set('tab', tab);
    if (shown === 'discover') next.set(VIEW_PARAM, 'discover');
    else next.delete(VIEW_PARAM);
    setSearchParams(next, opts);
  }

  /**
   * An item's own page, on the section that lists it. What is installed lives
   * in Yours, so a connector or a skill opened from a catalog page leaves
   * Discover; a plugin's page opens in either view.
   */
  function openDetail(ref: Ref, opts?: { replace?: boolean }) {
    const view = ref.kind === 'catalog' ? 'discover' : ref.kind === 'plugin' ? null : 'yours';
    go(ref, DETAIL_KIND_TAB[ref.kind], view, opts);
  }

  /** A section's catalog, at one entry's page when named. */
  function openDiscover(scope: DiscoverScope, entry?: string) {
    go(entry ? { kind: 'catalog', name: entry } : null, SCOPE_TAB[scope], 'discover');
  }

  return { openDetail, openDiscover };
}
