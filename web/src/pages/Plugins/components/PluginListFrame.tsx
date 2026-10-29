import type { ReactNode } from 'react';
import { ListSkeleton } from '@/components/mcp/McpPrimitives';
import type { DiscoverScope } from '../utils/marketplace';
import type { BulkActions } from '../utils/bulkActions';
import type { PluginList } from '../hooks/usePluginListSurface';
import { BulkActionBar } from './BulkActionBar';
import type { BulkScopeSpec } from './BulkScopeMenu';
import { FilterEmpty } from './FilterEmpty';
import { ListControls } from './ListControls';
import { LoadFailed } from './LoadFailed';

/**
 * The frame every Plugins tab draws its groups in: the pills and Select over
 * them, one ladder for what replaces them (a load that never answered, the
 * first load, a narrowing that left nothing), and the bulk bar under them.
 *
 * Kept mounted under a detail page (`hidden`) so a fold opened or a scroll
 * position is still there on the way back.
 */
export function PluginListFrame<T>({
  list,
  query,
  loadFailed,
  noun,
  catalogScope,
  bulk,
  hidden,
  className,
  groupsClassName,
  below,
  confirm,
  footer,
  children,
}: {
  list: PluginList<T>;
  /** The tab's own list query. A failed refetch keeps the rows it already
   *  had, so only a load that never answered takes the groups' place. */
  query: { data: unknown; isLoading: boolean; error: unknown; refetch: () => unknown };
  /** Which list failed, in the page's words. */
  loadFailed: string;
  /** What the tab lists, for the empty narrowing ("plugins"). */
  noun: string;
  /** The catalog side a search also counts matches on, where there is one. */
  catalogScope?: DiscoverScope;
  bulk: BulkActions & { scope?: BulkScopeSpec };
  hidden: boolean;
  className?: string;
  /** Wraps the groups and `below` together, where the tab spaces them. */
  groupsClassName?: string;
  /** Drawn under the groups whatever the ladder says, for rows another
   *  query answers (the connectors the app ships). */
  below?: ReactNode;
  /** A question a row is asking, which the bar sits under. */
  confirm?: ReactNode;
  /** After the bar: what follows the list. */
  footer?: ReactNode;
  children: ReactNode;
}) {
  let groups: ReactNode;
  if (query.error && !query.data) {
    groups = (
      <LoadFailed what={loadFailed} error={query.error} onRetry={() => void query.refetch()} />
    );
  } else if (query.isLoading) {
    groups = <ListSkeleton />;
  } else if (list.nothingMatches) {
    // Before the empty account: a search over nothing still answers that it
    // found nothing, where the first-run block (held back while narrowed)
    // would leave the body blank.
    groups = (
      <FilterEmpty
        noun={noun}
        filter={list.filter}
        stateFilter={list.stateFilter}
        onReset={list.reset}
        catalogScope={catalogScope}
      />
    );
  } else {
    groups = children;
  }
  const body = (
    <>
      {groups}
      {below}
    </>
  );

  return (
    <div className={className} hidden={hidden}>
      {!query.isLoading && list.controls && <ListControls {...list.controls} />}
      {groupsClassName ? <div className={groupsClassName}>{body}</div> : body}
      {confirm}
      {list.selection.selecting && !hidden && (
        <BulkActionBar {...bulk} progress={list.progress} onExit={list.selection.exit} />
      )}
      {footer}
    </div>
  );
}
