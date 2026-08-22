import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import type { BulkScopeSpec } from '../components/BulkScopeMenu';
import type { ScopeWorkspace } from '../components/ScopeControl';
import type { BulkTarget } from '../components/useBulkSelection';
import { clearDenyPlan, onlyInPlan } from '../utils/scopeTargets';

/**
 * The three bulk scope actions — all workspaces, only in a chosen set, move
 * into one — over any selection of rows.
 *
 * The algorithm is the same wherever it runs: a user-tier row is active
 * everywhere minus its deny markers, so "everywhere" and "only in X" both
 * reduce to the per-workspace toggles whose current state differs from the
 * wanted one, and rows the action can't reach are left out of the run rather
 * than failed. Only the endpoints and the eligibility rules differ per tab,
 * and those are what the adapter names.
 */

export interface ScopeBulkAdapter<T> {
  /** The workspaces every scope action addresses. */
  workspaces: ScopeWorkspace[];
  /** The tab's bulk runner. */
  run: (targets: BulkTarget[]) => void;
  /** Bulk-target key for a row; it only labels failures. */
  key: (row: T) => string;
  /**
   * The row's live per-workspace deny markers, or null when the row has no
   * deny checklist at all — a workspace-tier row (it is already in exactly one
   * place), or a row switched off at the user tier, where a per-workspace
   * marker means nothing.
   */
  denyMarkers: (row: T) => readonly string[] | null;
  /** Flip one workspace's deny marker on a deny-eligible row. */
  setWorkspaceEnabled: (
    row: T,
    workspaceId: string,
    enabled: boolean,
  ) => Promise<unknown>;
  /** Surface a workspace row to the user tier, or null if this row cannot. */
  promote: (row: T) => (() => Promise<unknown>) | null;
  /**
   * The row can move into some workspace. The menu counts before a destination
   * is picked, so this is destination-blind and `moveTo` gets the final say —
   * a row already living in the chosen workspace is counted here and skipped
   * there.
   */
  movable: (row: T) => boolean;
  /** Move the row into `workspaceId`, or null if it cannot go there. */
  moveTo: (row: T, workspaceId: string) => (() => Promise<unknown>) | null;
}

export function useScopeBulk<T>(
  /** The selected rows, in whatever union shape the tab's tiers need. */
  rows: readonly T[],
  adapter: ScopeBulkAdapter<T>,
): BulkScopeSpec {
  const { t } = useTranslation();
  const liveWsIds = adapter.workspaces.map((w) => w.id);

  function denyTarget(row: T, chosen: ReadonlySet<string> | null): BulkTarget | null {
    const markers = adapter.denyMarkers(row);
    if (!markers) return null;
    const plan = chosen
      ? onlyInPlan(markers, liveWsIds, chosen)
      : clearDenyPlan(markers, liveWsIds);
    if (plan.length === 0) return null;
    return {
      key: adapter.key(row),
      run: async () => {
        for (const step of plan) {
          await adapter.setWorkspaceEnabled(row, step.workspaceId, step.enabled);
        }
      },
    };
  }

  function targets(build: (row: T) => (() => Promise<unknown>) | null): BulkTarget[] {
    return rows.flatMap((row) => {
      const run = build(row);
      return run ? [{ key: adapter.key(row), run }] : [];
    });
  }

  // A scope pick that resolves to no calls is a no-op, not a failure: say so
  // rather than running an empty fan-out and reporting "0 succeeded".
  function runScope(scopeTargets: BulkTarget[]) {
    if (scopeTargets.length === 0) {
      toast({ title: t('plugins.bulk.noChanges') });
      return;
    }
    adapter.run(scopeTargets);
  }

  const denyEligible = rows.filter((row) => adapter.denyMarkers(row) !== null);
  const promoteTargets = targets(adapter.promote);
  const clearTargets = denyEligible
    .map((row) => denyTarget(row, null))
    .filter((x): x is BulkTarget => x !== null);

  return {
    workspaces: adapter.workspaces,
    everywhereCount: promoteTargets.length + clearTargets.length,
    onEverywhere: () => runScope([...promoteTargets, ...clearTargets]),
    onlyInCount: denyEligible.length,
    onOnlyIn: (workspaceIds) => {
      const chosen = new Set(workspaceIds);
      runScope(
        denyEligible
          .map((row) => denyTarget(row, chosen))
          .filter((x): x is BulkTarget => x !== null),
      );
    },
    moveCount: rows.filter(adapter.movable).length,
    onMoveTo: (workspaceId) =>
      runScope(targets((row) => adapter.moveTo(row, workspaceId))),
  };
}
