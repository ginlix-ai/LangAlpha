import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import type { BulkScopeSpec } from '../components/BulkScopeMenu';
import type { ScopeWorkspace } from '../components/ScopeControl';
import type { BulkTarget } from '../components/useBulkSelection';
import { clearDenyPlan, onlyInPlan } from '../utils/scopeTargets';

/**
 * The bulk scope actions (all workspaces, only in a chosen set, and, where a
 * tab has workspace-tier rows, move into one) over any selection of rows.
 *
 * The algorithm is the same wherever it runs: a user-tier row is active
 * everywhere minus its deny markers, so "everywhere" and "only in X" both
 * reduce to the per-workspace toggles whose current state differs from the
 * wanted one, and rows the action can't reach are left out of the run rather
 * than failed. On a row with a setting of its own for workspaces not created
 * yet, both also decide that: "everywhere" includes them, and "only in X"
 * excludes them. Only the endpoints and the eligibility rules differ per tab,
 * and those are what the adapter names.
 */

export interface ScopeBulkAdapter<T> {
  /** The workspaces every scope action addresses. */
  workspaces: ScopeWorkspace[];
  /** The tab's bulk runner. */
  run: (targets: BulkTarget[]) => void;
  /** Bulk-target key for a row. */
  key: (row: T) => string;
  /** What the user calls the row, which a failure is named by. */
  name: (row: T) => string;
  /**
   * The row's live per-workspace deny markers, or null when the row has no
   * deny checklist at all — a workspace-tier row (it is already in exactly one
   * place), or a row switched off at the user tier, where a per-workspace
   * marker means nothing.
   */
  denyMarkers: (row: T) => readonly string[] | null;
  /**
   * Home when this row's own checklist offers it, else null. Both actions
   * count it as one of the row's workspaces, as the checklist does: "All
   * workspaces" clears its deny and "only in" (which never offers Home as a
   * choice) switches it off, or the badge the action leaves behind counts
   * Home against what the button promised.
   */
  homeWorkspaceId?: (row: T) => string | null;
  /** Flip one workspace's deny marker on a deny-eligible row. */
  setWorkspaceEnabled: (
    row: T,
    workspaceId: string,
    enabled: boolean,
  ) => Promise<unknown>;
  /**
   * Set whether workspaces created later start with a deny-eligible row on,
   * or null when that is already the answer. "All workspaces" asks for on and
   * "only in" for off, each in the same target as the row's deny flips, so
   * the badge it leaves behind says what the button promised and a failure
   * counts once, against the row. Absent on a tab whose rows always start on
   * in a new workspace.
   */
  newWorkspaces?: (row: T, on: boolean) => (() => Promise<unknown>) | null;
  /** Surface a workspace row to the user tier, or null if this row cannot.
   *  Absent on a tab with no workspace-tier rows. */
  promote?: (row: T) => (() => Promise<unknown>) | null;
  /** The two below are the "move into a workspace" action, absent on a tab
   *  whose rows only live on the account (MCP: install at the account level,
   *  select per workspace). */
  move?: {
    /**
     * The row can move into some workspace. The menu counts before a
     * destination is picked, so this is destination-blind and `to` gets the
     * final say: a row already living in the chosen workspace is counted here
     * and skipped there.
     */
    movable: (row: T) => boolean;
    /** Move the row into `workspaceId`, or null if it cannot go there. */
    to: (row: T, workspaceId: string) => (() => Promise<unknown>) | null;
  };
}

export function useScopeBulk<T>(
  /** The selected rows, in whatever union shape the tab's tiers need. */
  rows: readonly T[],
  adapter: ScopeBulkAdapter<T>,
): BulkScopeSpec {
  const { t } = useTranslation();
  const { move } = adapter;
  const liveWsIds = adapter.workspaces.map((w) => w.id);

  function denyTarget(row: T, chosen: ReadonlySet<string> | null): BulkTarget | null {
    const markers = adapter.denyMarkers(row);
    if (!markers) return null;
    const homeId = adapter.homeWorkspaceId?.(row) ?? null;
    const reach = homeId ? [...liveWsIds, homeId] : liveWsIds;
    const plan = chosen ? onlyInPlan(markers, reach, chosen) : clearDenyPlan(markers, reach);
    const setNew = adapter.newWorkspaces?.(row, chosen === null) ?? null;
    if (plan.length === 0 && !setNew) return null;
    return {
      key: adapter.key(row),
      name: adapter.name(row),
      run: async () => {
        for (const step of plan) {
          await adapter.setWorkspaceEnabled(row, step.workspaceId, step.enabled);
        }
        if (setNew) await setNew();
      },
    };
  }

  function targets(build: (row: T) => (() => Promise<unknown>) | null): BulkTarget[] {
    return rows.flatMap((row) => {
      const run = build(row);
      return run ? [{ key: adapter.key(row), name: adapter.name(row), run }] : [];
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
  const promoteTargets = adapter.promote ? targets(adapter.promote) : [];
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
    move: move && {
      count: rows.filter(move.movable).length,
      onMoveTo: (workspaceId) => runScope(targets((row) => move.to(row, workspaceId))),
    },
  };
}
