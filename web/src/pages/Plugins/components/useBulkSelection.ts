import { createElement, useCallback, useState } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { ToastAction } from '@/components/ui/toast';
import { toast } from '@/components/ui/use-toast';
import { invalidatePluginFanout } from '@/hooks/usePlugins';
import { useLocale } from '@/hooks/useLocale';
import { formatList } from '@/lib/format';
import {
  formatApiErrorDetail,
  mcpSignInNeeded,
  type McpSignInNeeded,
} from '@/pages/ChatAgent/utils/api';
import { runBulk } from '../utils/bulkRun';

/**
 * Selection-mode state for one tab of the Plugins page. Keys are
 * tab-namespaced row identifiers (`catalog:<name>`, `<wsid>:<name>`, …); the
 * tab that owns the hook maps keys back to rows when it assembles the bulk
 * actions, so the hook itself never learns row semantics.
 *
 * Keys survive a filter change, but every tab resolves them against the rows
 * it is currently SHOWING, so a bulk action reaches exactly what is on screen
 * and the bar counts the same set. That split is deliberate: pruning the keys
 * instead would break select-some-then-search-for-the-next, while resolving
 * against the whole account would act on rows the filter is hiding.
 */

export interface BulkSelection {
  selecting: boolean;
  selected: ReadonlySet<string>;
  start: () => void;
  exit: () => void;
  toggle: (key: string) => void;
  setMany: (keys: string[], on: boolean) => void;
}

export function useBulkSelection(): BulkSelection {
  const [selecting, setSelecting] = useState(false);
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());

  const start = useCallback(() => setSelecting(true), []);
  const exit = useCallback(() => {
    setSelecting(false);
    setSelected(new Set());
  }, []);
  const toggle = useCallback((key: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);
  const setMany = useCallback((keys: string[], on: boolean) => {
    setSelected((prev) => {
      const next = new Set(prev);
      for (const key of keys) {
        if (on) next.add(key);
        else next.delete(key);
      }
      return next;
    });
  }, []);

  return { selecting, selected, start, exit, toggle, setMany };
}

/**
 * A value that changes whenever the rows an action would reach do. Keyed by
 * the selected rows ON SCREEN, not the retained selection: a selection
 * survives a filter change on purpose, so a confirm armed on the rows one
 * filter showed would otherwise fire on the rows the next filter shows. The
 * Set's identity says nothing (rebuilt on every toggle) and its size too
 * little: dropping two rows and picking two others leaves the count as it was.
 */
export function bulkSelectionKey(targetKeys: Iterable<string>): string {
  return [...targetKeys].sort().join('\u0000');
}

/** One executable unit of a bulk action. */
export interface BulkTarget<R = unknown> {
  key: string;
  /** What the user calls the row, which the result names it by if it fails. */
  name: string;
  run: () => Promise<R>;
}

/** What a bulk action did, which its result is titled by. */
export type BulkVerb = 'enable' | 'disable' | 'remove' | 'uninstall' | 'scope';
/** What a tab's rows are, which the result counts. */
export type BulkNoun = 'plugin' | 'connector' | 'skill' | 'subagent';

export interface BulkRunOptions<R = unknown> {
  verb: BulkVerb;
  noun: BulkNoun;
  /**
   * Starts the sign-in a switch-on was refused for, from the result's own
   * button, so the outcome and its next step are one message rather than two.
   * Given the first refusal's servers: the result names every row refused,
   * and a sign-in that lands lets the next switch through or names the next.
   */
  signIn?: (servers: McpSignInNeeded[]) => void;
  /** Lines about what the rows that succeeded took with them, from the
   *  values their runs returned, said before any failure. */
  outcome?: (results: R[]) => string[];
}

/** Names a result spells out before it counts the rest. */
const NAMED = 3;

/**
 * Runs a bulk fan-out with progress, then invalidates ONCE (each target hits a
 * raw API function, not a mutation hook, so N ops don't trigger N invalidation
 * storms), toasts the outcome, and drops select mode.
 *
 * The radius is the caller's to name because this runs on three tabs: a tab
 * whose bulk actions cannot change plugin identity — MCP and Skills both keep
 * plugin-owned rows out of bulk delete, and enable/disable/scope leave
 * ownership alone — has no reason to refetch the plugin list. The full radius
 * stays the default, so a new caller is over-invalidated rather than stale.
 */
export function useBulkRunner(
  selection: BulkSelection,
  invalidate: (qc: QueryClient) => void = invalidatePluginFanout,
) {
  const { t } = useTranslation();
  const locale = useLocale();
  const queryClient = useQueryClient();
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);

  const run = useCallback(
    async <R>(targets: BulkTarget<R>[], opts: BulkRunOptions<R>) => {
      if (targets.length === 0) return;
      setProgress({ done: 0, total: targets.length });
      try {
        const results: R[] = [];
        const result = await runBulk(
          targets,
          async (target) => {
            results.push(await target.run());
          },
          { onProgress: (done, total) => setProgress({ done, total }) },
        );
        // A refused sign-in is a next step, not a fault: it is said apart from
        // the failures, by name, and offered rather than reported.
        const refused = result.failed.flatMap(({ item, error }) => {
          const servers = mcpSignInNeeded(error);
          return servers ? [{ item, servers }] : [];
        });
        const failed = result.failed.filter(({ error }) => !mcpSignInNeeded(error));
        const names = (items: BulkTarget[]) => {
          const shown = items.slice(0, NAMED).map((item) => item.name);
          if (items.length > NAMED) {
            shown.push(t('plugins.bulk.moreNames', { count: items.length - NAMED }));
          }
          return formatList(shown, locale);
        };
        // Titled by what happened: what changed when anything did, otherwise
        // what could not be done to all of them.
        const done = result.ok.length > 0;
        const what = t(`plugins.bulk.noun.${opts.noun}`, {
          count: done ? result.ok.length : targets.length,
        });
        const lines: string[] = done && opts.outcome ? opts.outcome(results) : [];
        if (refused.length > 0) {
          lines.push(
            t('plugins.bulk.needSignIn', {
              count: refused.length,
              names: names(refused.map((r) => r.item)),
            }),
          );
        }
        if (failed.length > 0) {
          lines.push(
            t(done ? 'plugins.bulk.failedNamed' : 'plugins.bulk.failedReason', {
              names: names(failed.map((f) => f.item)),
              reason: formatApiErrorDetail(failed[0].error),
            }),
          );
        }
        const firstRefused = refused[0]?.servers;
        const signIn = opts.signIn;
        toast({
          // Alarm only when nothing changed. A run that did most of what was
          // asked is titled by that, and a red toast under "Switched on 3"
          // says the opposite of its own title; the failure line names the
          // row and the reason, which is what the user acts on.
          variant: !done && failed.length > 0 ? 'destructive' : undefined,
          title: t(done ? `plugins.bulk.done.${opts.verb}` : `plugins.bulk.notDone.${opts.verb}`, {
            what,
          }),
          description:
            lines.length > 0
              ? lines.map((line) => createElement('span', { key: line, className: 'block' }, line))
              : undefined,
          // The sign-in is the one next step a refusal has, so it rides this
          // message and keeps it up until it is answered.
          action:
            firstRefused && signIn
              ? createElement(
                  ToastAction,
                  { altText: t('plugins.oauth.signIn'), onClick: () => signIn(firstRefused) },
                  t('plugins.oauth.signIn'),
                )
              : undefined,
          // Names take longer to read than a count.
          duration: firstRefused && signIn ? undefined : result.failed.length > 0 ? 8000 : undefined,
        });
      } finally {
        setProgress(null);
        invalidate(queryClient);
      }
      selection.exit();
    },
    [invalidate, locale, queryClient, selection, t],
  );

  return { progress, run };
}
