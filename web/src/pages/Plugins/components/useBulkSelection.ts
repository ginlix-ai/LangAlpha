import { useCallback, useState } from 'react';
import { useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { invalidatePluginFanout } from '@/hooks/usePlugins';
import { runBulk } from '../utils/bulkRun';

/**
 * Selection-mode state for one tab of the Plugins page. Keys are
 * tab-namespaced row identifiers (`catalog:<name>`, `<wsid>:<name>`, …); the
 * tab that owns the hook maps keys back to rows when it assembles the bulk
 * actions, so the hook itself never learns row semantics.
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

/** ServerRowShell selection props for one row, or nothing outside select mode. */
export function rowSelection(selection: BulkSelection, key: string) {
  if (!selection.selecting) return {};
  return {
    selecting: true,
    selected: selection.selected.has(key),
    onSelectToggle: () => selection.toggle(key),
  };
}

/** One executable unit of a bulk action; `key` only labels failures. */
export interface BulkTarget {
  key: string;
  run: () => Promise<unknown>;
}

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
  const queryClient = useQueryClient();
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);

  const run = useCallback(
    async (targets: BulkTarget[]) => {
      if (targets.length === 0) return;
      setProgress({ done: 0, total: targets.length });
      try {
        const result = await runBulk(targets, (target) => target.run(), {
          onProgress: (done, total) => setProgress({ done, total }),
        });
        const parts = [t('plugins.bulk.okPart', { count: result.ok.length })];
        if (result.failed.length > 0) {
          parts.push(t('plugins.bulk.failPart', { count: result.failed.length }));
        }
        toast({
          variant: result.failed.length > 0 ? 'destructive' : undefined,
          title: t('plugins.bulk.resultTitle'),
          description: parts.join(' · '),
        });
      } finally {
        setProgress(null);
        invalidate(queryClient);
      }
      selection.exit();
    },
    [invalidate, queryClient, selection, t],
  );

  return { progress, run };
}
