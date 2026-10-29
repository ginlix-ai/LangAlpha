import { useMutation, useQuery, useQueryClient, type QueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { queryKeys } from '../lib/queryKeys';
import {
  deleteSubagent,
  formatApiErrorDetail,
  getSubagents,
  isSubagentVersionConflict,
  putSubagent,
  SUBAGENT_INHERIT,
  type SubagentBaseValues,
  type SubagentFields,
  type SubagentsResponse,
  type SubagentView,
} from '../pages/ChatAgent/utils/api';

/**
 * React Query hooks for the Subagents pane. One list entry holds every
 * subagent, so every write lands back in it: the PUT's answer replaces its
 * row, then the list is re-read, since one write can change another row's
 * problems (a description that now matches, a name that now collides).
 *
 * Writes share one mutation scope, so they reach the server in the order the
 * user made them and each carries the version the one before it produced.
 * Two switches flipped in a row would otherwise both send the version they
 * were clicked on, and the second would be refused as a conflict with the
 * first.
 */

const WRITE_SCOPE = { id: 'subagents' };

export function useSubagents() {
  return useQuery({
    queryKey: queryKeys.subagents.list(),
    queryFn: getSubagents,
    staleTime: 30_000,
    // The agent edits these files mid-thread, and the main agent's settings
    // and each row's problems follow what the user changes in Settings, both
    // away from this page, so a surface showing the list re-reads it when it
    // appears or the window regains focus, even while fresh.
    refetchOnMount: 'always',
    refetchOnWindowFocus: 'always',
  });
}

function cachedList(qc: QueryClient): SubagentsResponse | undefined {
  return qc.getQueryData<SubagentsResponse>(queryKeys.subagents.list());
}

function cachedView(qc: QueryClient, name: string): SubagentView | undefined {
  return cachedList(qc)?.subagents.find((s) => s.name === name);
}

function putRow(qc: QueryClient, view: SubagentView) {
  qc.setQueryData<SubagentsResponse>(queryKeys.subagents.list(), (prev) => {
    if (!prev) return prev;
    const at = prev.subagents.findIndex((s) => s.name === view.name);
    const subagents =
      at < 0
        ? [...prev.subagents, view]
        : prev.subagents.map((s, i) => (i === at ? view : s));
    return { ...prev, subagents };
  });
}

/** An edit as a control makes it: the value it now shows. */
export type SubagentEdit = Partial<SubagentBaseValues> &
  Pick<SubagentFields, 'description' | 'instructions' | 'available_to'>;

/** Fields where null means "follow the thread" (or the user's own setting). */
const INHERITABLE: ReadonlySet<string> = new Set(['model', 'effort', 'search_provider', 'search_depth']);

/** Lists the agent reads in order: preferred connectors lead its roster and
 *  preferred skills preload in it. The others are sets. */
const ORDERED: ReadonlySet<string> = new Set(['preferred_connectors', 'preferred_skills']);

/**
 * What an edit sends. On a base, a value equal to what the base ships with
 * goes as null, which drops the user's key rather than pinning a copy of
 * today's base value that would hold once the base moves on. Choosing to
 * follow the thread on a base that pins a value goes as `inherit`, since a
 * null there would clear the key and land back on the pin.
 */
function encodeEdit(view: SubagentView, edit: SubagentEdit): SubagentFields {
  const base = view.base;
  if (!base) return edit;
  const fields: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(edit)) {
    const shipped: unknown = base[key as keyof SubagentBaseValues];
    const same = Array.isArray(value) && Array.isArray(shipped)
      ? value.length === shipped.length &&
        value.every((v, i) => (ORDERED.has(key) ? shipped[i] === v : shipped.includes(v)))
      : value === shipped;
    fields[key] = same ? null : value === null && INHERITABLE.has(key) ? SUBAGENT_INHERIT : value;
  }
  return fields as SubagentFields;
}

/**
 * Sends an edit to an existing subagent, encoded against the row the cache
 * holds when the write runs rather than when it was asked for: an earlier
 * write has replaced the version by then.
 */
export function sendSubagentEdit(qc: QueryClient, name: string, edit: SubagentEdit): Promise<SubagentView> {
  const view = cachedView(qc, name);
  return putSubagent(name, { version: view?.version ?? null, fields: view ? encodeEdit(view, edit) : edit });
}

/** A write's radius, and the bulk bar's after its whole run: the one list. */
export function invalidateSubagents(qc: QueryClient) {
  void qc.invalidateQueries({ queryKey: queryKeys.subagents.all });
}

/** Says a write was refused because the file moved under it, and re-reads. */
function useConflictToast() {
  const { t } = useTranslation();
  return (err: unknown) => {
    if (isSubagentVersionConflict(err)) {
      toast({
        title: t('plugins.subagents.conflictTitle'),
        description: t('plugins.subagents.conflictBody'),
      });
      return true;
    }
    return false;
  };
}

export interface SubagentPatch {
  name: string;
  edit: SubagentEdit;
}

/** What an edit to an existing subagent shares: the scope, the version read
 *  when it runs, and the server's answer landing in the list. */
function editOptions(qc: QueryClient) {
  return {
    scope: WRITE_SCOPE,
    mutationFn: ({ name, edit }: SubagentPatch) => sendSubagentEdit(qc, name, edit),
    onSuccess: (view: SubagentView) => {
      putRow(qc, view);
      invalidateSubagents(qc);
    },
  };
}

/**
 * One control's write to an existing subagent. Shown at once and read back
 * from the server's answer; a refusal puts the list back as the server has it.
 */
export function usePatchSubagent() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const conflict = useConflictToast();
  return useMutation({
    ...editOptions(qc),
    onMutate: async ({ name, edit }) => {
      await qc.cancelQueries({ queryKey: queryKeys.subagents.list() });
      const view = cachedView(qc, name);
      if (view) putRow(qc, { ...view, ...edit });
    },
    onError: (err) => {
      invalidateSubagents(qc);
      if (conflict(err)) return;
      toast({
        variant: 'destructive',
        title: t('plugins.subagents.saveFailed'),
        description: formatApiErrorDetail(err),
      });
    },
  });
}

/**
 * A new custom subagent, blank or copied from another. The caller shows a
 * refused field beside the field, so this only lands the row.
 */
export function useCreateSubagent() {
  const qc = useQueryClient();
  return useMutation({
    scope: WRITE_SCOPE,
    mutationFn: ({
      name,
      startFrom,
      fields,
    }: {
      name: string;
      startFrom: string | null;
      fields: SubagentFields;
    }) => putSubagent(name, { version: null, start_from: startFrom, fields }),
    onSuccess: (view) => {
      putRow(qc, view);
      invalidateSubagents(qc);
    },
    onError: () => invalidateSubagents(qc),
  });
}

/**
 * Text the user drafted. The caller first checks that the saved text is still
 * what the draft started from, so this sends the latest version: a switch
 * flipped while typing must not refuse the save. A refusal keeps the draft,
 * and the caller says why beside it.
 */
export function useSaveSubagentText() {
  const qc = useQueryClient();
  return useMutation({
    ...editOptions(qc),
    onError: () => invalidateSubagents(qc),
  });
}

/** Deletes a custom subagent, or drops a base's override (the reset). */
export function useDeleteSubagent() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const conflict = useConflictToast();
  return useMutation({
    scope: WRITE_SCOPE,
    mutationFn: (name: string) => {
      const version = cachedView(qc, name)?.version ?? '';
      return deleteSubagent(name, version);
    },
    onSuccess: () => invalidateSubagents(qc),
    onError: (err) => {
      invalidateSubagents(qc);
      if (conflict(err)) return;
      toast({
        variant: 'destructive',
        title: t('plugins.subagents.saveFailed'),
        description: formatApiErrorDetail(err),
      });
    },
  });
}
