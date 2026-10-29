import { useState } from 'react';
import { useSetMcpServerBinding } from '@/hooks/useMcpServers';
import { formatApiErrorDetail, type McpServerBindingPatch } from '@/pages/ChatAgent/utils/api';

/**
 * Which control asked for a binding write. A change to many tools at once is
 * its own scope rather than a row write with a longer list: it cannot be
 * pinned to the tool it was about, and its refusal belongs in the bar that
 * asked rather than under the row-wide switches.
 */
export type WriteScope =
  | { kind: 'tool'; name: string }
  | { kind: 'server' }
  | { kind: 'bulk' };

export const SERVER: WriteScope = { kind: 'server' };
export const BULK: WriteScope = { kind: 'bulk' };

const scopeKey = (scope: WriteScope) =>
  scope.kind === 'tool' ? `tool:${scope.name}` : scope.kind;

export interface BindingWrites {
  write: (scope: WriteScope, body: McpServerBindingPatch) => Promise<void>;
  busy: (scope: WriteScope) => boolean;
  /** Any write in flight, which every per-tool control falls back to. */
  anyBusy: boolean;
  refusalOn: (scope: WriteScope) => string | null;
}

/**
 * Every binding change on a row is one write on one mutation, told apart by
 * the control that asked for it. The server answers 422 with the reason in
 * words, and the words belong next to the control that asked, so the scope is
 * what pins a refusal and what keeps two tools saving at once from freezing
 * each other: a shared `isPending` would hold down every select on an
 * eighty-eight-tool server while one of them saved.
 */
export function useBindingWrites(serverName: string | null): BindingWrites {
  const setBinding = useSetMcpServerBinding();
  const [pending, setPending] = useState<ReadonlySet<string>>(new Set());
  const [failure, setFailure] = useState<{ scope: string; message: string } | null>(null);
  const write = async (scope: WriteScope, body: McpServerBindingPatch) => {
    if (!serverName) return;
    const key = scopeKey(scope);
    setFailure(null);
    setPending((prev) => new Set(prev).add(key));
    try {
      await setBinding.mutateAsync({ name: serverName, body });
    } catch (err) {
      setFailure({ scope: key, message: formatApiErrorDetail(err) });
    } finally {
      setPending((prev) => {
        const next = new Set(prev);
        next.delete(key);
        return next;
      });
    }
  };
  return {
    write,
    busy: (scope) => pending.has(scopeKey(scope)),
    anyBusy: pending.size > 0,
    refusalOn: (scope) => (failure?.scope === scopeKey(scope) ? failure.message : null),
  };
}
