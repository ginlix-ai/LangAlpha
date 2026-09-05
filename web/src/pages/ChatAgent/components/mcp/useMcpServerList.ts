import { useCallback, useEffect, useRef, useState } from 'react';
import { formatApiErrorDetail, type McpServerInput } from '../../utils/api';

/**
 * The list controller both MCP surfaces run on: the user-level Plugins page
 * and the workspace settings MCP tab. Add/edit modal, import modal, in-flight
 * toggle and delete were written once per surface and had already drifted
 * apart; each surface now keeps only what is genuinely its own (OAuth
 * connect/disconnect/refresh on one side, discovery + promote on the other).
 *
 * Outcome reporting stays with the caller — each surface owns its copy, and an
 * omitted reporter means that outcome is deliberately silent (the workspace
 * toggle is optimistic with rollback, so it says nothing on failure).
 */

/** Non-blocking policy nudges, from whatever shape the mutation answered with. */
function warningsOf(result: unknown): string[] {
  return (result as { warnings?: string[] | null } | null | undefined)?.warnings ?? [];
}

/** Anything the controller acts on by name — a catalog row or an effective row. */
interface NamedServer {
  name: string;
}

export interface McpServerListOptions {
  create: (body: McpServerInput) => Promise<unknown>;
  update: (vars: { name: string; body: McpServerInput }) => Promise<unknown>;
  toggle: (vars: { name: string; enabled: boolean }) => Promise<unknown>;
  remove: (name: string) => Promise<unknown>;
  onSaveWarnings?: (warnings: string[]) => void;
  onToggleWarnings?: (warnings: string[]) => void;
  onToggleError?: (err: unknown) => void;
  onDeleteError?: (err: unknown) => void;
  /**
   * Deletes go through a confirm strip: `requestDelete` only arms it, and a
   * failed `confirmDelete` leaves it armed to retry. Without this a
   * `requestDelete` deletes straight away — the row's own menu item was the
   * confirmation.
   */
  confirmBeforeDelete?: boolean;
}

export function useMcpServerList<TServer extends NamedServer>(options: McpServerListOptions) {
  const [modalOpen, setModalOpen] = useState(false);
  const [importOpen, setImportOpen] = useState(false);
  const [editing, setEditing] = useState<TServer | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);

  // The reporters are inline closures at the call sites, so reading them
  // through a ref is what lets every handler below be referentially stable —
  // which is what `React.memo(McpServerRow)` needs to skip a re-render while
  // the workspace list polls.
  const latest = useRef(options);
  useEffect(() => {
    latest.current = options;
  });

  const openAdd = useCallback(() => {
    setEditing(null);
    setSubmitError(null);
    setModalOpen(true);
  }, []);

  const openEdit = useCallback((server: TServer) => {
    setEditing(server);
    setSubmitError(null);
    setModalOpen(true);
  }, []);

  const closeModal = useCallback(() => {
    setModalOpen(false);
    setEditing(null);
  }, []);

  const openImport = useCallback(() => setImportOpen(true), []);
  const closeImport = useCallback(() => setImportOpen(false), []);

  const submit = useCallback(
    async (body: McpServerInput) => {
      setSubmitError(null);
      try {
        const saved = editing
          ? await latest.current.update({ name: editing.name, body })
          : await latest.current.create(body);
        setModalOpen(false);
        setEditing(null);
        const warnings = warningsOf(saved);
        if (warnings.length > 0) latest.current.onSaveWarnings?.(warnings);
      } catch (err) {
        // Kept inline in the still-open modal so the user can fix and retry.
        setSubmitError(formatApiErrorDetail(err));
      }
    },
    [editing],
  );

  const toggle = useCallback(async (server: NamedServer, enabled: boolean) => {
    setTogglingName(server.name);
    try {
      const result = await latest.current.toggle({ name: server.name, enabled });
      const warnings = warningsOf(result);
      if (warnings.length > 0) latest.current.onToggleWarnings?.(warnings);
    } catch (err) {
      latest.current.onToggleError?.(err);
    } finally {
      setTogglingName(null);
    }
  }, []);

  const runDelete = useCallback(async (name: string) => {
    setDeletingName(name);
    try {
      await latest.current.remove(name);
      setDeletingName(null);
    } catch (err) {
      latest.current.onDeleteError?.(err);
      if (!latest.current.confirmBeforeDelete) setDeletingName(null);
    }
  }, []);

  const requestDelete = useCallback(
    (server: NamedServer) => {
      if (latest.current.confirmBeforeDelete) setDeletingName(server.name);
      else void runDelete(server.name);
    },
    [runDelete],
  );

  const cancelDelete = useCallback(() => setDeletingName(null), []);

  const confirmDelete = useCallback(() => {
    if (deletingName) void runDelete(deletingName);
  }, [deletingName, runDelete]);

  return {
    modalOpen,
    importOpen,
    editing,
    submitError,
    togglingName,
    deletingName,
    openAdd,
    openEdit,
    closeModal,
    openImport,
    closeImport,
    submit,
    toggle,
    requestDelete,
    cancelDelete,
    confirmDelete,
  };
}
