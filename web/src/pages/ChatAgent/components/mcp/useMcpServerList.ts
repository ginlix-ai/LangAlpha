import { useCallback, useEffect, useRef, useState } from 'react';
import { useBfcacheRestore } from '@/hooks/useBfcacheRestore';
import {
  connectRequestFor,
  useMcpOauthActions,
  type ConnectRequest,
} from '@/pages/Plugins/hooks/useMcpOauthActions';
import {
  mcpSignInNeeded,
  type McpServerDraft,
  type McpServerInput,
  type PendingSignIn,
} from '../../utils/api';
import { refusalOf, type Refusal } from '../../utils/api/refusal';

/**
 * The list controller both MCP surfaces run on: the user-level Plugins page
 * and the workspace settings MCP tab. The add/edit form, the import, the
 * switch, delete and the sign-in a save can turn out to need are written once
 * here; each surface keeps only what is genuinely its own (where a sign-in's
 * save lands, connect/disconnect and delete on one side, discovery on the
 * other).
 *
 * Outcome reporting stays with the caller — each surface owns its copy, and an
 * omitted reporter means that outcome is deliberately silent (the workspace
 * toggle is optimistic with rollback, so it says nothing on failure).
 */

/** What the form says about the server it opened on, until it changes. */
export interface FormSaid {
  /** Plain, beside the form: a sign-in the user cancelled at the vendor. */
  note?: string;
  /** Why this server was not saved: the form's own last save, an import
   *  entry's reason, or a sign-in that came back failed. */
  refusal?: Refusal;
}

/** Anything the controller acts on: a catalog row or an effective row. */
type ListServer = McpServerDraft & Parameters<typeof connectRequestFor>[0];

/** What a save answers with, as far as the list reads it. */
interface Saved {
  name?: string;
  warnings?: string[] | null;
}

export interface McpServerListOptions<TServer extends ListServer> {
  create: (body: McpServerInput) => Promise<Saved>;
  update: (vars: { name: string; body: McpServerInput }) => Promise<Saved>;
  toggle: (vars: { name: string; enabled: boolean }) => Promise<Saved>;
  /** Absent where a row cannot be deleted (the workspace tab: removal is an
   *  account action, taken on the Plugins page). */
  remove?: (name: string) => Promise<unknown>;
  onSaveWarnings?: (warnings: string[]) => void;
  /** An add went through and the form closed. `toolCount` is what the form's
   *  own check listed for exactly what was saved, or null without one. */
  onCreated?: (created: { name: string; toolCount: number | null }) => void;
  onToggleWarnings?: (warnings: string[]) => void;
  /** A refused switch. One refused for want of a sign-in is offered that
   *  sign-in instead, and never arrives here. */
  onToggleError?: (err: unknown, server: TServer) => void;
  onDeleteError?: (err: unknown) => void;
  /**
   * The save a sign-in carries, for a server that is saved only by its
   * sign-in: where it lands is the surface's (an add on in one workspace, an
   * edit that keeps its switch). Absent, that answer is a refusal like any
   * other.
   */
  pendingFor?: (body: McpServerInput, editing: TServer | null) => PendingSignIn;
  /** The connect a row's own Connect starts, which a switch refused for want
   *  of it offers. Defaults to the row as it stands. */
  connectFor?: (server: TServer) => ConnectRequest;
  /**
   * Deletes go through a confirm strip: `requestDelete` only arms it, and a
   * failed `confirmDelete` leaves it armed to retry. Without this a
   * `requestDelete` deletes straight away — the row's own menu item was the
   * confirmation.
   */
  confirmBeforeDelete?: boolean;
}

/** What the form opens on. One value, so nothing it said outlives it. */
interface Opening<TServer> {
  /** The row being edited, as saved, or null for an add. */
  editing: TServer | null;
  /** For an add, a server already written out to start from; for an edit,
   *  the changes a sign-in carried, laid over the row. */
  server?: McpServerInput;
  said?: FormSaid;
}

/**
 * An edited row with the changes a sign-in carried laid over it. Headers stay
 * the row's: a typed value is not kept between sign-ins, and the saved one is
 * the better start.
 */
function withEdits<T extends ListServer>(row: T, edits: McpServerInput | undefined): T {
  if (!edits) return row;
  const { url, description, instruction, tool_exposure_mode } = edits;
  return {
    ...row,
    ...(url !== undefined && { url }),
    ...(description !== undefined && { description }),
    ...(instruction !== undefined && { instruction }),
    ...(tool_exposure_mode !== undefined && { tool_exposure_mode }),
  };
}

export function useMcpServerList<TServer extends ListServer>(
  options: McpServerListOptions<TServer>,
) {
  const oauth = useMcpOauthActions();
  const [opening, setOpening] = useState<Opening<TServer> | null>(null);
  const [importOpen, setImportOpen] = useState(false);
  // A sign-in is starting for the form's server; the form holds as if saving
  // until the page leaves for the vendor, the start comes back refused, or the
  // sign-in goes on in another window (the desktop app) and the form closes.
  const [signingIn, setSigningIn] = useState(false);
  const [togglingName, setTogglingName] = useState<string | null>(null);
  const [deletingName, setDeletingName] = useState<string | null>(null);

  // The reporters are inline closures at the call sites, so reading them
  // through a ref is what lets every handler below be referentially stable —
  // which is what `React.memo(McpServerRow)` needs to skip a re-render while
  // the workspace list polls.
  const latest = useRef(options);
  const oauthRef = useRef(oauth);
  useEffect(() => {
    latest.current = options;
    oauthRef.current = oauth;
  });

  // Back from the vendor's page as a bfcache restore: the form is as it was
  // left, and the sign-in it was waiting on is over either way.
  useBfcacheRestore(() => setSigningIn(false));

  const open = useCallback((next: Opening<TServer>) => {
    setSigningIn(false);
    setOpening(next);
  }, []);

  const openAdd = useCallback(() => open({ editing: null }), [open]);

  /** The add form, filled from a server written out elsewhere: an import entry
   *  to fix, or a save to try again. Separate from `openAdd`, which is wired
   *  straight to click handlers and would take the click event as its server. */
  const openAddWith = useCallback(
    (server: McpServerInput, said?: FormSaid) => open({ editing: null, server, said }),
    [open],
  );

  const openEdit = useCallback(
    (row: TServer, carried?: { edits?: McpServerInput; said?: FormSaid }) =>
      open({ editing: row, server: carried?.edits, said: carried?.said }),
    [open],
  );

  const closeModal = useCallback(() => {
    setOpening(null);
    setSigningIn(false);
  }, []);

  /** A refused save, or a refused start of the sign-in that would have saved
   *  it, said in the form that asked, beside whatever it opened saying. */
  const refuse = useCallback(
    (err: unknown) => setOpening((o) => o && { ...o, said: { ...o.said, refusal: refusalOf(err) } }),
    [],
  );

  /** Start the sign-in that saves `body`, from the form or an import entry. */
  const signInToSave = useCallback(
    (
      body: McpServerInput,
      editing: TServer | null,
      outcomes?: Pick<ConnectRequest, 'onNotStarted' | 'onRefused' | 'onHandedOff'>,
    ) => {
      const pendingFor = latest.current.pendingFor;
      if (!pendingFor) return;
      oauthRef.current.connect({
        ...connectRequestFor(body),
        ...outcomes,
        name: editing?.name ?? body.name,
        pending: pendingFor(body, editing),
      });
    },
    [],
  );

  const editing = opening?.editing ?? null;

  const submit = useCallback(
    async (body: McpServerInput, facts?: { toolCount: number | null }) => {
      setOpening((o) => o && { ...o, said: o.said && { note: o.said.note } });
      try {
        const saved = editing
          ? await latest.current.update({ name: editing.name, body })
          : await latest.current.create(body);
        closeModal();
        if (!editing) {
          // The row the list should point at, under the name the server kept.
          latest.current.onCreated?.({
            name: saved.name ?? body.name,
            toolCount: facts?.toolCount ?? null,
          });
        }
        if (saved.warnings?.length) latest.current.onSaveWarnings?.(saved.warnings);
      } catch (err) {
        if (latest.current.pendingFor && mcpSignInNeeded(err)) {
          // Not a refusal: the same server saves once the user has signed in,
          // and the sign-in is what saves it.
          setSigningIn(true);
          signInToSave(body, editing, {
            onNotStarted: () => setSigningIn(false),
            onRefused: refuse,
            // The sign-in carries the body now, and its callback saves it;
            // the form has nothing left to do but hold the user, and the
            // save can take as long as the vendor's page stays open.
            onHandedOff: closeModal,
          });
          return;
        }
        // Kept inline in the still-open modal so the user can fix and retry.
        refuse(err);
      }
    },
    [editing, refuse, closeModal, signInToSave],
  );

  const toggle = useCallback(
    async (server: TServer, enabled: boolean) => {
      setTogglingName(server.name);
      try {
        const result = await latest.current.toggle({ name: server.name, enabled });
        if (result.warnings?.length) latest.current.onToggleWarnings?.(result.warnings);
      } catch (err) {
        const request = latest.current.connectFor?.(server) ?? connectRequestFor(server);
        if (!oauthRef.current.offerSignInIfRefused(err, request)) {
          latest.current.onToggleError?.(err, server);
        }
      } finally {
        setTogglingName(null);
      }
    },
    [],
  );

  const runDelete = useCallback(async (name: string) => {
    const remove = latest.current.remove;
    if (!remove) return;
    setDeletingName(name);
    try {
      await remove(name);
      setDeletingName(null);
    } catch (err) {
      latest.current.onDeleteError?.(err);
      if (!latest.current.confirmBeforeDelete) setDeletingName(null);
    }
  }, []);

  const requestDelete = useCallback(
    (server: { name: string }) => {
      if (latest.current.confirmBeforeDelete) setDeletingName(server.name);
      else void runDelete(server.name);
    },
    [runDelete],
  );

  const cancelDelete = useCallback(() => setDeletingName(null), []);

  const confirmDelete = useCallback(() => {
    if (deletingName) void runDelete(deletingName);
  }, [deletingName, runDelete]);

  const openImport = useCallback(() => setImportOpen(true), []);
  const closeImport = useCallback(() => setImportOpen(false), []);

  return {
    oauth,
    editing,
    togglingName,
    deletingName,
    openAdd,
    openAddWith,
    openEdit,
    openImport,
    toggle,
    requestDelete,
    cancelDelete,
    confirmDelete,
    /** The add/edit form's props while it is open, for `McpServerModal`. */
    modal: opening && {
      initial: opening.editing && withEdits(opening.editing, opening.server),
      prefill: opening.editing ? null : (opening.server ?? null),
      said: opening.said ?? null,
      onClose: closeModal,
      onSubmit: submit,
      signingIn,
    },
    /** The import's props while it is open, for `McpImportModal`. */
    importer: importOpen
      ? {
          onClose: closeImport,
          onSignIn: options.pendingFor
            ? (server: McpServerInput) => signInToSave(server, null)
            : undefined,
          // This dialog closes on Fix, and it was the only place that said
          // why the entry was not added, so the form goes on saying it.
          onFix: (server: McpServerInput, refusal: Refusal | null) => {
            closeImport();
            openAddWith(server, refusal ? { refusal } : undefined);
          },
          signingIn: oauth.connectingName,
        }
      : null,
  };
}
