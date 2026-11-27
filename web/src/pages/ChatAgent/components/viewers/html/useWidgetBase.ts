import { useEffect, useState } from 'react';
import { useWorkspaceFileGrant } from '@/hooks/useWorkspaceFileGrant';
import { useWorkspaceId } from '../../../contexts/WorkspaceContext';
import { serveBase } from './wsfilesUrl';

/** How long a widget waits for a grant before it renders without one. */
const GRANT_WAIT_MS = 2000;

/**
 * The `<base>` a widget renders under: its workspace folder through the
 * viewer's file grant, `null` for none (no workspace, as on a public share, or
 * no grant in time), `undefined` while still waiting.
 *
 * Held once decided. A grant renews well before it expires, and a new prefix
 * would rebuild the srcDoc, which reloads the widget and loses whatever the
 * user did in it; the first prefix stays valid for its own lifetime.
 */
export function useWidgetBase(): string | null | undefined {
  const workspaceId = useWorkspaceId();
  const grant = useWorkspaceFileGrant(workspaceId);
  const [waited, setWaited] = useState(false);
  useEffect(() => {
    if (!workspaceId) return;
    const timer = setTimeout(() => setWaited(true), GRANT_WAIT_MS);
    return () => clearTimeout(timer);
  }, [workspaceId]);

  const prefix = grant.data?.prefix;
  let candidate: string | null | undefined;
  if (!workspaceId) candidate = null;
  else if (prefix) candidate = serveBase(prefix);
  else if (grant.isError || waited) candidate = null;

  const [held, setHeld] = useState(candidate);
  if (held === undefined && candidate !== undefined) setHeld(candidate);
  return held !== undefined ? held : candidate;
}
