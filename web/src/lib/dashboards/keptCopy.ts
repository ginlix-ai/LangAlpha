import type { QueryClient } from '@tanstack/react-query';
import { putArtifact } from '@/hooks/useArtifact';
import type { PageLoads } from '@/lib/pageLoads';
import { setArtifactKept } from '@/pages/ChatAgent/utils/api/artifacts';
import type { Artifact } from '@/types/artifacts';
import { isPageListed, readCachedDashboards } from './saved';

/**
 * The copy the server keeps of a page set as a dashboard. A removed page
 * stays kept while its Undo is on screen, and is let go when the undo window
 * ends or the tab is hidden for good (`pagehide`). A tab closed so that
 * neither runs, or whose request does not outlive the page, leaves the page
 * kept: harmless, it only goes on cutting versions. Held at module level
 * because the toast outlives the component that removed the page.
 */

/** How long a removal can be undone; a removed page's copy is let go after it. */
export const UNDO_MS = 5_000;

/** Each artifact's pending release, by the function that calls it off. */
const pending = new Map<string, () => void>();

/** Keep a page or stop keeping it, with the cached record following the answer.
 *  A keep from the open page passes what it reported loading. */
export async function setKept(qc: QueryClient, artifactId: string, kept: boolean, loads?: PageLoads): Promise<Artifact> {
  const record = await setArtifactKept(artifactId, kept, loads);
  putArtifact(qc, record);
  return record;
}

/** Let go unless it is back on the list (another tab, a pin since). */
function letGo(qc: QueryClient, artifactId: string): void {
  if (!isPageListed(readCachedDashboards(qc), artifactId)) void setKept(qc, artifactId, false).catch(() => {});
}

/** Stop keeping a page now, calling off any release still waiting on it. */
export function releaseCopy(qc: QueryClient, artifactId: string): void {
  pending.get(artifactId)?.();
  letGo(qc, artifactId);
}

/**
 * Let a removed page's copy go once its undo window ends. `reclaim` is the
 * Undo: it calls the release off, or keeps the page again if it already ran.
 * A later hold on the same page replaces this one.
 */
export function holdRelease(qc: QueryClient, artifactId: string): { reclaim: () => void } {
  pending.get(artifactId)?.();
  let ran = false;
  function stop() {
    clearTimeout(timer);
    window.removeEventListener('pagehide', commit);
    if (pending.get(artifactId) === stop) pending.delete(artifactId);
  }
  function commit() {
    ran = true;
    stop();
    letGo(qc, artifactId);
  }
  const timer = setTimeout(commit, UNDO_MS);
  window.addEventListener('pagehide', commit);
  pending.set(artifactId, stop);
  return {
    reclaim: () => {
      if (ran) void setKept(qc, artifactId, true).catch(() => {});
      else stop();
    },
  };
}

/** A page pinned again inside its undo window keeps its copy. */
export function keepCopy(artifactId: string): void {
  pending.get(artifactId)?.();
}
