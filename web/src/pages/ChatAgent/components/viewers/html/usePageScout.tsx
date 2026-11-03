import type { ReactNode } from 'react';
import { useState } from 'react';
import { isHtmlFile } from '../../../utils/filePaths';
import { PageScout } from './PageScout';

/**
 * The page, loaded out of sight, so a file its code asks for as it loads is on
 * the review's list as the page is now: a frame the panel shows may hold code
 * the agent has changed since it loaded. `holding` says the list is not whole
 * yet, and `failed` that the page could not run, until `retry`; `reads` says
 * whether the list may be read now; `element` belongs
 * anywhere in the review's tree. `follow` takes the page's code as each list
 * was read (`page_sha256`), in render.
 *
 * The first read goes before the scout, so the scout never runs code older
 * than the code that read names; a later read naming other code means the
 * agent changed the page meanwhile, and the scout loads it again and holds
 * the review until it reports.
 */
export function usePageScout(workspaceId: string, path: string | null): {
  holding: boolean;
  failed: boolean;
  retry: () => void;
  reads: boolean;
  element: ReactNode;
  follow: (code: string | null | undefined) => void;
} {
  const page = path !== null && isHtmlFile(path);
  const [released, setReleased] = useState(false);
  const [ran, setRan] = useState<{ code: string | null; run: number }>({ code: null, run: 0 });
  const [failed, setFailed] = useState(false);
  const run = (code: string | null) => {
    setRan({ code, run: ran.run + 1 });
    setReleased(false);
    setFailed(false);
  };
  const follow = (code: string | null | undefined) => {
    if (!page || !code || code === ran.code) return;
    run(code);
  };
  return {
    // A page that never ran holds the review: published from its source alone,
    // a file its code builds would be missing from the version.
    holding: page && (!released || failed),
    failed: page && failed,
    retry: () => run(ran.code),
    // Not while it runs: each report it makes would read the list again.
    reads: !page || released || ran.code === null,
    element: page && ran.code !== null && (
      <PageScout
        key={ran.run}
        workspaceId={workspaceId}
        path={path}
        onSettled={() => setReleased(true)}
        onFailed={() => setFailed(true)}
      />
    ),
    follow,
  };
}
