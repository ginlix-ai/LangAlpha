import { useMemo, type ComponentProps } from 'react';
import { artifactPathKey, useArtifactComments, useOwnedArtifact } from '@/hooks/useArtifact';
import { useWorkspaceFolders } from '../../../../contexts/WorkspaceContext';
import PdfRail from '../../pdf/PdfRail';
import { inFilter } from '../../../artifactComments/commentModel';
import type { ViewerCommentsTarget } from '../ViewerComments';

const NO_COUNTS: ReadonlyMap<number, number> = new Map();

/**
 * How many comments each page has that the list's Open shows (an unread
 * answer that resolved one included), by its 1-based number. The same
 * queries the comment layer reads, so the counts move with its pins.
 */
function usePageCommentCounts({ workspaceId, path }: ViewerCommentsTarget): ReadonlyMap<number, number> {
  const root = useWorkspaceFolders()?.rootDir;
  const { record } = useOwnedArtifact(workspaceId, artifactPathKey(path, root));
  const { data } = useArtifactComments(record?.artifact_id ?? null);
  return useMemo(() => {
    if (!data?.length) return NO_COUNTS;
    const counts = new Map<number, number>();
    for (const c of data) {
      const page = c.anchor.kind === 'text' ? c.anchor.page : undefined;
      if (page && inFilter(c, 'open')) counts.set(page, (counts.get(page) ?? 0) + 1);
    }
    return counts;
  }, [data]);
}

/** The page strip of a PDF the owner comments on, each page with its count of open comments. */
export function CountedRail({ target, ...rail }: Omit<ComponentProps<typeof PdfRail>, 'comments'> & { target: ViewerCommentsTarget }) {
  return <PdfRail {...rail} comments={usePageCommentCounts(target)} />;
}
