import { useMemo, type ComponentProps } from 'react';
import { artifactPathKey, useArtifactComments, useOwnedArtifact } from '@/hooks/useArtifact';
import { useWorkspaceFolders } from '../../../../contexts/WorkspaceContext';
import PdfRail from '../../pdf/PdfRail';
import { inFilter } from '../../../artifactComments/commentModel';
import type { ArtifactCommentAnchor } from '@/types/artifacts';
import type { ViewerCommentsTarget } from '../ViewerComments';

const NO_COUNTS: ReadonlyMap<number, number> = new Map();

/** The pages a comment is on, each once: selected words are on one, boxes may be on several. */
function pagesOf(anchor: ArtifactCommentAnchor): Set<number> {
  if (anchor.kind === 'text') return new Set(anchor.page ? [anchor.page] : []);
  if (anchor.kind === 'region') return new Set(anchor.boxes.flatMap((b) => (b.page ? [b.page] : [])));
  return new Set();
}

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
      if (!inFilter(c, 'open')) continue;
      for (const page of pagesOf(c.anchor)) counts.set(page, (counts.get(page) ?? 0) + 1);
    }
    return counts;
  }, [data]);
}

/** The page strip of a PDF the owner comments on, each page with its count of open comments. */
export function CountedRail({ target, ...rail }: Omit<ComponentProps<typeof PdfRail>, 'comments'> & { target: ViewerCommentsTarget }) {
  return <PdfRail {...rail} comments={usePageCommentCounts(target)} />;
}
