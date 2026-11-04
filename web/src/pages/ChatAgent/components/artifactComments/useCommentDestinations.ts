import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useArtifactByPath, useArtifactDestinations } from '@/hooks/useArtifact';
import { useArtifactCommentSender } from './sendContext';
import { commentDestinations, type CommentDestinations } from './destinations';

/**
 * Where the comments on the file at `docPath` (workspace-relative, as the
 * file routes read it) go, beside the chat this view sits next to, if any.
 * Every control that sends a comment reads it here (`commentDestinations`).
 */
export function useCommentDestinations(
  workspaceId: string | null | undefined,
  docPath: string | null | undefined,
): CommentDestinations {
  const { t } = useTranslation();
  const { besideThreadId } = useArtifactCommentSender();
  const { data: record } = useArtifactByPath(workspaceId, docPath);
  const { data } = useArtifactDestinations(record?.artifact_id ?? null, besideThreadId);
  return useMemo(() => commentDestinations(t, data, besideThreadId), [t, data, besideThreadId]);
}
