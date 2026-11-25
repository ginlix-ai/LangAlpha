import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useArtifactDestinations, useArtifactRecord } from '@/hooks/useArtifact';
import type { ArtifactTarget } from '@/types/api';
import { useArtifactCommentSender } from './sendContext';
import { commentDestinations, threadOf, type CommentDestinations } from './destinations';

/**
 * Where the comments on `target` (as `targetFor` keys it) go, beside the chat
 * this view sits next to, if any. Every control that sends a comment reads it
 * here (`commentDestinations`).
 */
export function useCommentDestinations(
  workspaceId: string | null | undefined,
  target: ArtifactTarget | null,
): CommentDestinations {
  const { t } = useTranslation();
  const { beside } = useArtifactCommentSender();
  const { data: record } = useArtifactRecord(workspaceId, target);
  const { data } = useArtifactDestinations(record?.artifact_id ?? null, threadOf(beside));
  return useMemo(() => commentDestinations(t, data, beside), [t, data, beside]);
}
