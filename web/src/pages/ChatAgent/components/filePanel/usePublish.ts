/**
 * Publishing a file's record, with the answers that are not a new version
 * read the one way the Publish dialog and the crumb's Update both need.
 */
import { toast } from '@/components/ui/use-toast';
import { apiErrorCoded, apiErrorObject, apiErrorStatus } from '@/api/errors';
import { useArtifactPublishing } from '@/hooks/useArtifact';
import type { ArtifactVersion, PublishArtifactRequest, ShareFileEntry } from '@/types/api';

/**
 * The list a publish found in place of the one the owner reviewed (409
 * `files_changed`), or null for any other error.
 */
export function filesChangedIn(err: unknown): ShareFileEntry[] | null {
  const detail = apiErrorStatus(err) === 409 ? apiErrorCoded(err, 'files_changed') : null;
  if (!detail) return null;
  return Array.isArray(detail.files) ? (detail.files as ShareFileEntry[]) : [];
}

/** Which cap a file's list went over: a count of files, or their total bytes. */
export interface ShareCap {
  code: 'too_many_files' | 'too_many_bytes';
  limit: number;
}

/** The cap a 422 names, or null for any other error. */
export function shareCapIn(err: unknown): ShareCap | null {
  if (apiErrorStatus(err) !== 422) return null;
  const { code, limit } = apiErrorObject(err) ?? {};
  if (code !== 'too_many_files' && code !== 'too_many_bytes') return null;
  return { code, limit: typeof limit === 'number' ? limit : 0 };
}

/** Why the file cannot be published as it is: over a cap, gone, or unreadable for now. */
export type PublishRefusal = { kind: 'cap'; cap: ShareCap } | { kind: 'missing' } | { kind: 'unavailable' };

export function refusalIn(err: unknown): PublishRefusal | null {
  const cap = shareCapIn(err);
  if (cap) return { kind: 'cap', cap };
  if (apiErrorCoded(err, 'file_missing')) return { kind: 'missing' };
  if (apiErrorStatus(err) === 503) return { kind: 'unavailable' };
  return null;
}

/**
 * What a publish came to. A changed list and a refusal are for the owner to
 * review; any other failure is already toasted.
 */
export type PublishResult =
  | { kind: 'published'; version: ArtifactVersion; cut: boolean }
  | { kind: 'filesChanged'; files: ShareFileEntry[] }
  | { kind: 'refused'; refusal: PublishRefusal }
  | { kind: 'failed' };

/** `failed` is the toast for an answer the owner can do nothing with but try again. */
export function usePublish(failed: string) {
  const { publish } = useArtifactPublishing();
  const send = async (artifactId: string, body: PublishArtifactRequest): Promise<PublishResult> => {
    try {
      const { version, cut } = await publish.mutateAsync({ artifactId, body });
      return { kind: 'published', version, cut };
    } catch (err) {
      const files = filesChangedIn(err);
      if (files) return { kind: 'filesChanged', files };
      const refusal = refusalIn(err);
      if (refusal) return { kind: 'refused', refusal };
      console.error('[publish] Request failed:', err);
      toast({ description: failed, variant: 'destructive' });
      return { kind: 'failed' };
    }
  };
  return { send, pending: publish.isPending };
}
