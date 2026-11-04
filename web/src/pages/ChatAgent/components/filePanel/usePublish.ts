/**
 * Publishing a file's record, with the answers that are not a new version
 * read the one way the Publish dialog and the crumb's Update both need.
 */
import { toast } from '@/components/ui/use-toast';
import { apiErrorCoded, apiErrorObject, apiErrorStatus } from '@/api/errors';
import { useArtifactPublishing } from '@/hooks/useArtifact';
import { pageLoadsOf } from '@/lib/pageLoads';
import type { Artifact, ArtifactVersion, PublishArtifactRequest, ShareFileEntry } from '@/types/api';

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

/**
 * Why the file cannot be published as it is: over a cap, gone, unreadable for
 * now, or using files only the running computer holds.
 */
export type PublishRefusal =
  | { kind: 'cap'; cap: ShareCap }
  | { kind: 'missing' }
  | { kind: 'unavailable' }
  | { kind: 'needsComputer' };

export function refusalIn(err: unknown): PublishRefusal | null {
  const cap = shareCapIn(err);
  if (cap) return { kind: 'cap', cap };
  if (apiErrorCoded(err, 'file_missing')) return { kind: 'missing' };
  if (apiErrorCoded(err, 'needs_computer')) return { kind: 'needsComputer' };
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

/** What a caller decides about a publish; the page's report goes with it on its own. */
export type PublishBody = Omit<PublishArtifactRequest, 'loaded' | 'earlier'>;

/** `failed` is the toast for an answer the owner can do nothing with but try again. */
export function usePublish(failed: string) {
  const { publish } = useArtifactPublishing();
  const send = async (artifact: Artifact, body: PublishBody): Promise<PublishResult> => {
    // The server reads the list again with this report to compare it, so it
    // must be the one `useArtifactStatus` read the reviewed list with.
    const loads = pageLoadsOf(artifact.workspace_id, artifact.path);
    try {
      const { version, cut } = await publish.mutateAsync({
        artifactId: artifact.artifact_id,
        body: { ...body, loaded: loads.now, earlier: loads.earlier },
      });
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
