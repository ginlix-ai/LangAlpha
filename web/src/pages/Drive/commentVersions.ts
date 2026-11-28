import type { ArtifactComment } from '@/types/artifacts';
import type { DriveVersion } from '@/types/drive';

type Dated = Pick<DriveVersion, 'seq' | 'sha256' | 'created_at'>;
type Made = Pick<ArtifactComment, 'file_sha256' | 'created_at'>;

/** When a version stopped being the current one: the next version's time, or never. */
function spans(versions: readonly Dated[]): { version: Dated; from: number; until: number }[] {
  const bySeq = [...versions].sort((a, b) => a.seq - b.seq);
  return bySeq.map((version, i) => ({
    version,
    from: Date.parse(version.created_at),
    until: i + 1 < bySeq.length ? Date.parse(bySeq[i + 1].created_at) : Number.POSITIVE_INFINITY,
  }));
}

/**
 * The version of a file a comment was made on, among the versions still kept;
 * null where it cannot be told or that version is gone.
 *
 * By the hash of the bytes the comment was made on, which is exact: a comment
 * made on the last head while a new one loaded carries the last one's. Where
 * two versions share the bytes (one restored from the other), the one current
 * when the comment was made; failing that, the latest made before it. A hash
 * no kept version has is a version cleared or expired since, so it is not
 * guessed by time. Only a comment with no hash (made where the surface had
 * none to give) is placed by time: the version current when it was made,
 * unless one saved after it and before the next kept is gone, which may be
 * the one the comment was made on.
 */
export function versionOfComment(comment: Made, versions: readonly Dated[]): number | null {
  const made = Date.parse(comment.created_at);
  const all = spans(versions);
  if (comment.file_sha256) {
    const same = all.filter((s) => s.version.sha256 === comment.file_sha256);
    if (same.length === 0) return null;
    if (Number.isNaN(made)) return same[0].version.seq;
    const current = same.find((s) => s.from <= made && made < s.until);
    if (current) return current.version.seq;
    const before = same.filter((s) => s.from <= made).at(-1);
    return (before ?? same[0]).version.seq;
  }
  if (Number.isNaN(made)) return null;
  const at = all.findIndex((s) => s.from <= made && made < s.until);
  if (at < 0) return null;
  const { seq } = all[at].version;
  const next = all[at + 1]?.version.seq;
  return next !== undefined && next !== seq + 1 ? null : seq;
}
