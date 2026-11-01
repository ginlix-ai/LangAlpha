import { useEffect, useId, useRef, useState, type RefObject } from 'react';
import { Check, Copy, ExternalLink, Link2, Lock } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Loader } from '@/components/ui/loader';
import { ToggleSwitch } from '@/components/ui/switch';
import { toast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';
import { formatBytes, shortMonthDay } from '@/lib/format';
import { useCopyShareLink } from '@/hooks/useCopyShareLink';
import { useLocale } from '@/hooks/useLocale';
import {
  useArtifactPublishing,
  useArtifactStatus,
  useArtifactVersions,
  useOwnedArtifact,
} from '@/hooks/useArtifact';
import type { ArtifactStatus, ArtifactVersion, PublishArtifactRequest, ShareFileEntry } from '@/types/api';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';
import { basename, dirname } from '../utils/fileRefResolver';
import { shareLinkHref } from '../utils/api/shareLinks';
import { refusalIn, usePublish, type PublishRefusal } from './filePanel/usePublish';

/** The server takes a label of 1 to 200 characters. */
const LABEL_MAX = 200;

interface PublishDialogProps {
  open: boolean;
  workspaceId: string;
  /** Any spelling the panel holds; the record's `path` is canonical. */
  path: string;
  onClose: () => void;
}

/** The last attempt's answer when it was not a new version, standing until the next attempt or close. */
type Outcome =
  /** A 409's list, shown in place of the status until the status is read again after the attempt. */
  | { kind: 'conflict'; files: ShareFileEntry[]; statusAt: number }
  | { kind: 'refused'; refusal: PublishRefusal }
  /** Published, but the files were as the latest version left them. */
  | { kind: 'unchanged'; seq: number };

function FileRow({ entry, added, t }: {
  entry: ShareFileEntry;
  /** Read by the file now but in no version yet: what an Update would add. */
  added: boolean;
  t: (key: string) => string;
}) {
  const locale = useLocale();
  const dir = dirname(entry.path);
  return (
    <li className="flex items-center gap-3 px-3 py-1.5 text-xs" data-selectable>
      <span className="flex-1 min-w-0 truncate" title={entry.path}>
        {dir && <span style={{ color: 'var(--color-text-tertiary)' }}>{dir}/</span>}
        <span style={{ color: 'var(--color-text-primary)' }}>{basename(entry.path)}</span>
      </span>
      <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
        {formatBytes(entry.size, locale)}
      </span>
      <span
        className="shrink-0 w-20 text-right"
        style={{ color: added ? 'var(--color-warning)' : 'var(--color-text-quaternary)' }}
      >
        {added ? t('publish.notShared') : t(`shareLink.reason.${entry.reason}`)}
      </span>
    </li>
  );
}

function VersionRow({ version }: { version: ArtifactVersion }) {
  const { t } = useTranslation();
  const locale = useLocale();
  return (
    <li className="flex items-center gap-3 px-3 py-1.5 text-xs">
      <span className="shrink-0 tabular-nums font-medium" style={{ color: 'var(--color-text-primary)' }}>
        {t('publish.version', { seq: version.seq })}
      </span>
      <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
        {shortMonthDay(new Date(version.published_at), locale)}
      </span>
      <span
        className="flex-1 min-w-0 truncate"
        title={version.label ?? undefined}
        style={{ color: 'var(--color-text-secondary)' }}
        data-selectable
      >
        {version.label}
      </span>
      {version.origin === 'keep_updated' && (
        <span className="shrink-0" style={{ color: 'var(--color-text-quaternary)' }}>
          {t('publish.keptUpdated')}
        </span>
      )}
    </li>
  );
}

/** Versions, newest first. */
function VersionList({ artifactId }: { artifactId: string | undefined }) {
  const { t } = useTranslation();
  const headingId = useId();
  const versionsQuery = useArtifactVersions(artifactId);
  return (
    <div className="flex flex-col gap-1.5">
      <span id={headingId} className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {t('publish.versions')}
      </span>
      <div
        className={cn('rounded-lg border overflow-hidden', versionsQuery.isLoading && 'py-3')}
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        {versionsQuery.isLoading ? (
          <div className="flex justify-center">
            <Loader size={16} className="text-(--color-text-tertiary)" />
          </div>
        ) : versionsQuery.isError ? (
          <p className="px-3 py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('publish.versionsFailed')}
          </p>
        ) : (
          <ul
            className="max-h-32 overflow-y-auto divide-y"
            style={{ borderColor: 'var(--color-border-subtle)' }}
            aria-labelledby={headingId}
          >
            {(versionsQuery.data ?? []).map((version) => (
              <VersionRow key={version.seq} version={version} />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

/** The review list: exactly what a publish sends. */
function ReviewList({ heading, status, files, added, unavailable, failed, fetching, onRetry }: {
  /** Null while the status is read. */
  heading: string | null;
  status: ArtifactStatus | undefined;
  files: ShareFileEntry[];
  added: ReadonlySet<string>;
  /** The status could not be read: for now, or for any other reason. */
  unavailable: boolean;
  failed: boolean;
  fetching: boolean;
  onRetry: () => void;
}) {
  const { t } = useTranslation();
  const headingId = useId();
  return (
    <div className="flex flex-col gap-1.5">
      <span id={headingId} className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {heading ?? t('publish.listing')}
      </span>
      <div
        className={cn('rounded-lg border overflow-hidden', !status && !failed && 'py-4')}
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        {failed ? (
          <div className="flex items-center justify-between gap-3 px-3 py-2 text-xs">
            <span style={{ color: 'var(--color-text-tertiary)' }}>
              {unavailable ? t('publish.unavailable') : t('publish.listFailed')}
            </span>
            <button
              type="button"
              onClick={onRetry}
              disabled={fetching}
              className="shrink-0 hover:underline disabled:opacity-50"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('common.retry')}
            </button>
          </div>
        ) : !status ? (
          <div className="flex justify-center">
            <Loader size={16} className="text-(--color-text-tertiary)" />
          </div>
        ) : (
          <ul
            className="max-h-56 overflow-y-auto divide-y"
            style={{ borderColor: 'var(--color-border-subtle)' }}
            aria-labelledby={headingId}
          >
            {files.map((entry) => (
              <FileRow key={entry.path} entry={entry} added={added.has(entry.path)} t={t} />
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

/**
 * Publish a workspace file: share its `/a/<code>` link and cut a version.
 *
 * The list is the review step: Publish (and Update, which is publishing
 * again) sends exactly the paths shown, and the server refuses a list that no
 * longer matches (409) with the one it found, which replaces the list and
 * waits for the owner to confirm again. Visitors get the latest version;
 * Unshare stops the link and keeps every version. The chat and the owner's
 * comments are never part of it.
 */
function PublishDialog({ open, workspaceId, path, onClose }: PublishDialogProps) {
  // Written by the review while a request is in flight, which holds the dialog open.
  const busy = useRef(false);
  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o && !busy.current) onClose(); }}>
      <DialogContent
        className="sm:max-w-lg"
        style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}
      >
        {/* Mounted only while the dialog shows, so every open starts a fresh
            review, and the exit animation plays over the last one. */}
        <PublishReview workspaceId={workspaceId} path={path} busyRef={busy} onClose={onClose} />
      </DialogContent>
    </Dialog>
  );
}

function PublishReview({ workspaceId, path, busyRef, onClose }: {
  workspaceId: string;
  path: string;
  busyRef: RefObject<boolean>;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const root = useWorkspaceFolders()?.rootDir;
  // A file never recorded has no status to review, so this makes its private record.
  const owned = useOwnedArtifact(workspaceId, path, { root });
  const artifact = owned.record ?? null;
  const latestSeq = artifact?.latest_seq ?? 0;
  const statusQuery = useArtifactStatus(artifact?.artifact_id, { always: true });
  const { unshare, keepUpdated } = useArtifactPublishing();
  const publish = usePublish(t('publish.failed'));
  const { copy, copiedCode } = useCopyShareLink();

  const [label, setLabel] = useState('');
  // Sent with the first publish; a shared file toggles the record itself.
  const [keepDraft, setKeepDraft] = useState<boolean | null>(null);
  const [outcome, setOutcome] = useState<Outcome | null>(null);

  const busy = publish.pending || unshare.isPending || keepUpdated.isPending;
  useEffect(() => {
    busyRef.current = busy;
  }, [busy, busyRef]);

  const shared = !!artifact?.shared;
  const status = statusQuery.data;
  const statusRefusal = refusalIn(statusQuery.error);
  // A cap or a missing file ends the review; a file unreadable for now is
  // retried from the list.
  const blocking = statusRefusal?.kind === 'unavailable' ? null : statusRefusal;
  const refusal = blocking ?? (outcome?.kind === 'refused' ? outcome.refusal : null);
  const files = outcome?.kind === 'conflict' && statusQuery.dataUpdatedAt <= outcome.statusAt
    ? outcome.files
    : status?.files ?? [];
  const drift = shared ? status?.drift ?? null : null;
  const added = new Set(drift?.added ?? []);
  const hasDrift = !!drift && drift.added.length + drift.removed.length + drift.changed.length > 0;
  // An Update of an unchanged file cuts nothing, so it waits for a change.
  const upToDate = shared && status?.changed === false && !hasDrift;
  // Publishing sends the list on screen, so it waits for one that is current.
  const listReady = !!status && !statusQuery.isFetching && !statusQuery.isError;
  const canPublish = !!artifact && listReady && files.length > 0 && !busy && !upToDate;
  const keepOn = shared ? !!artifact?.keep_updated : keepDraft ?? !!artifact?.keep_updated;
  const recordFailed = !!owned.error;
  const href = artifact ? shareLinkHref(artifact.code) : '';
  const copied = !!artifact && copiedCode === artifact.code;
  const name = artifact?.name ?? basename(path);

  const failed = (err: unknown, description: string) => {
    console.error('[PublishDialog] Request failed:', err);
    toast({ description, variant: 'destructive' });
  };

  const send = async () => {
    if (!artifact || !canPublish) return;
    const body: PublishArtifactRequest = { files: files.map((f) => f.path), label: label.trim() || null };
    if (!shared) body.keep_updated = keepOn;
    const statusAt = statusQuery.dataUpdatedAt;
    setOutcome(null);
    const result = await publish.send(artifact.artifact_id, body);
    if (result.kind === 'published') {
      setLabel('');
      setKeepDraft(null);
      if (!result.cut) setOutcome({ kind: 'unchanged', seq: result.version.seq });
    } else if (result.kind === 'filesChanged') {
      setOutcome({ kind: 'conflict', files: result.files, statusAt });
    } else if (result.kind === 'refused') {
      setOutcome({ kind: 'refused', refusal: result.refusal });
    }
  };

  const stop = () => {
    if (!artifact || busy) return;
    setOutcome((prev) => (prev?.kind === 'unchanged' ? null : prev));
    unshare.mutate(artifact.artifact_id, { onError: (err) => failed(err, t('publish.changeFailed')) });
  };

  const toggleKeep = () => {
    if (!artifact || busy) return;
    if (!shared) {
      setKeepDraft(!keepOn);
      return;
    }
    keepUpdated.mutate(
      { artifactId: artifact.artifact_id, on: !keepOn },
      { onError: (err) => failed(err, t('publish.changeFailed')) },
    );
  };

  const close = () => {
    if (!busy) onClose();
  };

  // A link shared before versions whose files were past the caps: visitors
  // get nothing until a publish, which clears it.
  const legacyRefused = shared && !!artifact?.legacy_refused;
  let stateHint: string;
  if (!shared) stateHint = latestSeq > 0 ? t('publish.privateWithVersions') : t('publish.privateHint');
  else if (legacyRefused) stateHint = t('publish.legacyRefused');
  else if (latestSeq === 0 || status?.changed == null) stateHint = t('publish.sharedHint');
  else if (upToDate) stateHint = t('publish.upToDate', { seq: latestSeq });
  else stateHint = t('publish.changedSince', { seq: latestSeq });

  const listHeading = !shared
    ? t('publish.filesPublish', { count: files.length })
    : upToDate
      ? t('publish.filesShared', { count: files.length })
      : t('publish.filesUpdate', { count: files.length });

  let refusalText: string | null = null;
  if (refusal?.kind === 'cap') {
    refusalText = refusal.cap.code === 'too_many_bytes'
      ? t('publish.tooManyBytes', { size: formatBytes(refusal.cap.limit, locale) })
      : t('publish.tooManyFiles', { limit: refusal.cap.limit });
  } else if (refusal?.kind === 'missing') {
    refusalText = t('publish.missing');
  } else if (refusal?.kind === 'unavailable') {
    refusalText = t('publish.unavailable');
  }

  return (
    <>
      <DialogTitle className="text-base font-semibold truncate pr-6" style={{ color: 'var(--color-text-primary)' }}>
        {t('publish.title', { file: name })}
      </DialogTitle>
      <DialogDescription className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
        {t('publish.description')}
      </DialogDescription>

      <div className="flex flex-col gap-4 pt-1">
        {/* The link */}
        <div className="flex items-center gap-2">
          <div
            className="flex-1 flex items-center px-3 py-2 rounded-md text-xs min-w-0"
            style={{ backgroundColor: 'var(--color-bg-input)', color: 'var(--color-text-secondary)' }}
          >
            <span className="truncate font-mono" data-selectable>
              {href || (recordFailed ? t('publish.linkFailed') : '…')}
            </span>
          </div>
          {recordFailed ? (
            <Button variant="outline" size="sm" className="text-xs" onClick={owned.retry} disabled={owned.fetching}>
              {t('common.retry')}
            </Button>
          ) : (
            <Button
              variant="outline"
              size="sm"
              className="gap-1.5 text-xs"
              onClick={() => { if (artifact) void copy(artifact.code); }}
              disabled={!artifact}
            >
              {copied
                ? <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} />
                : <Copy className="h-3.5 w-3.5" />}
              {copied ? t('publish.copied') : t('publish.copy')}
            </Button>
          )}
        </div>

        {/* Where it stands, and keep updated */}
        <div
          className="flex flex-col rounded-lg border divide-y"
          style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-card)' }}
        >
          <div className="flex items-center justify-between gap-3 px-3 py-2.5">
            <div className="flex items-center gap-2 min-w-0">
              {shared
                ? <Link2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                : <Lock className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />}
              <div className="flex flex-col min-w-0">
                <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {!shared
                    ? t('publish.private')
                    : latestSeq > 0 ? t('publish.publishedAt', { seq: latestSeq }) : t('publish.published')}
                </span>
                <span className="text-xs" style={{ color: legacyRefused ? 'var(--color-warning)' : 'var(--color-text-tertiary)' }}>{stateHint}</span>
              </div>
            </div>
            {shared && (
              <Button
                variant="outline"
                size="sm"
                className="h-7 shrink-0 gap-1.5 px-2.5 text-xs"
                onClick={stop}
                disabled={busy}
              >
                {unshare.isPending && <Loader size={12} style={{ color: 'inherit' }} />}
                {t('publish.unshare')}
              </Button>
            )}
          </div>
          <div
            className="flex items-center justify-between gap-3 px-3 py-2.5"
            style={{ borderColor: 'var(--color-border-muted)' }}
          >
            <div className="flex flex-col min-w-0">
              <span className="text-sm" style={{ color: 'var(--color-text-primary)' }}>
                {t('publish.keepUpdated')}
              </span>
              <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('publish.keepUpdatedHint')}
              </span>
            </div>
            {keepUpdated.isPending
              ? <Loader size={16} className="text-(--color-text-tertiary)" />
              : (
                <ToggleSwitch
                  checked={keepOn}
                  onChange={toggleKeep}
                  disabled={!artifact || busy}
                  ariaLabel={t('publish.keepUpdated')}
                />
              )}
          </div>
        </div>

        {latestSeq > 0 && <VersionList artifactId={artifact?.artifact_id} />}

        {/* Notices, about the list and the last attempt */}
        {outcome?.kind === 'conflict' && (
          <p className="text-xs" role="status" style={{ color: 'var(--color-warning)' }}>
            {t('publish.filesChanged')}
          </p>
        )}
        {refusalText && (
          <p className="text-xs" role="status" style={{ color: 'var(--color-warning)' }}>
            {refusalText}
          </p>
        )}
        {outcome?.kind === 'unchanged' && (
          <p className="text-xs" role="status" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('publish.unchanged', { seq: outcome.seq })}
          </p>
        )}
        {added.size > 0 && (
          <p
            className="rounded-lg border px-3 py-2.5 text-xs"
            style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-warning-soft)', color: 'var(--color-text-primary)' }}
          >
            {t('publish.driftAdded', { count: added.size })}
          </p>
        )}

        {/* A refusal has no list to show, nor does a record that failed to load. */}
        {!recordFailed && !blocking && (
          <ReviewList
            heading={status ? listHeading : null}
            status={status}
            files={files}
            added={added}
            unavailable={statusRefusal?.kind === 'unavailable'}
            failed={statusQuery.isError}
            fetching={statusQuery.isFetching}
            onRetry={() => void statusQuery.refetch()}
          />
        )}

        {/* The version's note */}
        <label className="flex flex-col gap-1.5">
          <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
            {t('publish.label')}
          </span>
          <Input
            value={label}
            onChange={(e) => setLabel(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter' && !e.nativeEvent.isComposing) {
                e.preventDefault();
                void send();
              }
            }}
            maxLength={LABEL_MAX}
            placeholder={t('publish.labelPlaceholder')}
            disabled={busy || upToDate || !artifact}
            className="h-8 sm:text-xs bg-(--color-bg-input) border-(--color-border-muted) text-(--color-text-primary)"
          />
        </label>

        {/* Footer */}
        <div className="flex items-center justify-between gap-3 pt-1">
          {shared ? (
            <a
              href={`${href}?as=visitor`}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-xs hover:underline"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              <ExternalLink className="h-3.5 w-3.5" />
              {t('publish.viewAsVisitor')}
            </a>
          ) : <span />}
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={close} disabled={busy}>
              {t('common.close')}
            </Button>
            <Button size="sm" className="gap-1.5" onClick={() => void send()} disabled={!canPublish}>
              {publish.pending && <Loader size={14} style={{ color: 'inherit' }} />}
              {shared ? t('publish.update') : t('publish.publish')}
            </Button>
          </div>
        </div>
      </div>
    </>
  );
}

export default PublishDialog;
