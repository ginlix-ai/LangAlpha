import { useEffect, useId, useRef, useState, type RefObject } from 'react';
import { Check, Copy, ExternalLink, Link2, Lock } from 'lucide-react';
import { Trans, useTranslation } from 'react-i18next';
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
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip';
import { toast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';
import { formatBytes, shortMonthDay } from '@/lib/format';
import { useCopyShareLink } from '@/hooks/useCopyShareLink';
import { useLocale } from '@/hooks/useLocale';
import {
  useArtifactPublishing,
  useArtifactStatus,
  useFileLeftOut,
  useFileLeftOutPending,
  useArtifactVersions,
  useOwnedArtifact,
  sharedSeq,
} from '@/hooks/useArtifact';
import { usePageScout } from './viewers/html/usePageScout';
import type { ArtifactStatus, ArtifactVersion, ShareFileEntry } from '@/types/api';
import { useWorkspaceFolders } from '../contexts/WorkspaceContext';
import { basename, dirname } from '../utils/fileRefResolver';
import { shareLinkHref } from '../utils/api/shareLinks';
import { refusalIn, usePublish, type PublishBody, type PublishRefusal } from './filePanel/usePublish';

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

function FileRow({ artifactId, entry, added, leftOut, gutter, locked }: {
  artifactId: string;
  entry: ShareFileEntry;
  /** Read by the file now but in no version yet: what an Update would add. */
  added: boolean;
  /** Off what a publish carries, and on offer to put back. */
  leftOut: boolean;
  /** Some row of the list has a box, so the names line up past it. */
  gutter: boolean;
  /** A publish is under way with the list as it stands. */
  locked: boolean;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const toggle = useFileLeftOut(artifactId);
  const out = toggle.isPending ? toggle.variables.leftOut : leftOut;
  const dir = dirname(entry.path);
  const name = basename(entry.path);
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <li
          tabIndex={0}
          className="flex items-center gap-3 px-3 py-1.5 text-xs outline-none hover:bg-(--color-bg-hover) focus-visible:bg-(--color-bg-hover)"
          data-selectable
        >
          {leftOut || entry.optional ? (
            <input
              type="checkbox"
              className="h-3.5 w-3.5 shrink-0 cursor-pointer accent-(--color-accent-primary)"
              aria-label={t('publish.includeFile', { file: name })}
              checked={!out}
              disabled={locked || toggle.isPending}
              onChange={(e) => toggle.mutate(
                { path: entry.path, leftOut: !e.target.checked },
                { onError: () => toast({ description: t('publish.leftOutFailed'), variant: 'destructive' }) },
              )}
            />
          ) : gutter && <span aria-hidden className="w-3.5 shrink-0" />}
          {/* The folder gives way first: rows cut from the end would all read as
              the same folder. A touch screen cannot hover, so there it all wraps. */}
          <span className="flex flex-1 min-w-0 pointer-coarse:block pointer-coarse:wrap-anywhere">
            {dir && (
              <span className="min-w-0 truncate pointer-coarse:whitespace-normal" style={{ color: 'var(--color-text-tertiary)' }}>
                {dir}/
              </span>
            )}
            <span
              className="shrink-0 max-w-full truncate pointer-coarse:whitespace-normal"
              style={{ color: out ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' }}
            >
              {name}
            </span>
          </span>
          <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
            {entry.size !== null && formatBytes(entry.size, locale)}
          </span>
          <span
            className="shrink-0 w-24 text-right"
            style={{ color: added && !out ? 'var(--color-warning)' : 'var(--color-text-quaternary)' }}
          >
            {out ? t('publish.leftOut') : added ? t('publish.notShared') : t(`shareLink.reason.${entry.reason}`)}
          </span>
        </li>
      </TooltipTrigger>
      <TooltipContent side="top" align="start" className="max-w-md">
        <p className="wrap-anywhere">{entry.path}</p>
        <p className="mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
          {out
            ? t('publish.leftOutDetail')
            : t(`shareLink.reasonDetail.${entry.reason === 'loaded' && entry.optional ? 'loadedBefore' : entry.reason}`)}
        </p>
        {added && !out && <p className="mt-1" style={{ color: 'var(--color-warning)' }}>{t('publish.notSharedDetail')}</p>}
      </TooltipContent>
    </Tooltip>
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
function ReviewList({ artifactId, heading, status, files, added, failure, failed, fetching, locked, onRetry }: {
  artifactId: string;
  /** Null while the status is read. */
  heading: string | null;
  status: ArtifactStatus | undefined;
  files: ShareFileEntry[];
  added: ReadonlySet<string>;
  /** Why the status could not be read, when the owner can do something about it. */
  failure: string | null;
  failed: boolean;
  fetching: boolean;
  locked: boolean;
  onRetry: () => void;
}) {
  const { t } = useTranslation();
  const headingId = useId();
  const leftOut = status?.left_out ?? [];
  // By name rather than as the server found them, so a row stays under the
  // pointer when its box is changed. One array, so it also keeps its focus.
  const rows = [...files, ...leftOut].sort((a, b) =>
    Number(b.reason === 'entry') - Number(a.reason === 'entry') || a.path.localeCompare(b.path));
  const gutter = rows.some((entry) => entry.optional) || leftOut.length > 0;
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
              {failure ?? t('publish.listFailed')}
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
          <TooltipProvider delayDuration={300} skipDelayDuration={600}>
            <ul
              className="max-h-56 overflow-y-auto divide-y"
              style={{ borderColor: 'var(--color-border-subtle)' }}
              aria-labelledby={headingId}
            >
              {rows.map((entry) => (
                <FileRow
                  key={entry.path}
                  artifactId={artifactId}
                  entry={entry}
                  added={added.has(entry.path)}
                  leftOut={leftOut.includes(entry)}
                  gutter={gutter}
                  locked={locked}
                />
              ))}
            </ul>
          </TooltipProvider>
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
        style={{ backgroundColor: 'var(--color-shell-float, var(--color-bg-page))', borderColor: 'var(--color-shell-edge, var(--color-border-muted))' }}
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
  // What the dialog says of visitors is said of their version, not the newest.
  const visitorsAt = sharedSeq(artifact);
  const scout = usePageScout(workspaceId, artifact?.path ?? null);
  // Read once before the scout loads the page, naming the code it will run,
  // and again once it has heard the page out.
  const statusQuery = useArtifactStatus(artifact, { always: true, report: true, enabled: scout.reads });
  const { unshare, keepUpdated } = useArtifactPublishing();
  const changingList = useFileLeftOutPending(artifact?.artifact_id ?? '');
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
  scout.follow(status?.page_sha256);
  const statusRefusal = refusalIn(statusQuery.error);
  // A cap or a missing file ends the review; a file unreadable for now is
  // retried from the list.
  // Said where the list would be, so not again below it.
  const statusFailure = statusRefusal?.kind === 'unavailable' ? t('publish.unavailable')
    : statusRefusal?.kind === 'needsComputer' ? t('publish.needsComputer') : null;
  const blocking = statusFailure ? null : statusRefusal;
  // The page could not run to say what it loads: said and retried as a list
  // unreadable for now is.
  const listFailure = statusFailure ?? (scout.failed ? t('publish.unavailable') : null);
  // Past a cap the list still comes, for the owner to leave out what puts it over.
  const overCap = status?.over_cap ?? null;
  const refusal = blocking
    ?? (overCap ? { kind: 'cap' as const, cap: overCap } : null)
    ?? (outcome?.kind === 'refused' ? outcome.refusal : null);
  const files = outcome?.kind === 'conflict' && statusQuery.dataUpdatedAt <= outcome.statusAt
    ? outcome.files
    : status?.files ?? [];
  const drift = shared ? status?.drift ?? null : null;
  const added = new Set(drift?.added ?? []);
  const rootRefs = status?.root_refs ?? [];
  const hasDrift = !!drift && drift.added.length + drift.removed.length + drift.changed.length > 0;
  // An Update of an unchanged file cuts nothing, so it waits for a change.
  const upToDate = shared && status?.changed === false && !hasDrift;
  // Publishing sends the list on screen, so it waits for one that is current.
  const listReady = !!status && !overCap && !statusQuery.isFetching && !statusQuery.isError && !scout.holding && !changingList;
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
    // With the code it was read under: the server refuses the publish once
    // the agent has changed it, and the review loads the page again.
    const body: PublishBody = {
      files: files.map((f) => f.path),
      label: label.trim() || null,
      page_sha256: status?.page_sha256 ?? undefined,
    };
    if (!shared) body.keep_updated = keepOn;
    const statusAt = statusQuery.dataUpdatedAt;
    setOutcome(null);
    const result = await publish.send(artifact, body);
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
  else if (visitorsAt === 0 || status?.changed == null) stateHint = t('publish.sharedHint');
  else if (upToDate) stateHint = t('publish.upToDate', { seq: visitorsAt });
  else stateHint = t('publish.changedSince', { seq: visitorsAt });

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
    // Said only where a box can bring it back under.
    if (overCap && files.some((f) => f.optional)) refusalText += ` ${t('publish.overCapLeaveOut')}`;
  } else if (refusal?.kind === 'missing') {
    refusalText = t('publish.missing');
  } else if (refusal?.kind === 'unavailable') {
    refusalText = t('publish.unavailable');
  } else if (refusal?.kind === 'needsComputer') {
    refusalText = t('publish.needsComputer');
  }

  return (
    <>
      {scout.element}
      {/* The ellipsis is the words' own, so Terminal's brackets (data-bracket,
          on the title) both stay in view around a long name. */}
      <DialogTitle className="flex min-w-0 text-base font-semibold pr-6" style={{ color: 'var(--color-text-primary)' }} title={name}>
        <span className="truncate">{t('publish.title', { file: name })}</span>
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
                    : visitorsAt > 0 ? t('publish.publishedAt', { seq: visitorsAt }) : t('publish.published')}
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
        {rootRefs.length > 0 && (
          <p className="text-xs" role="status" style={{ color: 'var(--color-warning)' }}>
            <Trans
              i18nKey="publish.rootRefs"
              values={{ example: rootRefs[0] }}
              components={{ path: <code className="break-all" />, cmd: <code className="whitespace-nowrap" /> }}
            />
          </p>
        )}

        {/* A refusal has no list to show, nor does a record that failed to load. */}
        {!recordFailed && !blocking && (
          <ReviewList
            artifactId={artifact?.artifact_id ?? ''}
            heading={status ? listHeading : null}
            status={status}
            files={files}
            added={added}
            failure={listFailure}
            locked={busy}
            failed={statusQuery.isError || scout.failed}
            fetching={statusQuery.isFetching}
            onRetry={() => (scout.failed ? scout.retry() : void statusQuery.refetch())}
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
