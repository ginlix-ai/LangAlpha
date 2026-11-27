import React, { Suspense } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import type { UseQueryResult } from '@tanstack/react-query';
import { Select } from '@/components/ui/select';
import { useDriveVersions, useDriveVersionText, useTextsApart } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { shortMonthDay } from '@/lib/format';
import { cn } from '@/lib/utils';
import { DocumentLoadingFallback } from '@/pages/ChatAgent/components/filePanel/fallbacks';
import type { DriveEntry, DriveVersion, DriveVersionText } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { loadFailedLine, readProblem } from '../driveRefusal';
import { PaneProblem } from './PaneProblem';
import { whoLabel } from '../who';

const CodeEditor = React.lazy(() => import('@/pages/ChatAgent/components/viewers/CodeEditor'));

/** Why a version's text cannot be compared, or null when it can. */
function textProblem(t: TFunction, read: UseQueryResult<DriveVersionText>, seq: number): { line: string; retry: boolean } | null {
  if (read.error) return readProblem(read.error, t, { notText: t('drive:changes.notText'), busy: t('drive:changes.stillReading', { seq }) });
  // A Word or PDF version the server could not read text out of. A failed
  // read is tried again on the server's own schedule, not on a click.
  switch (read.data?.status) {
    case 'too_large': return { line: t('drive:changes.tooLarge', { seq }), retry: false };
    case 'empty': return { line: t('drive:changes.empty', { seq }), retry: false };
    case 'failed': return { line: t('drive:changes.failed', { seq }), retry: false };
    default: return null;
  }
}

/**
 * One version of a memo against another, additions and removals marked in
 * the text itself. The one before is the default; any other can be picked.
 * Word and PDF memos are compared on the text the server reads out of them.
 */
export function MemoChanges({ entry, version, against, onPick }: {
  entry: DriveEntry;
  /** The version shown, as `resolveFileView` reads the address; null is the newest. */
  version: number | null;
  /** The version it is set against, as resolved; null is the one before it. */
  against: number | null;
  onPick: (version: number | null, against: number | null) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { space } = useDriveContext();
  const listing = useDriveVersions(space, entry.id);
  const versions = listing.data?.versions ?? [];
  const shownSeq = version ?? entry.version ?? null;
  const shown: DriveVersion | undefined = versions.find((v) => v.seq === shownSeq);
  const previous = shownSeq !== null ? versions.find((v) => v.seq < shownSeq) : undefined;
  const base: DriveVersion | undefined = against !== null ? versions.find((v) => v.seq === against) : previous;

  // Whether a version has text to compare is the server's to say: a Word or
  // PDF file is read as extracted text even where its bytes pass for text.
  const ok = !!shown && shown.text !== 'none';
  const modified = useDriveVersionText(space, entry.id, entry.name, ok && shown ? { seq: shown.seq, sha: shown.sha256 } : null);
  const original = useDriveVersionText(space, entry.id, entry.name, ok && base ? { seq: base.seq, sha: base.sha256 } : null);

  // A save folded into the head changes its bytes under the same seq, so a
  // text can be newer than the listing it was asked from; read that again.
  const apart = useTextsApart(listing, [{ read: modified, sha: shown?.sha256 }, { read: original, sha: base?.sha256 }]);

  const label = (v: DriveVersion) => t('drive:changes.option', {
    seq: v.seq,
    date: shortMonthDay(new Date(v.created_at), locale),
    who: whoLabel(t, v.actor),
  });

  let body: React.ReactNode;
  if (listing.error) {
    body = <PaneProblem line={loadFailedLine(listing.error, t)} onRetry={() => void listing.refetch()} />;
  } else if (listing.isPending) {
    body = <DocumentLoadingFallback />;
  } else if (!shown) {
    body = <PaneProblem line={t('drive:errors.gone')} onBack={() => onPick(null, null)} />;
  } else if (!ok) {
    body = <PaneProblem line={t('drive:changes.notText')} />;
  } else if (apart && !modified.error && !original.error) {
    // A failed reread keeps the other bytes' text beside its error, so the
    // error is what shows; the problem branch below never reaches the editor.
    body = <DocumentLoadingFallback />;
  } else {
    const problem = textProblem(t, modified, shown.seq) ?? (base ? textProblem(t, original, base.seq) : null);
    if (problem) {
      body = <PaneProblem line={problem.line} onRetry={problem.retry ? () => void Promise.all([modified.refetch(), original.refetch()]) : undefined} />;
    } else if (modified.data?.text == null || (base && original.data?.text == null)) {
      body = <DocumentLoadingFallback />;
    } else {
      const cut = modified.data.truncated || original.data?.truncated;
      body = (
        <>
          {!base && <p className="drive-changes-note">{t('drive:changes.first', { seq: shown.seq })}</p>}
          {cut && <p className="drive-changes-note">{t('drive:changes.truncated')}</p>}
          <div className="drive-editor">
            <Suspense fallback={<DocumentLoadingFallback />}>
              <CodeEditor
                value={modified.data.text}
                originalValue={original.data?.text ?? ''}
                // Extracted text is plain whatever the file's own kind.
                fileName={modified.data.source === 'extracted' ? 'extracted.txt' : entry.name}
                diffMode
                inline
                readOnly
              />
            </Suspense>
          </div>
        </>
      );
    }
  }

  return (
    <div className="drive-changes">
      {versions.length > 0 && shown && (
        <div className="drive-changes-bar">
          <label className="drive-changes-pick">
            <span>{t('drive:changes.showing')}</span>
            <Select
              value={String(shown.seq)}
              onChange={(e) => onPick(Number(e.target.value), null)}
              aria-label={t('drive:changes.showing')}
              className="drive-changes-select"
            >
              {versions.map((v) => <option key={v.seq} value={v.seq}>{label(v)}</option>)}
            </Select>
          </label>
          <span className="drive-changes-pick">
            <span>{t('drive:changes.compareWith')}</span>
            {previous && (
              <button
                type="button"
                className={cn('drive-link-button', base?.seq === previous.seq && 'is-on')}
                aria-pressed={base?.seq === previous.seq}
                onClick={() => onPick(shown.seq, null)}
              >
                {t('drive:changes.versionN', { seq: previous.seq })}
              </button>
            )}
            <Select
              value={base && base.seq !== previous?.seq ? String(base.seq) : ''}
              onChange={(e) => onPick(shown.seq, e.target.value ? Number(e.target.value) : null)}
              aria-label={t('drive:changes.anyVersion')}
              className="drive-changes-select"
            >
              <option value="">{t('drive:changes.anyVersion')}</option>
              {versions.filter((v) => v.seq !== shown.seq).map((v) => <option key={v.seq} value={v.seq}>{label(v)}</option>)}
            </Select>
          </span>
        </div>
      )}
      <div className="drive-changes-body">{body}</div>
    </div>
  );
}
