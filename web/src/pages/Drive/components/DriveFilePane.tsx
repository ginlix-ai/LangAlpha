import type React from 'react';
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { UseQueryResult } from '@tanstack/react-query';
import { RotateCcw } from 'lucide-react';
import { apiErrorStatus } from '@/api/errors';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useDriveVersions } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { FileBody as Body } from '@/pages/ChatAgent/components/filePanel/fileBody';
import type { FileError } from '@/pages/ChatAgent/components/filePanel/fileErrors';
import type { OpenFileHandler } from '@/pages/ChatAgent/utils/fileLocation';
import type { DriveEntry, DriveVersion } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { atVersion, driveBodyMode, tooLargeToPreview, useDriveBody } from '../driveBody';
import { nameOf } from '../drivePaths';
import type { DriveEditor } from '../useDriveEditor';
import { whoLabel } from '../who';
import { DriveBar } from './DriveBar';
import { DriveHistory } from './DriveHistory';
import { FileBody, type BodyView } from './FileBody';
import { FileHeader } from './FileHeader';

/**
 * What a diff sets beside what: the editor's draft beside the head it
 * conflicts with, or a past version beside the current file or the one
 * before it.
 */
type DiffWith = 'theirs' | 'current' | 'previous';

function bodyError(err: unknown): FileError | null {
  if (!err) return null;
  return apiErrorStatus(err) === 404 ? { category: 'not_found' } : { category: 'unknown' };
}

/**
 * The open file: its header, a bar when something needs saying, then the
 * file. History sits beside it on a wide screen and takes its place on a
 * phone.
 */
export function DriveFilePane({ path, entry, entryError, onRetry, version, showHistory, mobile, editor, head, imageLoader, onOpenLink }: {
  path: string;
  entry: DriveEntry | undefined;
  entryError: 'gone' | 'failed' | null;
  onRetry: () => void;
  /** The version shown in place of the head, from the URL. */
  version: number | null;
  showHistory: boolean;
  mobile: boolean;
  editor: DriveEditor;
  /** The head's body, read for the viewer and the editor alike. */
  head: UseQueryResult<Body>;
  imageLoader: (path: string) => Promise<string>;
  onOpenLink: OpenFileHandler;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { space, actions, go } = useDriveContext();
  const { session } = editor;
  const [htmlSource, setHtmlSource] = useState(false);

  // A diff belongs to what it was opened on: another version, or an editor
  // that has settled its conflict, starts without one.
  const diffKey = session ? `edit:${session.sitting}:${session.conflict}` : `view:${version ?? ''}`;
  const [diffState, setDiffState] = useState<{ key: string; with: DiffWith } | null>(null);
  const diff = diffState?.key === diffKey ? diffState.with : null;
  const toggleDiff = (next: DiffWith) => setDiffState(diff === next ? null : { key: diffKey, with: next });

  const versionsQuery = useDriveVersions(space, entry?.id ?? null, !!entry && (showHistory || version !== null));
  const listing = versionsQuery.data;
  const versions = listing?.versions;
  const viewed: DriveVersion | null = version !== null ? versions?.find((v) => v.seq === version) ?? null : null;
  const shownSeq = viewed?.seq ?? versions?.[0]?.seq ?? entry?.version ?? null;
  const previous: DriveVersion | null = shownSeq !== null ? versions?.find((v) => v.seq < shownSeq) ?? null : null;

  // A past version opens by its own size and kind of bytes.
  const shownEntry = entry ? atVersion(entry, viewed) : null;
  const old = useDriveBody(space, entry && viewed ? shownEntry : null, viewed ? { seq: viewed.seq, sha: viewed.sha256 } : null);
  const prev = useDriveBody(
    space,
    entry && diff === 'previous' && previous ? atVersion(entry, previous) : null,
    diff === 'previous' && previous ? { seq: previous.seq, sha: previous.sha256 } : null,
  );
  const shown = viewed ? old : head;
  const mode = shownEntry ? driveBodyMode(shownEntry) : 'none';
  const isText = !!shownEntry?.is_text && mode !== 'none';
  const isHtml = mode === 'html';
  const historyOpen = showHistory && !session;

  const setVersion = (seq: number | null) => go({ file: path, history: showHistory, version: seq });
  const toggleHistory = (open: boolean) => go({ file: path, history: open, version: open ? version : null }, { replace: true });

  const restore = async () => {
    if (!entry || !viewed) return;
    if (await actions.restoreVersion(entry, viewed.seq)) setVersion(null);
  };

  const theirsBy = whoLabel(t, entry?.modified_by);
  const bars: React.ReactNode[] = [];
  if (session?.conflict && editor.theirs) {
    bars.push(
      <DriveBar
        key="conflict"
        role="alert"
        actions={(
          <>
            <HeaderButton variant="ghost" onClick={() => toggleDiff('theirs')}>
              {diff === 'theirs' ? t('drive:conflict.hideCompare') : t('drive:conflict.compare')}
            </HeaderButton>
            <HeaderButton onClick={editor.takeTheirs}>{t('drive:conflict.takeTheirs')}</HeaderButton>
            <HeaderButton variant="primary" onClick={editor.keepMine} disabled={session.save !== 'idle'}>{t('drive:conflict.keepMine')}</HeaderButton>
          </>
        )}
      >
        <strong>{t('drive:conflict.title')}</strong>{' '}{t('drive:conflict.body', { who: theirsBy })}
      </DriveBar>,
    );
  } else if (editor.behind && editor.dirty) {
    bars.push(
      <DriveBar key="behind" role="status" actions={<HeaderButton onClick={editor.review}>{t('drive:edit.review')}</HeaderButton>}>
        {t('drive:edit.changedUnder', { who: theirsBy })}
      </DriveBar>,
    );
  }
  if (!session && viewed) {
    const compare = (target: DiffWith, label: string) => (
      <HeaderButton
        variant="ghost"
        onClick={() => toggleDiff(target)}
        className={cn(diff === target && 'drive-btn-on')}
        aria-pressed={diff === target}
      >
        {label}
      </HeaderButton>
    );
    bars.push(
      <DriveBar
        key="version"
        actions={(
          <>
            {isText && compare('current', t('drive:history.compareCurrent'))}
            {isText && previous && compare('previous', t('drive:history.comparePrevious', { seq: previous.seq }))}
            <HeaderButton icon={RotateCcw} onClick={() => void restore()} disabled={actions.pending.restoreVersion} title={t('drive:history.restoreHint')}>
              {t('drive:actions.restore')}
            </HeaderButton>
            <HeaderButton variant="ghost" onClick={() => setVersion(null)}>{t('drive:history.backToCurrent')}</HeaderButton>
          </>
        )}
      >
        {t('drive:history.viewing', { seq: viewed.seq, when: relativeTime(viewed.created_at, locale, now), who: whoLabel(t, viewed.actor) })}
      </DriveBar>,
    );
  }

  const pair = session || !diff || diff === 'theirs'
    ? null
    : diff === 'current'
      ? { original: old.data?.content, modified: head.data?.content, reads: [old, head] }
      : { original: prev.data?.content, modified: shown.data?.content, reads: [prev, shown] };
  const name = entry?.name ?? nameOf(path);

  const view = ((): BodyView => {
    // An open editor keeps its text on screen even when the file goes from under it.
    if (session && entry) {
      return {
        kind: 'editor',
        name: entry.name,
        draft: session.draft,
        original: editor.theirs?.content ?? session.base.content,
        compare: diff === 'theirs',
        modelPath: session.modelPath,
        editorRef: editor.editorRef,
        onChange: editor.change,
      };
    }
    if (entryError) return { kind: 'failure', name, gone: entryError === 'gone', onRetry };
    if (!entry || !space) return { kind: 'loading' };
    // A version the URL names that the listing cannot show is never stood in for by the head.
    if (version !== null && !viewed) {
      if (versionsQuery.error) return { kind: 'failure', name, gone: false, onRetry: () => void versionsQuery.refetch() };
      if (versionsQuery.isPending) return { kind: 'loading' };
      return { kind: 'versionGone', onBack: () => setVersion(null) };
    }
    if (pair) {
      const failed = pair.reads.find((r) => r.error);
      if (failed) return { kind: 'failure', name: entry.name, gone: false, onRetry: () => void failed.refetch() };
      return pair.original == null || pair.modified == null
        ? { kind: 'loading' }
        : { kind: 'comparison', name: entry.name, original: pair.original, modified: pair.modified };
    }
    const content = shown.data?.content;
    if (isHtml && content != null) return { kind: 'html', name: entry.name, content, source: htmlSource };
    return {
      kind: 'document',
      entry,
      body: shown.data ?? null,
      loading: mode !== 'none' && shown.isPending,
      // A body read as none is a PDF past the bound that could not be read by range.
      error: mode === 'none'
        ? { category: tooLargeToPreview(shownEntry ?? entry) ? 'too_large' : 'binary_file' }
        : shown.data?.mode === 'none' ? { category: 'too_large' } : bodyError(shown.error),
      onRetry: () => void shown.refetch(),
      seq: viewed?.seq ?? null,
    };
  })();

  const header = (
    <FileHeader
      path={path}
      entry={entry}
      mobile={mobile}
      editor={editor}
      canEdit={isText && version === null && !!head.data?.version}
      html={isHtml && version === null ? { source: htmlSource, toggle: () => setHtmlSource((s) => !s) } : null}
      historyOpen={historyOpen}
      onToggleHistory={() => toggleHistory(!showHistory)}
    />
  );
  const history = (
    <DriveHistory
      name={name}
      versions={versions}
      historyBytes={listing?.history_bytes ?? 0}
      tooLarge={!!listing?.too_large_for_history}
      onClear={entry ? () => actions.clearHistory(entry) : undefined}
      clearing={actions.pending.clearHistory}
      loading={versionsQuery.isPending}
      failed={!!versionsQuery.error}
      onRetry={() => void versionsQuery.refetch()}
      viewing={version}
      // On a phone the history gives way to the version picked, in one step: two would leave the second's version.
      onView={(seq) => (mobile ? go({ file: path, history: false, version: seq }) : setVersion(seq))}
      onClose={() => toggleHistory(false)}
    />
  );
  // On a phone the history takes the pane; on a wide screen it sits beside the file.
  if (mobile && historyOpen) {
    return (
      <section className="drive-file" aria-label={name}>
        {header}
        {history}
      </section>
    );
  }

  const fill = !!session || !!pair || (isHtml && !htmlSource);
  return (
    <section
      className="drive-file"
      aria-label={name}
      onKeyDown={(e) => {
        if (session && (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 's') {
          e.preventDefault();
          void editor.save();
        }
      }}
    >
      {header}
      {bars}
      <div className="drive-file-body">
        <div className={cn('drive-file-main', fill && 'is-fill')}>
          <FileBody view={view} imageLoader={imageLoader} onOpenLink={onOpenLink} />
        </div>
        {historyOpen && !mobile && history}
      </div>
    </section>
  );
}
