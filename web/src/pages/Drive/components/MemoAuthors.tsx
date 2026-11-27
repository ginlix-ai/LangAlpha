import { useTranslation } from 'react-i18next';
import { useDriveAttribution, useDriveVersionText, useTextsApart } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatTimestamp, localDayStart, shortMonthDay } from '@/lib/format';
import { DocumentLoadingFallback } from '@/pages/ChatAgent/components/filePanel/fallbacks';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { readProblem } from '../driveRefusal';
import { authorRuns } from '../memoMarks';
import { whoLabel } from '../who';
import { ActorGlyph } from './Actor';
import { PaneProblem } from './PaneProblem';

/**
 * The memo's current text, a run of lines at a time, each run labelled in
 * the narrow column beside it with a glyph for a person or the agent, who
 * wrote it and when; the label opens the version that wrote it in Changes.
 */
export function MemoAuthors({ entry, onPick }: { entry: DriveEntry; onPick: (seq: number) => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { space } = useDriveContext();
  const attribution = useDriveAttribution(space, entry.id, entry.version !== null && entry.sha256 ? { seq: entry.version, sha: entry.sha256 } : null);
  const text = useDriveVersionText(
    space,
    entry.id,
    entry.name,
    attribution.data ? { seq: attribution.data.seq, sha: attribution.data.sha256 } : null,
  );

  // A save folded into the head's version changes its bytes under the same
  // seq, so the text can be newer than the attribution it was asked for.
  const apart = useTextsApart(attribution, [{ read: text, sha: attribution.data?.sha256 }]);

  const failed = attribution.error ?? text.error;
  if (failed) {
    const problem = readProblem(failed, t, { notText: t('drive:authors.notText'), notMemo: t('drive:authors.notMemo'), busy: t('drive:authors.busy') });
    return <PaneProblem line={problem.line} onRetry={problem.retry ? () => void (attribution.error ? attribution.refetch() : text.refetch()) : undefined} />;
  }
  if (!attribution.data || text.data?.text == null || apart) return <DocumentLoadingFallback />;

  const runs = authorRuns(text.data.text, attribution.data);
  return (
    <div className="drive-authors" role="table" aria-label={t('drive:memo.tabs.who')}>
      {runs.map((run, i) => {
        const v = run.version;
        const at = v ? new Date(v.created_at).getTime() : null;
        const when = at === null ? '' : localDayStart(at) === localDayStart(now) ? t('drive:authors.today') : shortMonthDay(at, locale);
        const who = v ? whoLabel(t, v.actor) : '';
        return (
          <div key={`${run.seq}-${i}`} className="drive-authors-run" role="row">
            <span role="rowheader" className="drive-authors-label">
              <button
                type="button"
                className="drive-authors-who"
                onClick={() => onPick(run.seq)}
                title={v ? t('drive:authors.title', { seq: run.seq, when: formatTimestamp(v.created_at, locale) }) : undefined}
              >
                {v && <ActorGlyph kind={v.actor.kind} />}
                <span>{who ? t('drive:authors.label', { who, when }) : t('drive:changes.versionN', { seq: run.seq })}</span>
              </button>
            </span>
            <pre role="cell" className="drive-authors-text">{run.lines.join('\n')}</pre>
          </div>
        );
      })}
    </div>
  );
}
