import { useEffect, useId, useRef, useState } from 'react';
import type { TFunction } from 'i18next';
import { useTranslation } from 'react-i18next';
import { HeaderButton, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { Input } from '@/components/ui/input';
import { ChevronRight, MoreHorizontal, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from '@/components/ui/dropdown-menu';
import { recordEnd, useDriveRecord, useDriveVersions } from '@/hooks/useDrive';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { chatTime, dayRangeEnds, formatBytes, formatTimestamp, formatUnitList, integer } from '@/lib/format';
import type { DriveClearShown, DriveEntry, DriveMemoStatus, DriveModifiedBy, DriveRecordEvent, DriveRecordVersion, DriveVersion } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { loadFailedLine } from '../driveRefusal';
import { memoState, pinnedSeq, revisedSince } from '../memoMarks';
import { changeStats, recordRows } from '../recordRows';
import { whoLabel } from '../who';
import { ICON_BUTTON } from './PaneFrame';
import { Actor, ActorGlyph, ActorSentence } from './Actor';
import { PaneProblem } from './PaneProblem';

/** Line counts against the version before, in profit and loss colours. */
function LineStats({ added, removed }: { added: number | null; removed: number | null }) {
  const locale = useLocale();
  if (added === null || removed === null) return null;
  return (
    <span className="drive-record-stats">
      <span className="is-added">+{integer(added, locale)}</span>
      <span className="is-removed">{'−'}{integer(removed, locale)}</span>
    </span>
  );
}

function statusWord(t: TFunction, status: DriveMemoStatus | null): string {
  return status ? t(`drive:memo.status.${status}`) : t('drive:memo.status.none');
}

/** What an event did, as a sentence, and the muted tag that says what it touched. */
function eventLine(t: TFunction, e: DriveRecordEvent, locale: string): { text: string; tag: string } {
  const who = whoLabel(t, e.actor);
  switch (e.kind) {
    case 'mark': {
      let marked = t('drive:record.marked', { who });
      if (e.status_after !== null) {
        const status = statusWord(t, e.status_after);
        marked = e.final_seq !== null
          ? t('drive:record.markedStatusPinned', { who, status, seq: e.final_seq })
          : t('drive:record.markedStatus', { who, status });
      }
      const parts = [marked];
      if (e.tickers_after.length) parts.push(t('drive:record.alsoTickers', { tickers: formatUnitList(e.tickers_after, locale) }));
      return { text: parts.join(t('drive:record.and')), tag: t('drive:record.tags.memo') };
    }
    case 'unmark':
      return { text: t('drive:record.unmarked', { who }), tag: t('drive:record.tags.memo') };
    case 'clear':
      return { text: t('drive:record.cleared', { who, count: e.cleared }), tag: t('drive:record.tags.history') };
    case 'update': {
      const parts: string[] = [];
      let tag = t('drive:record.tags.status');
      if (e.status_before !== e.status_after) {
        parts.push(e.final_seq !== null
          ? t('drive:record.statusPinned', { who, from: statusWord(t, e.status_before), to: statusWord(t, e.status_after), seq: e.final_seq })
          : t('drive:record.status', { who, from: statusWord(t, e.status_before), to: statusWord(t, e.status_after) }));
      } else if (e.final_seq !== null) {
        parts.push(t('drive:record.repinned', { who, seq: e.final_seq }));
      }
      const before = e.tickers_before;
      const after = e.tickers_after;
      if (before.join() !== after.join()) {
        const tickers = formatUnitList(after, locale);
        // A second change in one event is told without its actor again.
        if (parts.length) {
          parts.push(after.length ? t('drive:record.alsoTickers', { tickers }) : t('drive:record.alsoTickersCleared'));
        } else {
          parts.push(after.length ? t('drive:record.tickers', { who, tickers }) : t('drive:record.tickersCleared', { who }));
          tag = t('drive:record.tags.tickers');
        }
      }
      return { text: parts.join(t('drive:record.and')), tag };
    }
  }
}

/** Saying why a version was made, where the person who made it had not. */
function AddWhy({ entry, seq }: { entry: DriveEntry; seq: number }) {
  const { t } = useTranslation();
  const { actions } = useDriveContext();
  const [open, setOpen] = useState(false);
  const [note, setNote] = useState('');
  if (!open) {
    return <button type="button" className="drive-link-button drive-record-addwhy tap-target" onClick={() => setOpen(true)}>{t('drive:record.addWhy')}</button>;
  }
  const submit = async () => {
    if (!note.trim()) return;
    if (await actions.explain(entry, seq, note.trim())) setOpen(false);
  };
  return (
    <form
      className="drive-record-whyform"
      onSubmit={(e) => {
        e.preventDefault();
        void submit();
      }}
    >
      <Input
        value={note}
        onChange={(e) => setNote(e.target.value)}
        maxLength={500}
        placeholder={t('drive:memo.whyPlaceholder')}
        aria-label={t('drive:record.addWhy')}
        autoFocus
        className="h-7 text-xs"
      />
      <HeaderButton type="submit" disabled={!note.trim() || actions.pending.explain}>{t('common.save')}</HeaderButton>
      <HeaderButton variant="ghost" onClick={() => setOpen(false)}>{t('common.cancel')}</HeaderButton>
    </form>
  );
}

/** A record time against now, as the chat shows times. */
function recordTime(iso: string, locale: string, now: number): string {
  return chatTime(new Date(iso).getTime(), locale, now);
}

/** "Current" and "Final" on the row that holds the head or the pinned final. */
function VersionTags({ entry, seqs }: { entry: DriveEntry; seqs: readonly number[] }) {
  const { t } = useTranslation();
  const state = memoState(entry);
  const final = state.kind === 'plain' ? null : pinnedSeq(state.mark);
  return (
    <>
      {entry.version !== null && seqs.includes(entry.version) && <span className="drive-tag">{t('drive:record.current')}</span>}
      {final !== null && seqs.includes(final) && <span className="drive-tag">{t('drive:memo.status.final')}</span>}
    </>
  );
}

/** A version's number and time, its tags and its lines, as the button that opens it. */
function VersionLine({ entry, v, onPick, title }: {
  entry: DriveEntry;
  v: DriveRecordVersion;
  onPick: (v: DriveVersion) => void;
  title?: string;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  return (
    <button type="button" className="drive-record-main tap-target" onClick={() => onPick(v)} title={title}>
      <span className="drive-record-top">
        <span className="drive-record-seq">{t('drive:record.versionAt', { seq: v.seq, when: recordTime(v.created_at, locale, now) })}</span>
        <VersionTags entry={entry} seqs={[v.seq]} />
        <LineStats added={v.added} removed={v.removed} />
      </span>
    </button>
  );
}

function VersionRow({ entry, v, onPick }: { entry: DriveEntry; v: DriveRecordVersion; onPick: (v: DriveVersion) => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  return (
    <li className="drive-record-item">
      <VersionLine entry={entry} v={v} onPick={onPick} />
      <span className="drive-record-who" title={formatTimestamp(v.created_at, locale)}>
        <Actor actor={v.actor} glyph thread />
        {v.restored_from !== null && <span className="drive-record-muted"> · {t('drive:record.restoredFrom', { seq: v.restored_from })}</span>}
      </span>
      {v.note
        ? <span className="drive-record-note">{v.note}</span>
        : v.actor.kind === 'person' && <AddWhy entry={entry} seq={v.seq} />}
    </li>
  );
}

/** Whether two makers are one: the same kind, person and agent. */
function sameMaker(a: DriveModifiedBy, b: DriveModifiedBy): boolean {
  return a.kind === b.kind && a.user_id === b.user_id && a.agent === b.agent;
}

/**
 * One version inside an opened change: its number, time and lines, and who
 * made it only where that is not who made the change. Picking it opens it as
 * a version row would.
 */
function ChangeVersionRow({ entry, v, lead, onPick }: {
  entry: DriveEntry;
  v: DriveRecordVersion;
  lead: DriveRecordVersion;
  onPick: (v: DriveVersion) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const otherMaker = !sameMaker(v.actor, lead.actor);
  return (
    <li className="drive-record-subitem">
      <VersionLine entry={entry} v={v} onPick={onPick} title={formatTimestamp(v.created_at, locale)} />
      {(otherMaker || v.restored_from !== null) && (
        <span className="drive-record-who">
          {otherMaker && <Actor actor={v.actor} glyph />}
          {v.restored_from !== null && (
            <span className="drive-record-muted">{otherMaker ? ' · ' : ''}{t('drive:record.restoredFrom', { seq: v.restored_from })}</span>
          )}
        </span>
      )}
    </li>
  );
}

/**
 * Versions an agent saved under one check-out, as one row: the range, when,
 * who and in which chat, the reason once, the lines summed; it opens to its
 * versions, newest first.
 */
function ChangeRow({ entry, versions, onPick }: {
  entry: DriveEntry;
  /** Two or more, newest first. */
  versions: DriveRecordVersion[];
  onPick: (v: DriveVersion) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const [open, setOpen] = useState(false);
  const listId = useId();
  const newest = versions[0];
  const oldest = versions[versions.length - 1];
  // Within one day the row reads as a version row does, at the newest
  // version's time; across days it gives the days alone.
  const days = dayRangeEnds(new Date(oldest.created_at).getTime(), new Date(newest.created_at).getTime(), locale);
  const when = days ? t('drive:record.dateRange', days) : recordTime(newest.created_at, locale, now);
  const { added, removed } = changeStats(versions);
  const note = versions.find((v) => v.note)?.note ?? null;
  const count = versions.length;
  return (
    <li className="drive-record-item">
      <span className="drive-record-top">
        <span className="drive-record-seq">{t('drive:record.changeAt', { from: oldest.seq, to: newest.seq, when })}</span>
        <VersionTags entry={entry} seqs={versions.map((v) => v.seq)} />
        <LineStats added={added} removed={removed} />
      </span>
      <span
        className="drive-record-who"
        title={t('drive:record.dateRange', { from: formatTimestamp(oldest.created_at, locale), to: formatTimestamp(newest.created_at, locale) })}
      >
        <Actor actor={newest.actor} glyph thread />
      </span>
      {note && <span className="drive-record-note">{note}</span>}
      <button
        type="button"
        className="drive-record-toggle tap-target"
        aria-expanded={open}
        aria-controls={open ? listId : undefined}
        aria-label={t('drive:record.changeVersions', { count })}
        onClick={() => setOpen((o) => !o)}
      >
        <ChevronRight className="drive-record-chevron" aria-hidden="true" />
        {t('drive:record.versionCount', { count })}
      </button>
      {open && (
        <ol id={listId} className="drive-record-sublist" aria-label={t('drive:record.changeVersions', { count })}>
          {versions.map((v) => <ChangeVersionRow key={v.seq} entry={entry} v={v} lead={newest} onPick={onPick} />)}
        </ol>
      )}
    </li>
  );
}

function EventRow({ e }: { e: DriveRecordEvent }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { text, tag } = eventLine(t, e, locale);
  return (
    <li className="drive-record-item is-event">
      <span className="drive-record-top">
        <span className="drive-record-event"><ActorGlyph kind={e.actor.kind} />{text}</span>
        <span className="drive-record-when" title={formatTimestamp(e.created_at, locale)}>{recordTime(e.created_at, locale, now)}</span>
        <span className="drive-record-tag">{tag}</span>
      </span>
      {e.actor.thread_id && (
        <span className="drive-record-who">
          <ActorSentence actor={e.actor} threadId={e.actor.thread_id} i18nKey="drive:record.inThread" />
        </span>
      )}
      {e.note && <span className="drive-record-note">{e.note}</span>}
    </li>
  );
}

/**
 * The record's own menu: clearing the earlier versions for good. Only the
 * person reaches it (the server refuses the agent and a computer's token),
 * and it is offered only while there is an earlier version to clear.
 */
function RecordMenu({ entry }: { entry: DriveEntry }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { space, actions } = useDriveContext();
  // Held from when the dialog opened, so a refetch while it is open cannot
  // move the cutoff the person agreed to or the numbers they read; kept past
  // close so the text holds through the closing animation.
  const [held, setHeld] = useState<{ shown: DriveClearShown; older: number; bytes: number; keptFinal: number | null } | null>(null);
  const [confirming, setConfirming] = useState(false);
  const sending = useRef(false);
  const listing = useDriveVersions(space, entry.id);
  // What a clear drops is the server's to count, by the rule the clear runs.
  // The count, the head and the pinned final are read off that one answer,
  // and the clear is bound to them: a save or a status change landing since
  // is never part of what the person agreed to.
  const older = listing.data?.clearable ?? 0;
  if (!confirming && (!listing.data || older === 0)) return null;
  const ask = () => {
    if (!listing.data) return;
    const head = listing.data.versions[0]?.seq ?? null;
    const mark = listing.data.memo ?? null;
    setHeld({
      shown: { head, final: mark ? pinnedSeq(mark) : null },
      older,
      bytes: listing.data.history_bytes,
      keptFinal: mark ? revisedSince(mark, head) : null,
    });
    setConfirming(true);
  };
  const size = held ? formatBytes(held.bytes, locale) : '';
  return (
    <div className="drive-record-head">
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} aria-label={t('drive:record.menu')}>
            <MoreHorizontal className="h-4 w-4" aria-hidden="true" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          <DropdownMenuItem variant="destructive" onSelect={ask}>
            <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
            {t('drive:record.clear')}
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
      <ConfirmDialog
        open={confirming}
        title={t('drive:record.clearTitle', { name: entry.name })}
        message={!held ? '' : held.keptFinal !== null
          ? t('drive:record.clearConfirmFinal', { count: held.older, size, seq: held.keptFinal })
          : t('drive:record.clearConfirm', { count: held.older, size })}
        confirmLabel={t('drive:record.clearSubmit')}
        surface="page"
        onConfirm={async () => {
          // A second click before the first answers is not a second clear.
          if (sending.current || !held) return;
          sending.current = true;
          try {
            if (await actions.clearHistory(entry, held.shown)) setConfirming(false);
          } finally {
            sending.current = false;
          }
        }}
        onOpenChange={(open) => {
          if (!open && !actions.pending.clearHistory) setConfirming(false);
        }}
      />
    </div>
  );
}

/**
 * A file's record, newest first: each version with who made it and why and
 * its lines added and removed, an agent's change of several versions as one
 * row, and between them the changes to its memo mark.
 * Picking a version hands it to `onPick`, whose pane decides how it opens.
 */
export function MemoRecord({ entry, onPick }: {
  entry: DriveEntry;
  onPick: (v: DriveVersion) => void;
}) {
  const { t } = useTranslation();
  const { space } = useDriveContext();
  const record = useDriveRecord(space, entry.id);
  const pages = record.data?.pages ?? [];
  const items = pages.flatMap((p) => p.items);
  // The newest page the person asked for: the first, then each one "Show
  // older" reads. Held by the file it was asked on, so another file starts
  // from its first page; a counting reread keeps the pages and so this.
  const [asked, setAsked] = useState({ entryId: entry.id, page: 0 });
  const askedPage = asked.entryId === entry.id ? asked.page : 0;
  // A change the asked page ends inside is read on to its first version, a
  // page at a time, never while another read of the record is under way (a
  // counting read included) and never again after one failed, until a read
  // succeeds. Each page that arrives is looked at again.
  const askedAt = askedPage < pages.length
    ? pages.slice(0, askedPage + 1).reduce((n, p) => n + p.items.length, 0) - 1
    : -1;
  const { cut, readOn } = recordEnd(items, record.hasNextPage, askedAt);
  const readNow = readOn && !record.isFetching && !record.isError;
  const { fetchNextPage } = record;
  const loaded = pages.length;
  useEffect(() => {
    if (readNow) void fetchNextPage({ cancelRefetch: false });
  }, [readNow, loaded, fetchNextPage]);
  if (record.error && !record.data) return <PaneProblem line={loadFailedLine(record.error, t)} onRetry={() => void record.refetch()} />;
  if (!record.data) return <div className="drive-pane-pad" aria-busy="true"><ListSkeleton rows={5} /></div>;
  // Grouped over every page loaded so far, so a change that straddles a
  // page boundary joins up when the older page arrives. Until it has, its
  // row is held back, since its range, count and lines would be part of it;
  // one not being read on for waits for "Show older" to bring the rest.
  const rows = recordRows(items);
  if (cut) rows.pop();
  return (
    <div className="drive-record">
      {/* The pane is held by path, so another file can arrive at the same
          one; a confirmation held for the first must not clear the second. */}
      <RecordMenu key={`${space}:${entry.id}`} entry={entry} />
      <ol className="drive-record-list" aria-label={t('drive:memo.tabs.record')}>
        {rows.map((row) => {
          switch (row.kind) {
            case 'version': return <VersionRow key={`v${row.version.seq}`} entry={entry} v={row.version} onPick={onPick} />;
            // Keyed by its newest version, which an older page joining it leaves in place.
            case 'change': return <ChangeRow key={`c${row.versions[0].seq}`} entry={entry} versions={row.versions} onPick={onPick} />;
            case 'event': return <EventRow key={`e${row.event.id}`} e={row.event} />;
          }
        })}
      </ol>
      {readOn && !record.isError && <div aria-busy="true"><ListSkeleton rows={1} /></div>}
      {record.hasNextPage && (
        <div className="drive-more">
          {record.isFetchNextPageError && <span role="alert">{loadFailedLine(record.error, t)}</span>}
          <HeaderButton
            onClick={() => {
              setAsked({ entryId: entry.id, page: loaded });
              void record.fetchNextPage();
            }}
            disabled={record.isFetchingNextPage}
          >
            {t('drive:record.older')}
          </HeaderButton>
        </div>
      )}
    </div>
  );
}
