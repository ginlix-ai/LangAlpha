import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { Textarea } from '@/components/ui/textarea';
import type { MemoChange } from '@/api/drive';
import type { DriveEntry, DriveMemoStatus } from '@/types/drive';
import { MAX_TICKERS, memoState, parseTickers, sameTickers } from '../memoMarks';
// The page's ground, as the Drive's other dialogs draw it.
import '@/pages/Dashboard/components/dashForms.css';

/** What the memo dialog is asked for: marking a file, changing a memo's details, or taking the mark off. */
export type MemoAsk = { kind: 'mark' | 'edit' | 'unmark'; entry: DriveEntry };

type StatusChoice = DriveMemoStatus | 'none';

const NOTE_MAX = 500;

const COPY: Record<MemoAsk['kind'], { title: string; body: string; submit: string }> = {
  mark: { title: 'drive:memo.dialog.markTitle', body: 'drive:memo.dialog.markBody', submit: 'drive:actions.markMemo' },
  edit: { title: 'drive:memo.dialog.editTitle', body: 'drive:memo.dialog.editBody', submit: 'common.save' },
  unmark: { title: 'drive:memo.dialog.unmarkTitle', body: 'drive:memo.dialog.unmarkBody', submit: 'drive:memo.dialog.unmarkSubmit' },
};

/**
 * The memo mark of one file: status, tickers and an optional reason, kept in
 * the memo's record. A change sends only what changed, so a status left alone
 * is not recorded again.
 */
export function MemoMarkDialog({ ask, onClose, onSubmit }: {
  ask: MemoAsk | null;
  onClose: () => void;
  /** Answers whether it went; a refusal has had its toast. */
  onSubmit: (entry: DriveEntry, change: MemoChange) => Promise<boolean>;
}) {
  const { t } = useTranslation();
  // Keyed by the caller per ask, so every field starts from the file it is for.
  // A former memo's mark is where re-marking starts.
  const state = memoState(ask?.entry);
  const mark = state.kind === 'plain' ? null : state.mark;
  const [kind, setKind] = useState(ask?.kind ?? 'mark');
  const startStatus: StatusChoice = kind === 'mark' ? (mark?.status ?? 'draft') : (mark?.status ?? 'none');
  const [status, setStatus] = useState<StatusChoice>(startStatus);
  const [typed, setTyped] = useState((mark?.tickers ?? []).join(', '));
  const [note, setNote] = useState('');
  const [busy, setBusy] = useState(false);
  const parsed = parseTickers(typed);
  const tickerError = parsed.bad.length
    ? t('drive:memo.dialog.badTickers', { tickers: parsed.bad.join(', ') })
    : parsed.tooMany
      ? t('drive:memo.dialog.tooManyTickers', { max: MAX_TICKERS })
      : null;

  const change = (): MemoChange | null => {
    const why = note.trim() || null;
    if (kind === 'unmark') return { memo: false, note: why };
    const next = status === 'none' ? null : status;
    // Only what differs from the mark as it stands is sent, on a mark as on
    // an edit: the server pins the head again on every `final` it receives,
    // so re-marking a former memo that was final would move its pin.
    const out: MemoChange = kind === 'mark' ? { memo: true, note: why } : { note: why };
    if (next !== (mark?.status ?? null)) {
      out.status = next;
      // The head the person saw: a save since and the server refuses (`changed`).
      if (ask?.entry.sha256) out.ifMatch = ask.entry.sha256;
    }
    // Emptied, the field sends no tickers at all: `[]`.
    if (!sameTickers(parsed.tickers, mark?.tickers ?? [])) out.tickers = parsed.tickers;
    return kind === 'mark' || out.status !== undefined || out.tickers !== undefined ? out : null;
  };

  const submit = async () => {
    if (!ask || busy || (kind !== 'unmark' && tickerError)) return;
    const next = change();
    if (!next) {
      onClose();
      return;
    }
    setBusy(true);
    const ok = await onSubmit(ask.entry, next);
    setBusy(false);
    if (ok) onClose();
  };

  const name = ask?.entry.name ?? '';
  const copy = COPY[kind];

  return (
    <Dialog open={!!ask} onOpenChange={(open) => { if (!open && !busy) onClose(); }}>
      <DialogContent className="sm:max-w-md border dash-form">
        <DialogHeader>
          <DialogTitle>{t(copy.title, { name })}</DialogTitle>
          <DialogDescription>{t(copy.body)}</DialogDescription>
        </DialogHeader>
        <form
          className="drive-memo-form"
          onSubmit={(e) => {
            e.preventDefault();
            void submit();
          }}
        >
          {kind !== 'unmark' && (
            <>
              <div className="drive-memo-field">
                <span id="drive-memo-status" className="drive-memo-label">{t('drive:memo.dialog.status')}</span>
                <SegmentedControl<StatusChoice>
                  value={status}
                  onChange={setStatus}
                  labelledBy="drive-memo-status"
                  disabled={busy}
                  options={[
                    { value: 'none', label: t('drive:memo.status.none') },
                    { value: 'draft', label: t('drive:memo.status.draft') },
                    { value: 'final', label: t('drive:memo.status.final') },
                  ]}
                />
              </div>
              <label className="drive-memo-field">
                <span className="drive-memo-label">{t('drive:memo.dialog.tickers')}</span>
                <Input
                  value={typed}
                  onChange={(e) => setTyped(e.target.value)}
                  placeholder={t('drive:memo.dialog.tickersPlaceholder')}
                  className="drive-memo-tickers-input"
                  aria-invalid={!!tickerError || undefined}
                  aria-describedby={tickerError ? 'drive-memo-ticker-error' : undefined}
                  autoFocus={kind === 'mark'}
                />
                {tickerError && <span id="drive-memo-ticker-error" role="alert" className="drive-memo-error">{tickerError}</span>}
              </label>
            </>
          )}
          <label className="drive-memo-field">
            <span className="drive-memo-label">{t('drive:memo.dialog.why')}</span>
            <Textarea
              value={note}
              onChange={(e) => setNote(e.target.value)}
              maxLength={NOTE_MAX}
              rows={2}
              placeholder={t('drive:memo.whyPlaceholder')}
              className="min-h-[56px]"
              autoFocus={kind === 'unmark'}
            />
          </label>
        </form>
        <DialogFooter className="drive-memo-foot">
          {kind === 'edit' && (
            <Button variant="ghost" className="drive-memo-unmark" onClick={() => setKind('unmark')} disabled={busy}>
              {t('drive:actions.unmarkMemo')}
            </Button>
          )}
          <Button variant="ghost" onClick={onClose} disabled={busy}>{t('common.cancel')}</Button>
          <Button onClick={() => void submit()} disabled={busy || (kind !== 'unmark' && !!tickerError)}>
            {busy ? t('common.saving') : t(copy.submit)}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
