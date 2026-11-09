import React, { useState } from 'react';
import { randomUUID } from '@/lib/randomUUID';
import { useTranslation } from 'react-i18next';
import { Info } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { toast } from '@/components/ui/use-toast';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { chatTime } from '@/lib/format';
import ThumbDownModal from './ThumbDownModal';
import { ISSUE_CATEGORIES } from './thumbDownCategories';
import { useReportsState } from '@/hooks/useReportsEnabled';
import { refusalMessage } from '@/components/report/refusalMessage';
import type { ThumbDownFollowUp, ThumbDownFollowUpOutcome } from '../hooks/useChatFeedback';
import type { FeedbackResult } from './messageList/types';

type FollowUp = (action: ThumbDownFollowUp) => Promise<ThumbDownFollowUpOutcome>;

interface ThumbDownSentDialogProps {
  feedback: FeedbackResult;
  onClose: () => void;
  onFollowUp: FollowUp;
  onWithdrawn: () => void;
}

const CATEGORY_KEY = new Map(ISSUE_CATEGORIES.map((c) => [c.value, c.key]));

type Phase = 'view' | 'sending' | 'confirmWithdraw' | 'withdrawing';

/**
 * A thumbs-down already sent to the team, reopened: what the team has, read
 * only, with room to add a note or ask for a human review, and a way to
 * withdraw it. Nothing sent is replaced, so what the user sees is what the
 * team has. Mounted only while open, so nothing here outlives a close.
 */
function ThumbDownSentDialog({ feedback, onClose, onFollowUp, onWithdrawn }: ThumbDownSentDialogProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const [note, setNote] = useState('');
  // One id per note text, kept across a retry so the note is posted once; an
  // edit is a new note.
  const [noteId, setNoteId] = useState(() => randomUUID());
  const [review, setReview] = useState(false);
  const [phase, setPhase] = useState<Phase>('view');
  const [problem, setProblem] = useState<string | null>(null);

  const reported = !!feedback.reported_at;
  const reviewRequested = !!feedback.consent_human_review;
  const busy = phase === 'sending' || phase === 'withdrawing';
  const canSend = !busy && (!reported || note.trim().length > 0 || review);
  const when = (ts?: string | null): string => {
    const ms = ts ? Date.parse(ts) : NaN;
    return Number.isNaN(ms) ? '' : chatTime(ms, locale, now);
  };

  const send = async (): Promise<void> => {
    if (!canSend) return;
    setPhase('sending');
    setProblem(null);
    const text = note.trim();
    const outcome = await onFollowUp({ kind: 'add', note: text ? { id: noteId, text } : null, humanReview: review });
    if (outcome.status !== 'ok') {
      setPhase('view');
      setProblem(refusalMessage(t, outcome));
      return;
    }
    toast({ description: review ? t('report.followUp.reviewSent') : t('report.followUp.added') });
    onClose();
  };

  const withdraw = async (): Promise<void> => {
    setPhase('withdrawing');
    setProblem(null);
    const outcome = await onFollowUp({ kind: 'withdraw' });
    if (outcome.status !== 'ok' && outcome.status !== 'gone') {
      setPhase('confirmWithdraw');
      setProblem(refusalMessage(t, outcome));
      return;
    }
    toast({ description: t('report.followUp.withdrawn') });
    onWithdrawn();
  };

  const close = (): void => {
    if (!busy) onClose();
  };

  const categories = feedback.issue_categories ?? [];
  const followUps = feedback.report_follow_ups ?? [];
  const confirming = phase === 'confirmWithdraw' || phase === 'withdrawing';

  return (
    <Dialog open onOpenChange={(open) => { if (!open) close(); }}>
      <DialogContent
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        <DialogHeader>
          <DialogTitle className="text-base" style={{ color: 'var(--color-text-primary)' }}>
            {confirming ? t('report.followUp.withdrawTitle') : t('report.followUp.title')}
          </DialogTitle>
          <DialogDescription className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {confirming
              ? t('report.followUp.withdrawBody')
              : reported
                ? t('report.followUp.sentAt', { time: when(feedback.reported_at) })
                : t('report.followUp.notSent')}
          </DialogDescription>
        </DialogHeader>

        {!confirming && (
          <div className="flex flex-col gap-3">
            {categories.length > 0 && (
              <div className="flex flex-wrap gap-2">
                {categories.map((c) => (
                  <span
                    key={c}
                    className="px-3 py-1 rounded-full text-xs"
                    style={{ color: 'var(--color-text-primary)', border: '1px solid var(--color-border-default)', backgroundColor: 'var(--color-bg-surface)' }}
                  >
                    {CATEGORY_KEY.has(c) ? t(`report.thumbs.categories.${CATEGORY_KEY.get(c)}`) : c}
                  </span>
                ))}
              </div>
            )}
            {feedback.comment && (
              <p className="text-sm wrap-break-word" style={{ color: 'var(--color-text-primary)' }}>{feedback.comment}</p>
            )}
            {followUps.map((f, i) => (
              <div key={`${f.at}-${i}`} className="text-sm wrap-break-word">
                <span className="text-xs mr-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('report.followUp.addedAt', { time: when(f.at) })}
                </span>
                <span style={{ color: 'var(--color-text-primary)' }}>{f.note}</span>
              </div>
            ))}

            <label className="flex flex-col gap-2 mt-1">
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                {t('report.followUp.addLabel')}
              </span>
              <textarea
                value={note}
                onChange={(e) => {
                  setNote(e.target.value);
                  setNoteId(randomUUID());
                }}
                maxLength={4000}
                rows={3}
                disabled={busy}
                placeholder={t('report.followUp.addPlaceholder')}
                className="w-full rounded-md px-3 py-2 text-sm resize-none"
                style={{
                  backgroundColor: 'var(--color-bg-input)',
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-muted)',
                }}
              />
            </label>

            {reviewRequested ? (
              <p className="flex items-start gap-1.5 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                <Info className="h-3.5 w-3.5 shrink-0 mt-px" aria-hidden="true" />
                {t('report.followUp.reviewRequested')}
              </p>
            ) : (
              <label
                className="rounded-lg pl-2.5 pr-3 py-2.5 cursor-pointer flex flex-col gap-1.5"
                style={{
                  backgroundColor: 'var(--color-bg-surface)',
                  border: `1px solid ${review ? 'var(--color-border-elevated)' : 'var(--color-border-muted)'}`,
                }}
              >
                <span className="flex items-center gap-2.5">
                  <input
                    type="checkbox"
                    checked={review}
                    disabled={busy}
                    onChange={(e) => setReview(e.target.checked)}
                    className="h-4 w-4 shrink-0"
                  />
                  <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                    {t('report.thumbs.reviewTitle')}
                  </span>
                </span>
                <span className="text-xs flex items-start gap-1" style={{ color: 'var(--color-text-tertiary)' }}>
                  <Info className="h-3 w-3 shrink-0 mt-0.5" aria-hidden="true" />
                  {t('report.thumbs.reviewBody')}
                </span>
              </label>
            )}
          </div>
        )}

        {problem && (
          <p className="text-xs" role="alert" style={{ color: 'var(--color-loss)' }}>{problem}</p>
        )}

        <DialogFooter className="flex-row items-center gap-2 sm:gap-2">
          {confirming ? (
            <>
              <span className="flex-1" />
              <Button variant="ghost" onClick={() => { setPhase('view'); setProblem(null); }} disabled={busy}>
                {t('report.followUp.keep')}
              </Button>
              <Button variant="destructive" onClick={() => void withdraw()} disabled={busy}>
                {t('report.followUp.withdrawConfirm')}
              </Button>
            </>
          ) : (
            <>
              <button
                type="button"
                onClick={() => { setPhase('confirmWithdraw'); setProblem(null); }}
                disabled={busy}
                className="text-xs underline underline-offset-2 mr-auto disabled:opacity-50"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                {t('report.followUp.withdraw')}
              </button>
              <Button variant="ghost" onClick={close} disabled={busy}>
                {t('common.close')}
              </Button>
              <Button onClick={() => void send()} disabled={!canSend}>
                {phase === 'sending' ? t('report.sending') : reported ? t('report.followUp.send') : t('report.followUp.sendAgain')}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ThumbDownSentDialog;

interface ThumbDownDialogProps {
  /** The thread's ratings are still loading, so ``sent`` is not yet known. */
  loading?: boolean;
  /** The rating when this turn already has a thumbs-down, else null. */
  sent: FeedbackResult | null;
  onSubmit: (issueCategories: string[], comment: string | null, consentHumanReview: boolean) => void;
  onCancel: () => void;
  onFollowUp?: FollowUp;
  onWithdrawn: () => void;
}

/**
 * The dialog a thumbs-down click opens: the form for a new rating, or what
 * was sent for one the team already has. For a thumbs-down the team never
 * got (reports off here, or a rating from before reports), the form as
 * before; sending it again gives that rating its report. Mounted only while open, so
 * the bubbles of a long thread don't each read the reports flag.
 */
export function ThumbDownDialog({ loading, sent, onSubmit, onCancel, onFollowUp, onWithdrawn }: ThumbDownDialogProps): React.ReactElement | null {
  const reports = useReportsState();
  // Opens once the turn's rating is known: a rated turn reads as unrated until
  // then, and the form would take edits a sent report never gets.
  if (loading) return null;
  // A rating with a report opens as sent, unless this deployment is known to
  // take none and the team never got it. That holds while the setting is not
  // known (loading, or unreadable), since the form would take edits a sent
  // report never gets, and the server refuses the sent dialog's actions if
  // reports are off.
  if (sent?.report_id && onFollowUp && (reports !== false || sent.reported_at)) {
    return (
      <ThumbDownSentDialog feedback={sent} onClose={onCancel} onFollowUp={onFollowUp} onWithdrawn={onWithdrawn} />
    );
  }
  return <ThumbDownModal isOpen onSubmit={onSubmit} onCancel={onCancel} />;
}
