import React, { useId, useState } from 'react';
import { randomUUID } from '@/lib/randomUUID';
import { useTranslation } from 'react-i18next';
import { CheckCircle2, ShieldCheck, ThumbsDown } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { sendReport, type ReportCategory, type ReportDraft, type ReportRefusal } from '@/api/reports';
import { refusalMessage } from './refusalMessage';

export type ReportSubject = Pick<ReportDraft, 'event' | 'source' | 'threadId' | 'runId' | 'error'>;

interface ReportDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  subject: ReportSubject;
}

type Phase = 'editing' | 'sending' | 'accepted' | 'duplicate';

const CATEGORIES: ReportCategory[] = ['bug', 'feedback', 'billing', 'other'];

/**
 * One report, from the account menu or a failed turn's banner. It says what
 * is sent before it is sent: the user's note and the ids our engineers need,
 * never the conversation.
 */
export function ReportDialog({ open, onOpenChange, subject }: ReportDialogProps): React.ReactElement {
  const { t } = useTranslation();
  const labelId = useId();
  const isError = subject.event === 'error_report';
  const [category, setCategory] = useState<ReportCategory>('bug');
  const [note, setNote] = useState('');
  const [phase, setPhase] = useState<Phase>('editing');
  const [refusal, setRefusal] = useState<ReportRefusal | null>(null);
  // One id per report as written, kept across a retry so the platform posts
  // it once; an edit is a new report, since the platform keeps the first body
  // it got under an id and a send that timed out may have landed.
  const [reportId, setReportId] = useState(() => randomUUID());
  const editCategory = (next: ReportCategory): void => {
    setCategory(next);
    setReportId(randomUUID());
  };
  const editNote = (next: string): void => {
    setNote(next);
    setReportId(randomUUID());
  };

  const done = phase === 'accepted' || phase === 'duplicate';
  const canSend = phase !== 'sending' && (isError || note.trim().length > 0);

  const reset = (): void => {
    setCategory('bug');
    setNote('');
    setPhase('editing');
    setRefusal(null);
    setReportId(randomUUID());
  };

  const handleOpenChange = (next: boolean): void => {
    if (phase === 'sending') return;
    if (!next && done) reset();
    onOpenChange(next);
  };

  const submit = async (): Promise<void> => {
    if (!canSend) return;
    setPhase('sending');
    setRefusal(null);
    const outcome = await sendReport(reportId, {
      ...subject,
      category: isError ? 'bug' : category,
      note,
    });
    if (outcome.status === 'accepted' || outcome.status === 'duplicate') {
      setPhase(outcome.status);
    } else {
      setPhase('editing');
      setRefusal(outcome);
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        <DialogHeader>
          <DialogTitle className="text-base" style={{ color: 'var(--color-text-primary)' }}>
            {isError ? t('report.titleError') : t('report.title')}
          </DialogTitle>
          <DialogDescription className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {isError ? t('report.descriptionError') : t('report.description')}
          </DialogDescription>
        </DialogHeader>

        {done ? (
          <div className="flex items-start gap-3 py-2" role="status">
            <CheckCircle2 className="h-5 w-5 shrink-0" style={{ color: 'var(--color-text-secondary)' }} aria-hidden="true" />
            <div className="flex flex-col gap-1">
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {phase === 'duplicate' ? t('report.duplicate') : t('report.sent')}
              </span>
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t('report.sentDetail')}
              </span>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            {!isError && (
              <div className="flex flex-col gap-2">
                <span id={labelId} className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                  {t('report.categoryLabel')}
                </span>
                <SegmentedControl
                  value={category}
                  onChange={editCategory}
                  labelledBy={labelId}
                  disabled={phase === 'sending'}
                  options={CATEGORIES.map((c) => ({ value: c, label: t(`report.category.${c}`) }))}
                />
              </div>
            )}
            {isError && subject.error && (
              <div
                className="rounded-md px-3 py-2 text-xs wrap-break-word"
                style={{ backgroundColor: 'var(--color-bg-page)', color: 'var(--color-text-secondary)' }}
              >
                {subject.error.message || subject.error.type}
              </div>
            )}
            <label className="flex flex-col gap-2">
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                {isError ? t('report.noteLabelOptional') : t('report.noteLabel')}
              </span>
              <textarea
                value={note}
                onChange={(e) => editNote(e.target.value)}
                maxLength={4000}
                rows={4}
                disabled={phase === 'sending'}
                placeholder={isError ? t('report.notePlaceholderError') : t('report.notePlaceholder')}
                className="w-full rounded-md px-3 py-2 text-sm resize-none"
                style={{
                  backgroundColor: 'var(--color-bg-input)',
                  color: 'var(--color-text-primary)',
                  border: '1px solid var(--color-border-muted)',
                }}
              />
            </label>
            {!isError && (
              <p className="flex items-start gap-1.5 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                <ThumbsDown className="h-3.5 w-3.5 shrink-0 mt-px" aria-hidden="true" />
                {t('report.thumbsHint')}
              </p>
            )}
            <p className="flex items-start gap-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              <ShieldCheck className="h-3.5 w-3.5 shrink-0 mt-px" aria-hidden="true" />
              {isError ? t('report.privacyError') : t('report.privacy')}
            </p>
            {refusal && (
              <p className="text-xs" role="alert" style={{ color: 'var(--color-loss)' }}>
                {refusalMessage(t, refusal)}
              </p>
            )}
          </div>
        )}

        <DialogFooter className="flex-row justify-end gap-2 sm:gap-2">
          {done ? (
            <Button onClick={() => handleOpenChange(false)}>{t('common.close')}</Button>
          ) : (
            <>
              <Button variant="ghost" onClick={() => handleOpenChange(false)} disabled={phase === 'sending'}>
                {t('common.cancel')}
              </Button>
              <Button onClick={() => void submit()} disabled={!canSend}>
                {phase === 'sending' ? t('report.sending') : t('report.send')}
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ReportDialog;
