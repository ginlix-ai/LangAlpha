import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Info } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { ISSUE_CATEGORIES } from './thumbDownCategories';
import { useReportsState } from '@/hooks/useReportsEnabled';


interface ThumbDownModalProps {
  isOpen: boolean;
  onSubmit: (issueCategories: string[], comment: string | null, consentHumanReview: boolean) => void;
  onCancel: () => void;
}

/**
 * ThumbDownModal Component
 *
 * Feedback modal for reporting issues with assistant responses.
 * Allows selecting issue categories, adding comments, and
 * optionally consenting to anonymous human review for credit refund.
 */
function ThumbDownModal({ isOpen, onSubmit, onCancel }: ThumbDownModalProps): React.ReactElement {
  const { t } = useTranslation();
  // The server sends a thumbs-down to the team whenever it takes reports, so
  // the dialog says so unless this deployment is known not to. A review is
  // offered only where it is known to reach someone.
  const reports = useReportsState();
  const [selectedCategories, setSelectedCategories] = useState<Set<string>>(new Set());
  const [comment, setComment] = useState('');
  const [consentHumanReview, setConsentHumanReview] = useState(false);

  const toggleCategory = (cat: string): void => {
    setSelectedCategories(prev => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  const handleSubmit = (): void => {
    if (selectedCategories.size === 0) return;
    onSubmit(
      Array.from(selectedCategories),
      comment.trim() || null,
      reports === true && consentHumanReview,
    );
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => { if (!open) onCancel(); }}>
      <DialogContent
        // The shadow is a class, not inline, so the card styles can lay the
        // dialog flat (styles/cardStyle.css).
        className="shadow-[0_20px_60px_rgba(0,0,0,0.5),0_0_0_1px_rgba(255,255,255,0.06)] [&>button:last-child]:hidden"
        style={{
          backgroundColor: 'var(--color-shell-float, var(--color-bg-elevated))',
          borderColor: 'var(--color-shell-edge, var(--color-border-default))',
        }}
      >
        <DialogHeader>
          <DialogTitle className="text-base" style={{ color: 'var(--color-text-primary)' }}>
            {t('report.thumbs.title')}
          </DialogTitle>
          <DialogDescription className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t(reports === false ? 'report.thumbs.descriptionLocal' : 'report.thumbs.description')}
          </DialogDescription>
        </DialogHeader>

        {/* Issue categories */}
        <p className="text-sm font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
          {t('report.thumbs.whatWentWrong')}
        </p>
        <div className="flex flex-wrap gap-2 mb-2">
          {ISSUE_CATEGORIES.map(({ value: cat, key }) => {
            const selected = selectedCategories.has(cat);
            return (
              <button
                key={cat}
                onClick={() => toggleCategory(cat)}
                className="px-3 py-1.5 rounded-full text-xs font-medium transition-colors"
                style={{
                  backgroundColor: selected ? 'var(--color-accent-soft)' : 'transparent',
                  color: selected ? 'var(--color-accent-primary)' : 'var(--color-text-secondary)',
                  border: `1px solid ${selected ? 'var(--color-accent-primary)' : 'var(--color-border-muted)'}`,
                }}
              >
                {t(`report.thumbs.categories.${key}`)}
              </button>
            );
          })}
        </div>

        {/* Comment */}
        <p className="text-sm mb-1" style={{ color: 'var(--color-text-secondary)' }}>
          {t('report.thumbs.commentLabel')}
        </p>
        <textarea
          value={comment}
          onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setComment(e.target.value)}
          maxLength={4000}
          placeholder={t('report.thumbs.commentPlaceholder')}
          rows={3}
          className="w-full rounded-lg px-3 py-2.5 text-sm resize-none mb-2"
          style={{
            backgroundColor: 'var(--color-bg-page)',
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-muted)',
          }}
        />

        {/* Human review consent, only where a report reaches the team: elsewhere
            nobody would review it. */}
        {reports === true && (
          <div
            className="rounded-lg pl-2.5 pr-3 py-2.5 cursor-pointer"
            style={{
              backgroundColor: 'var(--color-bg-surface)',
              border: `1px solid ${consentHumanReview ? 'var(--color-border-elevated)' : 'var(--color-border-muted)'}`,
            }}
            onClick={() => setConsentHumanReview(!consentHumanReview)}
          >
            <div className="flex items-center gap-2.5">
              <div
                className="w-4 h-4 rounded shrink-0 flex items-center justify-center"
                style={{
                  border: `1.5px solid ${consentHumanReview ? 'var(--color-accent-primary)' : 'var(--color-border-muted)'}`,
                  backgroundColor: consentHumanReview ? 'var(--color-accent-primary)' : 'transparent',
                }}
              >
                {consentHumanReview && (
                  <svg width="10" height="8" viewBox="0 0 10 8" fill="none">
                    <path d="M1 4L3.5 6.5L9 1" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
              )}
            </div>
            <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
              {t('report.thumbs.reviewTitle')}
            </span>
          </div>
          <p className="text-xs mt-1.5 flex items-start gap-1" style={{ color: 'var(--color-text-tertiary)' }}>
            <Info className="h-3 w-3 shrink-0 mt-0.5" />
            {t('report.thumbs.reviewBody')}
          </p>
        </div>
        )}

        {/* Action buttons */}
        <DialogFooter className="flex-row justify-end gap-3 sm:gap-3">
          <button
            onClick={onCancel}
            className="px-5 py-2 rounded-lg text-sm font-medium transition-colors"
            style={{
              color: 'var(--color-text-secondary)',
              border: '1px solid var(--color-border-muted)',
            }}
          >
            {t('common.cancel')}
          </button>
          <button
            onClick={handleSubmit}
            disabled={selectedCategories.size === 0}
            className="px-5 py-2 rounded-lg text-sm font-medium transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
            style={{
              backgroundColor: 'var(--color-loss)',
              color: 'var(--color-text-on-accent)',
            }}
          >
            {t('report.thumbs.submit')}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ThumbDownModal;
