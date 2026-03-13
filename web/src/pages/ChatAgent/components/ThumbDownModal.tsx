import React, { useState } from 'react';
import { Info, MessageSquareWarning } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';

const ISSUE_CATEGORIES = [
  'Incorrect data or numbers',
  'Wrong ticker or company',
  'Outdated information',
  'Flawed analysis',
  'Missing risk factors',
  'Code execution errors',
  'Didn\'t follow instructions',
  'Incomplete research',
];

interface ThumbDownModalProps {
  isOpen: boolean;
  onSubmit: (issueCategories: string[], comment: string | null, consentHumanReview: boolean) => void;
  onCancel: () => void;
  onReportWithAgent?: (instruction: string) => void;
}

/**
 * ThumbDownModal Component
 *
 * Feedback modal for reporting issues with assistant responses.
 * Allows selecting issue categories, adding comments, and
 * optionally consenting to anonymous human review for credit refund.
 */
function ThumbDownModal({ isOpen, onSubmit, onCancel, onReportWithAgent }: ThumbDownModalProps): React.ReactElement {
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
      consentHumanReview,
    );
  };

  return (
    <Dialog open={isOpen} onOpenChange={(open) => { if (!open) onCancel(); }}>
      <DialogContent
        className="[&>button:last-child]:hidden"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-default)',
          boxShadow: '0 20px 60px rgba(0, 0, 0, 0.5), 0 0 0 1px rgba(255, 255, 255, 0.06)',
        }}
      >
        <DialogHeader>
          <DialogTitle className="text-base" style={{ color: 'var(--color-text-primary)' }}>
            Report an Issue
          </DialogTitle>
          <DialogDescription className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            Help us improve the quality of responses
          </DialogDescription>
        </DialogHeader>

        {/* Issue categories */}
        <p className="text-sm font-medium mb-1" style={{ color: 'var(--color-text-secondary)' }}>
          What went wrong?
        </p>
        <div className="flex flex-wrap gap-2 mb-2">
          {ISSUE_CATEGORIES.map((cat) => {
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
                {cat}
              </button>
            );
          })}
        </div>

        {/* Comment */}
        <p className="text-sm mb-1" style={{ color: 'var(--color-text-secondary)' }}>
          Additional comments (optional)
        </p>
        <textarea
          value={comment}
          onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setComment(e.target.value)}
          placeholder="Tell us more about the issue..."
          rows={3}
          className="w-full rounded-lg px-3 py-2.5 text-sm resize-none outline-none mb-2"
          style={{
            backgroundColor: 'var(--color-bg-page)',
            color: 'var(--color-text-primary)',
            border: '1px solid var(--color-border-muted)',
          }}
        />

        {/* Human review consent */}
        <div
          className="rounded-lg pl-2.5 pr-3 py-2.5 cursor-pointer"
          style={{
            backgroundColor: 'var(--color-bg-surface)',
            border: `1px solid ${consentHumanReview ? 'var(--color-accent-primary)' : 'var(--color-border-muted)'}`,
          }}
          onClick={() => setConsentHumanReview(!consentHumanReview)}
        >
          <div className="flex items-center gap-2.5">
            <div
              className="w-4 h-4 rounded flex-shrink-0 flex items-center justify-center"
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
              Request human review
            </span>
          </div>
          <p className="text-xs mt-1.5 flex items-start gap-1" style={{ color: 'var(--color-text-tertiary)' }}>
            <Info className="h-3 w-3 flex-shrink-0 mt-0.5" />
            Share this conversation with our engineering team. Credits will be refunded for confirmed issues.
          </p>
        </div>

        {/* Agent self-report CTA */}
        {onReportWithAgent && (
          <div
            className="flex items-center gap-3 rounded-lg p-3 cursor-pointer transition-colors"
            style={{
              backgroundColor: 'var(--color-bg-surface)',
              border: '1px solid var(--color-border-muted)',
            }}
            onClick={() => {
              const categories = Array.from(selectedCategories);
              const instruction = [
                categories.length > 0 ? `User reported issues: ${categories.join(', ')}.` : '',
                comment.trim() ? `User feedback: "${comment.trim()}"` : '',
                'Reflect on your last response honestly. Identify the root cause — whether it\'s a bug in a tool, a flawed prompt instruction, bad data from an MCP server, or a reasoning error. Then file a GitHub issue via /self-improve with a truthful diagnosis.',
              ].filter(Boolean).join(' ');
              onReportWithAgent(instruction);
            }}
            onMouseEnter={(e: React.MouseEvent<HTMLDivElement>) => e.currentTarget.style.borderColor = 'var(--color-accent-primary)'}
            onMouseLeave={(e: React.MouseEvent<HTMLDivElement>) => e.currentTarget.style.borderColor = 'var(--color-border-muted)'}
          >
            <MessageSquareWarning
              className="h-5 w-5 flex-shrink-0"
              style={{ color: 'var(--color-accent-primary)' }}
            />
            <div>
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                Ask agent to self-report
              </span>
              <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-tertiary)' }}>
                The agent will reflect on what went wrong, diagnose the root cause, and file a GitHub issue via <code style={{ color: 'var(--color-accent-primary)', fontSize: '11px' }}>/self-improve</code>.
              </p>
            </div>
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
            Cancel
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
            Submit Report
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ThumbDownModal;
