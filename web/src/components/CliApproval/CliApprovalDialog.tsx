import { useRef } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import type { CliApproval } from '@/api/cliApprovals';
import { AUTO_APPROVE, useApproveAlways } from '@/hooks/useAutoApprove';
import {
  approvalNotes,
  approvalPreview,
  approvalTitle,
  formatCountdown,
  requestSentence,
  VARIANT,
  variant,
} from '@/lib/cliApprovals/copy';
import { threadPath } from '@/lib/desktopNotifications/notifications';

export interface CliApprovalDialogProps {
  approval: CliApproval;
  /** How many more wait behind this one. */
  more: number;
  /** The clock the countdown reads, from the host's own tick. */
  now: number;
  onAnswer: (approval: CliApproval, approve: boolean) => void;
  /** Closed without an answer: it waits, unshown here, until it runs out. */
  onDismiss: (approval: CliApproval) => void;
}

/**
 * One request the agent made from a command, put to the user wherever they
 * are in the app: what it would do, who asked and from which thread, and how
 * long it waits. Approve takes the destructive style for anything that
 * deletes or removes; a workspace to create offers "Always allow", as the
 * Chief of Staff's own card does. A request that hands the agent work shows
 * the start of the message or prompt it would send.
 */
export default function CliApprovalDialog({ approval, more, now, onAnswer, onDismiss }: CliApprovalDialogProps) {
  const { t } = useTranslation();
  const declineRef = useRef<HTMLButtonElement>(null);
  const { approveAlways, saving } = useApproveAlways(AUTO_APPROVE.workspaceCreation);
  const v = variant(approval);
  const { icon: Icon, approveKey, destructive } = VARIANT[v];
  const sentence = requestSentence(t, approval);
  const notes = approvalNotes(t, approval);
  const preview = approvalPreview(approval);
  const left = Date.parse(approval.expires_at) - now;
  const { thread_id: threadId, workspace_id: workspaceId } = approval.requester;

  return (
    <Dialog open onOpenChange={(open) => { if (!open) onDismiss(approval); }}>
      <DialogContent
        data-cli-approval={approval.approval_id}
        className="border sm:max-w-md"
        style={{
          backgroundColor: 'var(--color-shell-float, var(--color-bg-elevated))',
          borderColor: 'var(--color-shell-edge, var(--color-border-elevated))',
        }}
        // It opens unasked, so focus lands on Decline, never on the action.
        onOpenAutoFocus={(e) => {
          e.preventDefault();
          declineRef.current?.focus();
        }}
      >
        <DialogHeader className="gap-2 space-y-0 text-left sm:text-left">
          <div className="flex items-start gap-3 pr-6">
            <span
              aria-hidden
              className="mt-[-2px] flex h-7 w-7 shrink-0 items-center justify-center rounded-md"
              style={{ backgroundColor: 'var(--color-bg-subtle)', color: 'var(--color-text-secondary)' }}
            >
              <Icon className="h-4 w-4" />
            </span>
            <DialogTitle className="title-font min-w-0 text-base leading-snug break-words" style={{ color: 'var(--color-text-primary)' }}>
              {approvalTitle(t, approval)}
            </DialogTitle>
          </div>
          <DialogDescription className="leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
            <Trans
              i18nKey={sentence.key}
              values={sentence.values}
              // The sentence carries a <thread> tag only when the requester has a thread id.
              components={
                threadId
                  ? {
                      thread: (
                        <Link
                          to={threadPath(threadId)}
                          state={workspaceId ? { workspaceId } : undefined}
                          className="font-medium text-(--color-text-primary) underline decoration-(--color-border-strong) underline-offset-2 hover:decoration-current"
                        />
                      ),
                    }
                  : undefined
              }
            />
          </DialogDescription>
        </DialogHeader>

        {preview && (
          <blockquote
            data-testid="cli-approval-preview"
            className="max-h-40 overflow-y-auto rounded-lg px-4 py-3 text-sm whitespace-pre-wrap break-words"
            style={{ border: '1px solid var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
          >
            {preview}
          </blockquote>
        )}

        {notes.length > 0 && (
          <ul className="space-y-1 text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
            {notes.map((note) => <li key={note}>{note}</li>)}
          </ul>
        )}

        <div className="flex flex-col-reverse gap-3 sm:flex-row sm:items-center sm:justify-between">
          <p className="text-xs tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
            <span data-testid="cli-approval-expiry">{t('cliApproval.expiresIn', { time: formatCountdown(left) })}</span>
            {more > 0 && (
              <>
                <span aria-hidden> · </span>
                <span data-testid="cli-approval-more">{t('cliApproval.more', { count: more })}</span>
              </>
            )}
          </p>
          <DialogFooter className="gap-2 sm:gap-0">
            <Button
              type="button"
              variant="ghost"
              ref={declineRef}
              disabled={saving}
              onClick={() => onAnswer(approval, false)}
              style={{ color: 'var(--color-text-primary)' }}
            >
              {t('cliApproval.decline')}
            </Button>
            {v === 'workspaceCreate' && (
              <Button
                type="button"
                variant="outline"
                disabled={saving}
                title={t('cliApproval.alwaysApproveHint')}
                onClick={() => approveAlways(() => onAnswer(approval, true))}
              >
                {t('cliApproval.alwaysApprove')}
              </Button>
            )}
            <Button
              type="button"
              variant={destructive ? 'destructive' : 'default'}
              disabled={saving}
              onClick={() => onAnswer(approval, true)}
            >
              {t(approveKey)}
            </Button>
          </DialogFooter>
        </div>
      </DialogContent>
    </Dialog>
  );
}
