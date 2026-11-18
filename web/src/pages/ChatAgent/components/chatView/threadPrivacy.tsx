/**
 * Who sees a conversation in a shared workspace: the header says which and
 * offers the change its starter may make, the composer carries a chip for a
 * private one, and a new thread's chip is a switch that sets what its first
 * send creates. `useThreadPrivacy` holds the state and places the chip and
 * the dialog; nothing here renders for a workspace of one's own.
 */
import { useTranslation } from 'react-i18next';
import { Lock, Users } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { PillToggle } from '@/components/ui/chat-input.parts';
import type { ThreadPrivacy } from './useThreadPrivacy';

/** Under the chat's top bar: who sees the conversation, the change its
 *  starter may make, and for a private one, once, what private covers. */
export function ThreadPrivacyRow({ privacy, isMobile }: { privacy: ThreadPrivacy; isMobile: boolean }) {
  const { t } = useTranslation();
  const { shared, workspaceName, canChange } = privacy;
  const Glyph = shared ? Users : Lock;
  return (
    <div
      className={`shrink-0 border-b ${isMobile ? 'px-3' : 'px-6'} py-1.5`}
      style={{ borderColor: 'var(--color-border-muted)' }}
      data-testid="thread-privacy-row"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-0.5 text-xs">
        <span className="flex min-w-0 items-center gap-1.5" style={{ color: 'var(--color-text-secondary)' }} data-testid="thread-privacy-state">
          <Glyph aria-hidden className="h-3.5 w-3.5 shrink-0" />
          <span className="truncate">
            {shared ? t('sharedWorkspace.privacy.sharedWith', { workspace: workspaceName }) : t('sharedWorkspace.privacy.onlyYou')}
          </span>
        </span>
        {canChange && (
          <button
            type="button"
            onClick={() => privacy.request(!shared)}
            className="tap-target flex shrink-0 items-center gap-1.5 rounded-md px-1.5 py-0.5 font-medium transition-colors hover:bg-(--color-border-muted) focus-visible:ring-2 focus-visible:ring-ring"
            style={{ color: 'var(--color-text-primary)' }}
            data-testid="thread-privacy-change"
          >
            {shared
              ? <><Lock aria-hidden className="h-3.5 w-3.5" />{t('sharedWorkspace.privacy.makePrivate')}</>
              : <><Users aria-hidden className="h-3.5 w-3.5" />{t('sharedWorkspace.privacy.share')}</>}
          </button>
        )}
      </div>
      {!shared && (
        <p className="mt-0.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }} data-testid="thread-privacy-note">
          {t('sharedWorkspace.privacy.note', { workspace: workspaceName })}
          {/* Spaced by margin, as Chinese sentences take no space between them. */}
          {privacy.linkTurnedOff && <span className="ms-1">{t('sharedWorkspace.privacy.linkTurnedOff')}</span>}
        </p>
      )}
    </div>
  );
}

/** The composer's chip: "Private" on a private thread, which opens the share
 *  question; on a new thread a switch between private and shared. */
export function PrivacyChip({ privacy }: { privacy: ThreadPrivacy }) {
  const { t } = useTranslation();
  const { shared, workspaceName, canChange } = privacy;
  const label = shared
    ? t('sharedWorkspace.privacy.sharedWith', { workspace: workspaceName })
    : t('sharedWorkspace.privacy.chipPrivate');
  const title = shared
    ? t('sharedWorkspace.privacy.chipSharedTitle', { workspace: workspaceName })
    : t('sharedWorkspace.privacy.chipPrivateTitle');
  return (
    <span className="flex min-w-0" data-testid="composer-privacy-chip">
      <PillToggle
        icon={shared ? Users : Lock}
        label={label}
        title={title}
        aria="none"
        className="min-w-0"
        labelClassName="min-w-0 max-w-[160px] truncate"
        onToggle={canChange ? () => privacy.request(!shared) : undefined}
        disabledReason={canChange ? null : title}
      />
    </span>
  );
}

/** Asks before a conversation that exists is shared: from then on every
 *  member reads it from its first message. */
export function ShareThreadDialog({ open, workspaceName, onCancel, onConfirm }: {
  open: boolean;
  workspaceName: string;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const { t } = useTranslation();
  return (
    <Dialog open={open} onOpenChange={(next) => { if (!next) onCancel(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-shell-float, var(--color-bg-page))', borderColor: 'var(--color-shell-edge, var(--color-border-muted))' }}>
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Users aria-hidden className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            {t('sharedWorkspace.privacy.confirmTitle', { workspace: workspaceName })}
          </DialogTitle>
          <DialogDescription>{t('sharedWorkspace.privacy.confirmBody')}</DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="ghost" onClick={onCancel}>{t('common.cancel')}</Button>
          <Button onClick={onConfirm} data-testid="share-thread-confirm">{t('sharedWorkspace.privacy.confirmShare')}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

