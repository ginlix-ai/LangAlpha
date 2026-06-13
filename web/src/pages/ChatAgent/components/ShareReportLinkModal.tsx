import React, { useState } from 'react';
import { Globe, Monitor, Loader2 } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@/components/ui/dialog';

type BusyMode = 'shareable' | 'direct' | null;

interface ShareReportLinkModalProps {
  open: boolean;
  /** File name shown in the subtitle (display only). */
  fileName: string;
  /** Enable sharing and copy the public, revocable serve URL. Throws on failure. */
  onCopyShareable: () => Promise<void>;
  /** Copy the direct full-screen wsfiles URL. Throws on failure. */
  onCopyDirect: () => Promise<void>;
  onClose: () => void;
}

interface OptionCardProps {
  icon: React.ReactNode;
  title: string;
  desc: string;
  badge?: string;
  busy: boolean;
  disabled: boolean;
  onClick: () => void;
}

function OptionCard({ icon, title, desc, badge, busy, disabled, onClick }: OptionCardProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className="flex items-start gap-3 text-left rounded-lg border p-3 transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
      style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-card)' }}
      onMouseEnter={(e) => { if (!disabled) e.currentTarget.style.borderColor = 'var(--color-accent-primary)'; }}
      onMouseLeave={(e) => { e.currentTarget.style.borderColor = 'var(--color-border-muted)'; }}
    >
      <span className="mt-0.5 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }}>
        {busy ? <Loader2 className="h-5 w-5 animate-spin" /> : icon}
      </span>
      <span className="flex flex-col gap-1 min-w-0">
        <span className="flex items-center gap-2 text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {title}
          {badge && (
            <span
              className="text-[10px] font-semibold uppercase tracking-wide px-1.5 py-0.5 rounded"
              style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
            >
              {badge}
            </span>
          )}
        </span>
        <span className="text-xs leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
          {desc}
        </span>
      </span>
    </button>
  );
}

/**
 * Consent chooser shown before copying a link to an HTML report.
 *
 * Two options: a public, revocable share link (token-scoped), or a direct
 * full-screen wsfiles link (workspace-UUID credential, not revocable, reaches
 * the whole workspace). Replaces the old silent auto-enable-sharing behavior.
 */
function ShareReportLinkModal({
  open,
  fileName,
  onCopyShareable,
  onCopyDirect,
  onClose,
}: ShareReportLinkModalProps) {
  const { t } = useTranslation();
  const [busy, setBusy] = useState<BusyMode>(null);

  // Run a copy action: show a spinner on the chosen option, close on success.
  // The action owns its success/failure toast; on failure we keep the modal
  // open so the user can pick the other option or retry.
  const run = async (mode: Exclude<BusyMode, null>, fn: () => Promise<void>) => {
    if (busy) return;
    setBusy(mode);
    try {
      await fn();
      onClose();
    } catch {
      /* action already surfaced the failure toast */
    } finally {
      setBusy(null);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o && !busy) onClose(); }}>
      <DialogContent style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}>
        <DialogTitle className="text-lg font-semibold" style={{ color: 'var(--color-text-primary)' }}>
          {t('filePanel.copyLinkTitle')}
        </DialogTitle>
        <DialogDescription className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
          {t('filePanel.copyLinkSubtitle', { file: fileName })}
        </DialogDescription>

        <div className="flex flex-col gap-3 pt-1">
          <OptionCard
            icon={<Globe className="h-5 w-5" />}
            title={t('filePanel.shareableLinkOption')}
            desc={t('filePanel.shareableLinkDesc')}
            busy={busy === 'shareable'}
            disabled={!!busy}
            onClick={() => run('shareable', onCopyShareable)}
          />
          <OptionCard
            icon={<Monitor className="h-5 w-5" />}
            title={t('filePanel.directLinkOption')}
            desc={t('filePanel.directLinkDesc')}
            busy={busy === 'direct'}
            disabled={!!busy}
            onClick={() => run('direct', onCopyDirect)}
          />
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default ShareReportLinkModal;
