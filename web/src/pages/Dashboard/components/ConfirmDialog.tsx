import React from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { lastInputWasPointer } from '@/lib/inputModality';
import { cn } from '@/lib/utils';
import './dashForms.css';

interface ConfirmDialogProps {
  open: boolean;
  title: string;
  message: string;
  confirmLabel?: string;
  onConfirm?: () => void | Promise<void>;
  onOpenChange?: (open: boolean) => void;
  /** Open on the confirm button, so Enter confirms as a native confirm() does. */
  focusConfirm?: boolean;
  /** `page` sets it on the page's ground, as the dashboard's windows are. Other
   *  surfaces keep the elevated fill until the floating surfaces converge. */
  surface?: 'elevated' | 'page';
}

/**
 * Reusable confirmation dialog. Uses color tokens only.
 */
function ConfirmDialog({ open, title, message, confirmLabel, onConfirm, onOpenChange, focusConfirm = false, surface = 'elevated' }: ConfirmDialogProps) {
  const { t } = useTranslation();
  const confirmRef = React.useRef<HTMLButtonElement>(null);
  // Radix hands focus back to a Dialog.Trigger, and this dialog opens from
  // state without one, so it hands focus back to what held it itself.
  const returnFocusRef = React.useRef<HTMLElement | null>(null);
  const handleConfirm = async () => {
    if (onConfirm) await onConfirm();
    onOpenChange?.(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className={cn('sm:max-w-sm border', surface === 'page' && 'dash-form')}
        style={surface === 'page' ? undefined : { backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-elevated)' }}
        onOpenAutoFocus={(e) => {
          returnFocusRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
          if (!focusConfirm) return;
          e.preventDefault();
          confirmRef.current?.focus();
        }}
        onCloseAutoFocus={(e) => {
          // After a click DialogContent keeps focus off, as for every overlay.
          if (lastInputWasPointer()) return;
          e.preventDefault();
          returnFocusRef.current?.focus();
        }}
      >
        <DialogHeader>
          <DialogTitle className="title-font" style={{ color: 'var(--color-text-primary)' }}>
            {title}
          </DialogTitle>
          <p className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>{message}</p>
        </DialogHeader>
        <DialogFooter className="gap-2 pt-4">
          <button
            type="button"
            onClick={() => onOpenChange?.(false)}
            className="px-3 py-1.5 rounded text-sm border"
            style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
            onMouseEnter={(e) => e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'}
            onMouseLeave={(e) => e.currentTarget.style.backgroundColor = 'transparent'}
          >
            {t('common.cancel')}
          </button>
          <button
            ref={confirmRef}
            type="button"
            onClick={handleConfirm}
            className="px-4 py-1.5 rounded text-sm font-medium hover:opacity-90"
            style={{ backgroundColor: 'var(--color-btn-primary-bg)', color: 'var(--color-btn-primary-text)' }}
          >
            {confirmLabel || t('common.delete')}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default ConfirmDialog;
