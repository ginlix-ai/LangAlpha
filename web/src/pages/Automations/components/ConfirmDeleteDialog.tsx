import React from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';

interface ConfirmDeleteDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onConfirm: () => void;
  automationName?: string;
  loading: boolean;
}

export default function ConfirmDeleteDialog({ open, onOpenChange, onConfirm, automationName, loading }: ConfirmDeleteDialogProps) {
  const { t } = useTranslation();
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent
        className="sm:max-w-md border"
        style={{ backgroundColor: 'var(--color-shell-float, var(--color-bg-elevated))', borderColor: 'var(--color-shell-edge, var(--color-border-elevated))' }}
      >
        <DialogHeader>
          <DialogTitle style={{ color: 'var(--color-text-primary)' }}>{t('automation.deleteAutomation')}</DialogTitle>
          <DialogDescription style={{ color: 'var(--color-text-secondary)' }}>
            {t('automation.deleteConfirmMsg', { name: automationName })}
          </DialogDescription>
        </DialogHeader>
        <DialogFooter className="gap-2 sm:gap-0">
          <Button
            variant="ghost"
            onClick={() => onOpenChange(false)}
            style={{ color: 'var(--color-text-primary)' }}
          >
            {t('common.cancel')}
          </Button>
          <Button
            variant="destructive"
            onClick={onConfirm}
            disabled={loading}
          >
            {loading ? t('automation.deleting') : t('common.delete')}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
