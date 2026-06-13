import { useCallback, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogTitle,
} from '@/components/ui/dialog';

/**
 * Gate an "open in new tab" action with a confirm step when the destination is
 * the raw wsfiles URL — a non-revocable, workspace-wide credential the owner
 * shouldn't hand out. When `guard` is false (public share serve URL, widget
 * blob), the action runs immediately.
 *
 * A toast can't warn here: `window.open` moves focus to the new tab, so a
 * notice in the now-backgrounded app tab is never seen. The confirm runs
 * `open()` from within the button's own click handler, so the popup keeps its
 * user gesture and isn't blocked.
 *
 * Returns the click handler to wire to the button and the dialog node to render.
 */
export function useDirectLinkGuard(open: () => void, guard: boolean) {
  const { t } = useTranslation();
  const [confirming, setConfirming] = useState(false);

  const request = useCallback(() => {
    if (guard) setConfirming(true);
    else open();
  }, [guard, open]);

  const confirm = useCallback(() => {
    setConfirming(false);
    open();
  }, [open]);

  const dialog = (
    <Dialog open={confirming} onOpenChange={setConfirming}>
      <DialogContent
        className="sm:max-w-md"
        style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}
      >
        <DialogTitle className="text-base font-semibold" style={{ color: 'var(--color-text-primary)' }}>
          {t('filePanel.privateLinkWarningTitle')}
        </DialogTitle>
        <DialogDescription className="text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
          {t('filePanel.privateLinkWarning')}
        </DialogDescription>
        <DialogFooter className="gap-2 pt-2">
          <button
            type="button"
            onClick={() => setConfirming(false)}
            className="px-3 py-1.5 rounded text-sm border"
            style={{ color: 'var(--color-text-primary)', borderColor: 'var(--color-border-default)' }}
          >
            {t('common.cancel')}
          </button>
          <button
            type="button"
            onClick={confirm}
            className="px-4 py-1.5 rounded text-sm font-medium hover:opacity-90"
            style={{ backgroundColor: 'var(--color-accent-primary)', color: 'var(--color-text-on-accent)' }}
          >
            {t('filePanel.openInNewTab')}
          </button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );

  return { request, dialog };
}
