import { useState } from 'react';
import { Paperclip, Loader2 } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { ContextBus } from '@/lib/contextBus';
import { getWidgetContextSnapshot } from '../widgets/framework/contextSnapshot';
import { useToast } from '@/components/ui/use-toast';
import './RowAttachButton.css';

interface RowAttachButtonProps {
  /** Widget instance id — used to look up the registered exporter. */
  instanceId: string;
  /** Per-row identifier the widget's `rows(rowId)` exporter expects. */
  rowId: string;
  /** Optional className to control row-level positioning. */
  className?: string;
}

/**
 * Hover-revealed paperclip button for list rows — same "attach" affordance
 * as the widget-frame paperclip, scoped to a single row. Calls into the
 * registered widget exporter's `rows(rowId)` function to produce a snapshot
 * of just that row, then publishes via ContextBus so every mounted chat
 * input picks it up.
 *
 * This component carries the `widget-drag-cancel` class so clicking it inside
 * an edit-mode draggable widget doesn't initiate a drag. List widgets should
 * place this inside the row markup; pair with `.row-attach-host` (a `position:
 * relative; group` wrapper) so the button can absolute-position to the right.
 */
export function RowAttachButton({ instanceId, rowId, className }: RowAttachButtonProps) {
  const { t } = useTranslation();
  const { toast } = useToast();
  const [busy, setBusy] = useState(false);

  const handleClick = async (e: React.MouseEvent) => {
    e.stopPropagation();
    e.preventDefault();
    if (busy) return;
    setBusy(true);
    try {
      const snapshot = await getWidgetContextSnapshot(instanceId, rowId);
      if (!snapshot) {
        toast({
          variant: 'destructive',
          title: t('dashboard.widgets.frame.contextUnavailable', { defaultValue: 'Widget not available' }),
          description: t('dashboard.widgets.frame.contextUnavailableHint', {
            defaultValue: 'This widget has not registered a context exporter yet.',
          }),
        });
        return;
      }
      ContextBus.attach(snapshot);
      toast({
        title: t('dashboard.widgets.frame.contextAttached', { defaultValue: 'Added to context' }),
        description: snapshot.label,
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <button
      type="button"
      className={`row-attach widget-drag-cancel ${className ?? ''}`}
      onClick={handleClick}
      disabled={busy}
      aria-label={t('dashboard.widgets.frame.addRowToContext', { defaultValue: 'Attach to chat' })}
      title={t('dashboard.widgets.frame.addRowToContextTitle', { defaultValue: 'Attach to chat' })}
    >
      {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : <Paperclip className="h-3 w-3" />}
    </button>
  );
}
