import type { TFunction } from 'i18next';
import { toast } from '@/components/ui/use-toast';
import { ToastAction } from '@/components/ui/toast';

/**
 * The word that an add went through. The form closes on success and the new
 * row lands somewhere in a list that may be sorted or filtered away from
 * where the user is looking, so without this the only sign of success was the
 * dialog vanishing. The tool count is said only where the form's own check
 * listed the tools for exactly what was saved.
 */
export function toastConnectorAdded({
  name,
  toolCount,
  onOpen,
  t,
}: {
  name: string;
  toolCount: number | null;
  /** Opens the connector's own page; absent where the surface has none. */
  onOpen?: () => void;
  t: TFunction;
}) {
  const open = t('mcp.added.open');
  toast({
    title:
      toolCount === null
        ? t('mcp.added.title', { name })
        : t('mcp.added.titleTools', { name, count: toolCount }),
    action: onOpen ? (
      <ToastAction altText={open} onClick={onOpen}>
        {open}
      </ToastAction>
    ) : undefined,
    // A confirmation with a shortcut, not a question waiting on an answer, so
    // it leaves by itself even though it carries an action.
    duration: 6000,
  });
}
