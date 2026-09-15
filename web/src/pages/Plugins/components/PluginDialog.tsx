import { useId } from 'react';
import { ModalShell } from '@/components/ui/ModalShell';

/**
 * The dialog the install flow renders into: the house ModalShell, pinned one
 * layer above the z-[1010] modal layer because an update's outcome opens over
 * the plugin detail overlay that launched it, which is already there. Its own
 * component so the two entry points (the wizard, and an update that opens
 * straight onto its outcome) share one shape.
 */

export function PluginDialog({
  title,
  subtitle,
  onClose,
  dismissable = true,
  children,
}: {
  title: string;
  /** The step within the flow, named under the title. */
  subtitle: string;
  onClose: () => void;
  /** False while the step owns work the user cannot get back by reopening —
   *  an install in flight, whose report is the only copy of what happened. */
  dismissable?: boolean;
  children: React.ReactNode;
}) {
  const titleId = useId();
  return (
    <ModalShell
      labelId={titleId}
      title={title}
      subtitle={subtitle}
      onClose={onClose}
      dismissable={dismissable}
      zIndex={1015}
      bodyClassName="gap-0"
    >
      {children}
    </ModalShell>
  );
}
