import { useId } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { useBackdropDismiss, useDialogA11y } from '@/hooks/useDialogA11y';

/**
 * The modal shell the install flow renders into. It is its own component
 * because the flow has two entry points — the wizard, and an update that opens
 * straight onto its outcome — and a second hand-rolled overlay would be a
 * second focus trap to keep correct.
 *
 * It sits above the house z-[60] modal layer: an update's outcome opens over
 * the plugin detail overlay that launched it, which is already at z-[60].
 */

/** Every dismissal route is gated on one flag, so a non-dismissable step
 *  cannot be closed through a route someone forgot about. */
const NOOP = () => {};

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
  const { t } = useTranslation();
  const titleId = useId();
  const close = dismissable ? onClose : NOOP;
  const dialogRef = useDialogA11y<HTMLDivElement>(close);
  const backdrop = useBackdropDismiss<HTMLDivElement>(close);

  return (
    <div
      className="fixed inset-0 z-[70] flex items-center justify-center p-4"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      {...backdrop}
    >
      {/* Capped and split into a fixed head over a scrolling body, the same
          shape DetailOverlay uses. An install report grows with the package:
          uncapped, a large one overflows a centred flex child off both ends
          of the viewport at once, putting its own Done button out of reach. */}
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className="relative w-full max-w-lg max-h-[calc(100vh-2rem)] rounded-lg flex flex-col"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
        }}
      >
        {dismissable && (
          <button
            onClick={onClose}
            className="absolute top-3 right-3 p-1 rounded-full transition-colors hover:bg-foreground/10"
            style={{ color: 'var(--color-text-primary)' }}
            aria-label={t('common.close')}
          >
            <X className="h-4 w-4" />
          </button>
        )}

        <div className="flex-shrink-0 px-5 pt-5 pb-4 pr-12">
          <h3
            id={titleId}
            className="text-lg font-semibold mb-1"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {title}
          </h3>
          <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {subtitle}
          </p>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto px-5 pb-5">{children}</div>
      </div>
    </div>
  );
}
