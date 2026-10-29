/**
 * The computer-status renderer. The vocabulary it draws (the status table and
 * the predicates over it) lives in `utils/computerStatus.ts`, which hooks and
 * other modules read without pulling in a component.
 */
import { useTranslation } from 'react-i18next';

import { Loader } from '@/components/ui/loader';

import { PREPARING_UI, computerStatusUi, isComputerStatusTransitional } from '../utils/computerStatus';

interface ComputerStatusIndicatorProps {
  status: string | undefined | null;
  /** The machine is being prepared ({@link isComputerPreparing}), which its status alone cannot say. */
  preparing?: boolean;
  /** Text after the label, such as the step a preparation is on. */
  detail?: string | null;
  /** Glyph size in px; the label follows the surrounding font size. */
  glyphSize?: number;
  /** Drop the words and keep the glyph, for a surface that names the state elsewhere. */
  glyphOnly?: boolean;
  className?: string;
}

/**
 * Glyph plus text, never a bar or a wash. The dot shapes carry no text of
 * their own, so the label is what a screen reader hears; a machine in flight
 * also gets `role="status"`, which announces the transition without stealing
 * focus. Only in flight: a gallery page of settled machines would otherwise be
 * eight live regions announcing nothing.
 */
export function ComputerStatusIndicator({
  status,
  preparing = false,
  detail = null,
  glyphSize = 11,
  glyphOnly = false,
  className,
}: ComputerStatusIndicatorProps) {
  const { t } = useTranslation();
  const ui = preparing ? PREPARING_UI : computerStatusUi(status);
  const label = t(ui.labelKey, ui.fallback);
  const Icon = ui.icon;
  const announces = isComputerStatusTransitional(status);

  let glyph: React.ReactNode;
  if (ui.shape === 'live') {
    glyph = <Loader size={glyphSize} style={{ color: ui.color }} />;
  } else if (ui.shape === 'icon' && Icon) {
    glyph = <Icon style={{ width: glyphSize, height: glyphSize, color: ui.color }} />;
  } else {
    glyph = (
      <span
        className="rounded-full shrink-0"
        style={{
          width: glyphSize * 0.6,
          height: glyphSize * 0.6,
          backgroundColor: ui.shape === 'dot' ? ui.color : 'transparent',
          border: ui.shape === 'hollow' ? `1px solid ${ui.color}` : undefined,
        }}
      />
    );
  }

  return (
    <span
      className={className ? `inline-flex items-center gap-1.5 ${className}` : 'inline-flex items-center gap-1.5'}
      role={announces ? 'status' : undefined}
      aria-live={announces ? 'polite' : undefined}
      title={glyphOnly ? label : undefined}
    >
      <span aria-hidden="true" className="inline-flex items-center shrink-0">
        {glyph}
      </span>
      {glyphOnly ? (
        <span className="sr-only">{detail ? `${label}, ${detail}` : label}</span>
      ) : (
        <span>
          {label}
          {detail && (
            <>
              <span aria-hidden="true"> · </span>
              {detail}
            </>
          )}
        </span>
      )}
    </span>
  );
}
