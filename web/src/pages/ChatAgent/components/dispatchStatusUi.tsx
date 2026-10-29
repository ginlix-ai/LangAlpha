/**
 * How a run handed to a workspace analyst reads in the composer's report-back
 * list: the run's own status, which the roster follows. The transcript speaks
 * the hand-off's phase instead (`analystHandoff/handoffModel.ts`); both draw
 * from the run-row kit's glyphs.
 */
import { useTranslation } from 'react-i18next';
import { Check } from 'lucide-react';
import type { PTCDispatchStatus } from '../hooks/usePTCDispatchStatus';
import { ElapsedClock, RunGlyph, type RunGlyphKind } from './messageList/runCard';

const DANGER = 'var(--color-icon-danger)';

type ComposerGlyphKind = RunGlyphKind | 'check';

/** Text and a small glyph, never a filled pill or accent bar: amber stays
 *  annotation-only. The composer's list is roomy enough to keep a check for
 *  a finished run, which the transcript's dense rows leave out. */
function ComposerGlyph({ kind }: { kind: ComposerGlyphKind }) {
  return kind === 'check'
    ? <Check aria-hidden className="h-3 w-3 shrink-0 stroke-[2.5] text-(--color-success)" />
    : <RunGlyph kind={kind} />;
}

/** A run's own status, as the composer's report-back list shows it. */
const STATUS_LOOK: Record<PTCDispatchStatus, { labelKey: string; glyph: ComposerGlyphKind; labelColor: string }> = {
  starting: { labelKey: 'chat.ptcCard.statusStarting', glyph: 'live', labelColor: 'var(--color-text-tertiary)' },
  running: { labelKey: 'chat.ptcCard.statusWorking', glyph: 'live', labelColor: 'var(--color-text-tertiary)' },
  needs_input: { labelKey: 'chat.ptcCard.statusNeedsInput', glyph: 'ring', labelColor: 'var(--color-accent-primary)' },
  completed: { labelKey: 'chat.ptcCard.statusCompleted', glyph: 'check', labelColor: 'var(--color-text-tertiary)' },
  failed: { labelKey: 'chat.ptcCard.statusFailed', glyph: 'alert', labelColor: DANGER },
  stopped: { labelKey: 'chat.ptcCard.statusStopped', glyph: 'stop', labelColor: 'var(--color-text-tertiary)' },
};

/** A dispatched run's glyph alone, beside a label that names the run. The
 *  glyph is decorative, so its status word stays for a screen reader. */
export function DispatchStatusGlyph({ status }: { status: PTCDispatchStatus }) {
  const { t } = useTranslation();
  const ui = STATUS_LOOK[status];
  return (
    <>
      <ComposerGlyph kind={ui.glyph} />
      <span className="sr-only">{t(ui.labelKey)}</span>
    </>
  );
}

/** A dispatched run's status word and glyph, with the time it has been working
 *  while it runs. */
export function DispatchStatusIndicator({ status, startedAt }: { status: PTCDispatchStatus; startedAt?: number }) {
  const { t } = useTranslation();
  const ui = STATUS_LOOK[status];
  // One persistent role="status" live region; only the inner glyph/label swap
  // per state. Mounting a fresh live region per state can drop the
  // announcement, so transitions stay reliably announced this way.
  return (
    <span
      role="status"
      aria-live="polite"
      className="inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap text-[0.75rem] font-medium"
      style={{ color: ui.labelColor }}
    >
      <ComposerGlyph kind={ui.glyph} />
      {t(ui.labelKey)}
      {status === 'running' && (
        <ElapsedClock startedAt={startedAt} className="text-[0.6875rem] text-(--color-text-quaternary)" />
      )}
    </span>
  );
}
