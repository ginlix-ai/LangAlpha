/**
 * One quiet line about the machine a workspace lives on, drawn on a gallery
 * card only when the cards sit on more than one machine to tell apart: whether
 * it is up, its kind, its name, and the badge's words the name does not
 * already say.
 *
 * A running machine is just its green dot; any other state is also spelled
 * out, since that is the one worth reading. The disk is the machine's, so it
 * is not repeated here on every card (see DiskPercent).
 */
import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { ComputerBadge, computerIdentity } from '../ComputerBadge';
import { ComputerStatusIndicator } from '../computerStatusUi';
import { PREPARING_UI, computerStatusUi, isComputerPreparing } from '../../utils/computerStatus';

interface MachineSummaryProps {
  computer: Computer;
  isThisMac: boolean;
  /** Makes the name a button that opens the Computers dialog on this machine. */
  onOpen?: (computerId: string) => void;
  /** Leaves the badge's words out, for a line short of room; its glyph still says the kind. */
  compact?: boolean;
}

export function MachineSummary({ computer, isThisMac, onOpen, compact = false }: MachineSummaryProps) {
  const { t } = useTranslation();
  const preparing = isComputerPreparing(computer);
  const running = computer.status === 'running' && !preparing;
  const stateUi = preparing ? PREPARING_UI : computerStatusUi(computer.status);
  const KindIcon = computerIdentity(t, computer, isThisMac).icon;

  // The glyph carries the state for a screen reader (its label is sr-only), so
  // the visible word at the end is not read a second time.
  const name = (
    <>
      <ComputerStatusIndicator status={computer.status} preparing={preparing} glyphSize={10} glyphOnly className="shrink-0" />
      <KindIcon className="h-3 w-3 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} aria-hidden="true" />
      <span className="truncate" style={{ color: 'var(--color-text-secondary)' }}>{computer.name}</span>
    </>
  );

  return (
    <div className="flex min-w-0 items-center gap-1 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
      {onOpen ? (
        <button
          type="button"
          className="-mx-1 flex min-w-0 items-center gap-1.5 rounded px-1 hover:bg-(--color-bg-hover)"
          title={t('computer.manageComputer', 'Manage {{name}}', { name: computer.name })}
          onClick={(e) => {
            // The card opens the workspace; this name is the machine.
            e.stopPropagation();
            onOpen(computer.computer_id);
          }}
        >
          {name}
        </button>
      ) : (
        <span className="flex min-w-0 items-center gap-1.5">{name}</span>
      )}
      {!compact && <ComputerBadge computer={computer} isThisMac={isThisMac} name={computer.name} variant="plain" />}
      {!running && (
        <>
          <span aria-hidden="true">·</span>
          <span className="shrink-0 whitespace-nowrap" aria-hidden="true" data-testid="machine-state">
            {t(stateUi.labelKey, stateUi.fallback)}
          </span>
        </>
      )}
    </div>
  );
}

export default MachineSummary;
