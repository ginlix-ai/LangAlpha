/**
 * The user's one computer, named once in the toolbar instead of on every card:
 * every workspace lives on it, so a line per card would say the same thing
 * forty times. It stands where the computer filter stands once there are two.
 *
 * Its state, badge and a filling disk are the machine's, so they ride here
 * too. It is the trigger of the toolbar's computer menu, so it takes the
 * trigger's props and ref and puts them on its button.
 */
import type { ComponentPropsWithRef } from 'react';
import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { ComputerBadge } from '../ComputerBadge';
import { ComputerStatusIndicator } from '../computerStatusUi';
import { PREPARING_UI, computerStatusUi, isComputerPreparing } from '../../utils/computerStatus';
import { DiskPercent } from './DiskPercent';

interface ComputerChipProps extends Omit<ComponentPropsWithRef<'button'>, 'children'> {
  computer: Computer;
  isThisMac: boolean;
}

export function ComputerChip({ computer, isThisMac, ...button }: ComputerChipProps) {
  const { t } = useTranslation();
  const preparing = isComputerPreparing(computer);
  const running = computer.status === 'running' && !preparing;
  const stateUi = preparing ? PREPARING_UI : computerStatusUi(computer.status);

  return (
    <button
      type="button"
      data-testid="computer-chip"
      {...button}
    >
      {/* The glyph names the state for a screen reader; the word after is for the eye. */}
      <ComputerStatusIndicator status={computer.status} preparing={preparing} glyphSize={10} glyphOnly className="shrink-0" />
      <span className="min-w-0 truncate">{computer.name}</span>
      {/* A phone's line is shared with sort: the name is what says which
          machine, so the badge gives way there rather than the name. */}
      <ComputerBadge computer={computer} isThisMac={isThisMac} name={computer.name} className="max-sm:hidden" />
      {!running && (
        <span
          className="shrink-0 whitespace-nowrap text-xs"
          style={{ color: 'var(--color-text-tertiary)' }}
          aria-hidden="true"
          data-testid="machine-state"
        >
          {t(stateUi.labelKey, stateUi.fallback)}
        </span>
      )}
      <DiskPercent computer={computer} className="text-xs" />
    </button>
  );
}

export default ComputerChip;
