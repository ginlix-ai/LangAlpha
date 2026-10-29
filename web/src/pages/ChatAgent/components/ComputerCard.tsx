/**
 * One machine in the Computers dialog, as a card: a drawing of the machine
 * over its name and what is checked at a glance (status, spec, workspaces,
 * disk), with Start or Stop. Everything else is in its details, so every card
 * is the same size and the rail never stretches.
 *
 * Only the card in the middle of the rail takes input (railCard).
 */
import { useTranslation } from 'react-i18next';
import { ChevronRight } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { Computer } from '@/types/api';

import type { ThisMac } from '../hooks/useRemoteControl';
import { DiskBar, DiskUsageText } from './computerDiskUi';
import { ComputerFacts } from './computerFacts';
import {
  ComputerNotes,
  ComputerSpecRows,
  MachineFrame,
  PowerButton,
  StartFailureAction,
  type MachineActions,
} from './computerParts';
import { railCard, type RailPlace } from './computerRailState';

interface ComputerCardProps {
  computer: Computer;
  place: RailPlace;
  /** Set when this card is this Mac, in the desktop app on it. */
  thisMac: ThisMac | null;
  /** Name the provider in the facts, when there are providers to tell apart. */
  showProvider: boolean;
  actions: MachineActions;
  onOpenDetails: () => void;
}

function ComputerCard({ computer, place, thisMac, showProvider, actions, onOpenDetails }: ComputerCardProps) {
  const { t } = useTranslation();
  const disk = computer.disk ?? null;
  const rail = railCard(place.active, place.onActivate);

  return (
    <article
      className={cn('computer-card', place.active && 'is-active')}
      data-testid="computer-card"
      data-computer-id={computer.computer_id}
      aria-label={computer.name}
      {...rail.card}
    >
      {/* The machine itself opens its details, as the button under it does. */}
      <MachineFrame
        computer={computer}
        index={place.index}
        thisMac={thisMac}
        onOpen={place.active ? onOpenDetails : undefined}
        {...rail.content}
      />

      <div className="computer-card-body" {...rail.content}>
        <span className="computer-card-name truncate">{computer.name}</span>
        <ComputerFacts computer={computer} showProvider={showProvider} />
        <ComputerNotes computer={computer} thisMac={thisMac} />

        <ComputerSpecRows computer={computer}>
          <div>
            <dt>{t('computer.card.disk', 'Disk')}</dt>
            <dd>
              <DiskUsageText disk={disk} />
            </dd>
          </div>
        </ComputerSpecRows>
        {disk && (
          <div className="computer-card-storage">
            <DiskBar disk={disk} />
          </div>
        )}

        <div className="computer-card-actions">
          <StartFailureAction computer={computer} actions={actions} />
          <PowerButton computer={computer} actions={actions} />
          <Button variant="ghost" size="sm" className="ml-auto gap-0.5" onClick={onOpenDetails} data-computer-details>
            {t('computer.card.details', 'Details')}
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      </div>
    </article>
  );
}

export default ComputerCard;
