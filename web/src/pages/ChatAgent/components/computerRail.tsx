/**
 * The Computers dialog's rail: a card per machine, one in the middle at a
 * time, the reader clicking, swiping or arrowing through them; the last card
 * adds another. Under it, where the middle is and the arrows to step.
 */
import { Fragment, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronLeft, ChevronRight, Plus } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import type { Computer } from '@/types/api';

import { ArtFrame } from './machineArt/ArtFrame';
import { railCard, railOrdinal, useComputerRail, type RailPlace } from './computerRailState';

/**
 * Where the rail starts as it mounts: this machine's card, else the card at
 * `index`. Each new start is a new `visit`, the rail's key, so a rail left
 * and come back to mounts afresh even before the last one has faded out.
 */
export interface RailStart {
  id: string | null;
  index: number;
  /** Coming back to it, so focus goes to its Details. */
  focus: boolean;
  visit: number;
}

interface ComputerRailProps {
  computers: Computer[];
  start: RailStart;
  /** A machine just made here, glided to as it lands. */
  arrivingId: string | null;
  card: (computer: Computer, place: RailPlace) => ReactNode;
  onCreate: () => void;
  /** Bringing a machine is on offer here, so the New computer card says so. */
  ownMachines: boolean;
  /** At the plan's cap with nothing left to add; the note under the rail says why. */
  held: boolean;
  heldNoteId: string;
}

export function ComputerRail({ computers, start, arrivingId, card, onCreate, ownMachines, held, heldNoteId }: ComputerRailProps) {
  const { t } = useTranslation();
  const count = computers.length + 1;
  const startIndex = start.id ? computers.findIndex((c) => c.computer_id === start.id) : -1;
  const { ref: railRef, active, goTo, onScroll, onTakeOver, onKeyDown } = useComputerRail(
    count,
    { index: startIndex >= 0 ? startIndex : Math.min(start.index, count - 1), focus: start.focus },
    arrivingId,
    arrivingId ? computers.findIndex((c) => c.computer_id === arrivingId) : -1,
  );
  const place = (i: number): RailPlace => ({ index: i + 1, active: i === active, onActivate: () => goTo(i) });

  return (
    <div>
      {computers.length === 0 && (
        <p className="text-sm pb-3" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('computer.empty', 'No computers yet. Your first workspace creates one.')}
        </p>
      )}
      <div
        ref={railRef}
        className="computer-rail"
        // Focus lands here only when the card stepped to has nothing to take it.
        tabIndex={-1}
        aria-label={t('computer.computers', 'Computers')}
        onScroll={onScroll}
        onWheel={onTakeOver}
        onPointerDown={onTakeOver}
        onTouchStart={onTakeOver}
        onKeyDown={onKeyDown}
      >
        {computers.map((computer, i) => (
          <Fragment key={computer.computer_id}>{card(computer, place(i))}</Fragment>
        ))}
        <NewComputerCard
          place={place(computers.length)}
          onCreate={onCreate}
          ownMachines={ownMachines}
          held={held}
          heldNoteId={heldNoteId}
        />
      </div>
      <div className="computer-rail-nav">
        <span className="computer-rail-count" aria-live="polite">
          {t('computer.card.position', '{{n}} / {{total}}', { n: railOrdinal(active + 1), total: railOrdinal(count) })}
        </span>
        <div className="flex gap-1.5">
          <button
            type="button"
            className="computer-rail-arrow"
            aria-label={t('computer.card.previous', 'Previous computer')}
            disabled={active === 0}
            onClick={() => goTo(active - 1)}
          >
            <ChevronLeft className="h-3.5 w-3.5" />
          </button>
          <button
            type="button"
            className="computer-rail-arrow"
            aria-label={t('computer.card.next', 'Next computer')}
            disabled={active === count - 1}
            onClick={() => goTo(active + 1)}
          >
            <ChevronRight className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
    </div>
  );
}

interface NewComputerCardProps {
  place: RailPlace;
  onCreate: () => void;
  ownMachines: boolean;
  held: boolean;
  heldNoteId: string;
}

/** The rail's last card, which adds another. */
function NewComputerCard({ place, onCreate, ownMachines, held, heldNoteId }: NewComputerCardProps) {
  const { t } = useTranslation();
  const rail = railCard(place.active, place.onActivate);
  return (
    <article
      className={cn('computer-card is-add', place.active && 'is-active')}
      aria-label={t('computer.newComputer', 'New computer')}
      {...rail.card}
    >
      <ArtFrame tl={railOrdinal(place.index)} {...rail.content}>
        <span className="computer-card-plus" aria-hidden="true">
          <Plus className="h-5 w-5" strokeWidth={1.25} />
        </span>
      </ArtFrame>
      <div className="computer-card-body" {...rail.content}>
        <div className="computer-card-name">{t('computer.newComputer', 'New computer')}</div>
        <p className="computer-card-note">
          {ownMachines
            ? t('computer.card.newOwn', 'A cloud computer, or a machine you already have, reached over SSH.')
            : t('computer.card.newCloud', 'Another cloud computer, for work you want kept on its own machine.')}
        </p>
        <div className="computer-card-actions">
          <Button size="sm" onClick={onCreate} disabled={held} aria-describedby={held ? heldNoteId : undefined}>
            {t('computer.card.add', 'Add a computer')}
          </Button>
        </div>
      </div>
    </article>
  );
}
