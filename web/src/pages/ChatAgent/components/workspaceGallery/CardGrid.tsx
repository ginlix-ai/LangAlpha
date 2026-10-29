/**
 * The gallery's grid of workspace cards, and dragging them into the user's own
 * order.
 *
 * Cards drag only when the grid shows that order over the whole list: Custom,
 * with no search and no computer filter. A partial list cannot say where a
 * card goes among the ones it leaves out, and in Recent or Name a drop would
 * be undone by the next read. Elsewhere the cards are simply links. Where
 * they drag, they hold still for a moment while a drop is saved or the list is
 * read again: a drop then would move from an order about to be replaced.
 *
 * A mouse drags once it has moved a few pixels, so a click still opens the
 * workspace; a finger presses and holds, so a swipe still scrolls; a keyboard
 * picks a card up with Space, moves it with the arrows and drops it with Space
 * (Enter opens a card, as it does any link). The pinned block stays ahead of
 * the rest: a card is never dropped across the line between them.
 */
import { useCallback, useMemo, useRef, useState, type HTMLAttributes, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import {
  DndContext,
  DragOverlay,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  closestCenter,
  useSensor,
  useSensors,
  type Announcements,
  type DragEndEvent,
  type DragStartEvent,
  type UniqueIdentifier,
} from '@dnd-kit/core';
import { SortableContext, rectSortingStrategy, sortableKeyboardCoordinates, useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

import { isEffectivelyPinned } from '../../hooks/useNavigationData';
import { GALLERY_GRID } from './layout';
import type { WorkspaceRecord } from './types';

// Hoisted: dnd-kit memoizes on these objects' identity. The distance matches
// the sidebar tree's, so a press reads the same in both.
const MOUSE_OPTIONS = { activationConstraint: { distance: 8 } };
const TOUCH_OPTIONS = { activationConstraint: { delay: 250, tolerance: 8 } };
const KEYBOARD_OPTIONS = {
  coordinateGetter: sortableKeyboardCoordinates,
  // Enter is the card's own: it opens the workspace.
  keyboardCodes: { start: ['Space'], cancel: ['Escape'], end: ['Space', 'Enter'] },
};
// Up and down only: the grid scrolls one way, and a sideways nudge would move
// whatever else scrolls under the pointer.
const AUTO_SCROLL = { threshold: { x: 0, y: 0.15 } };

/** What a card is told about dragging. */
export interface CardDrag {
  /** Spread on the element a press, a hold or a key lifts the card by. Absent when it cannot drag. */
  handle?: {
    ref: (element: HTMLElement | null) => void;
    props: HTMLAttributes<HTMLElement>;
  };
  /** `placeholder`: the slot the lifted card left. `lifted`: the copy that follows the pointer. */
  state: 'resting' | 'placeholder' | 'lifted';
  /** A keyboard drag is under way: the key that ends it must not also open the card. */
  dragging: boolean;
  /** Could drag but for a save or a read in flight: Space is still the drag's, not the card's. */
  held: boolean;
}

interface CardGridProps {
  workspaces: readonly WorkspaceRecord[];
  /** Custom, over the whole list. */
  canDrag: boolean;
  /** A drop is being saved: the cards hold still until it lands. */
  saving: boolean;
  /** The list is being read again: no card lifts until it lands. */
  refreshing: boolean;
  /** Take a drop. False when it is turned away (a save still in flight), so it is not announced as dropped. */
  onMove: (activeId: string, overId: string) => boolean;
  /** The name a card shows, for what a screen reader is told. */
  label: (workspace: WorkspaceRecord) => string;
  renderCard: (workspace: WorkspaceRecord, index: number, drag: CardDrag) => ReactNode;
}

export function CardGrid({ workspaces, canDrag, saving, refreshing, onMove, label, renderCard }: CardGridProps) {
  const { t } = useTranslation();
  const [activeId, setActiveId] = useState<UniqueIdentifier | null>(null);
  const sensors = useSensors(
    useSensor(MouseSensor, MOUSE_OPTIONS),
    useSensor(TouchSensor, TOUCH_OPTIONS),
    useSensor(KeyboardSensor, KEYBOARD_OPTIONS),
  );

  const ids = useMemo(() => workspaces.map((ws) => ws.workspace_id), [workspaces]);
  const byId = useMemo(() => new Map(workspaces.map((ws) => [ws.workspace_id, ws] as const)), [workspaces]);
  const active = activeId != null ? byId.get(String(activeId)) ?? null : null;
  const nameOf = useCallback((id: UniqueIdentifier | undefined) => {
    const ws = id != null ? byId.get(String(id)) : undefined;
    return ws ? label(ws) : '';
  }, [byId, label]);

  // Whether the lifted card has left its own slot yet: until it has, being
  // over itself is not news, and would talk over "picked up".
  const leftSlotRef = useRef(false);
  // Whether the last drop was taken. Set by the drop handler, which dnd-kit
  // runs before it asks for the announcement.
  const takenRef = useRef(true);
  const accessibility = useMemo(() => {
    const announcements: Announcements = {
      onDragStart: ({ active: a }) => {
        leftSlotRef.current = false;
        return t('workspace.dragPickedUp', { name: nameOf(a.id) });
      },
      onDragOver: ({ active: a, over }) => {
        if (!over) return undefined;
        if (over.id === a.id) return leftSlotRef.current ? t('workspace.dragInPlace', { name: nameOf(a.id) }) : undefined;
        leftSlotRef.current = true;
        return t('workspace.dragOver', { name: nameOf(a.id), over: nameOf(over.id) });
      },
      onDragEnd: ({ active: a, over }) => {
        if (!over || over.id === a.id) return t('workspace.dragNotMoved', { name: nameOf(a.id) });
        return takenRef.current
          ? t('workspace.dragDropped', { name: nameOf(a.id), over: nameOf(over.id) })
          : t('workspace.dragStillSaving', { name: nameOf(a.id) });
      },
      onDragCancel: ({ active: a }) => t('workspace.dragCancelled', { name: nameOf(a.id) }),
    };
    return { announcements, screenReaderInstructions: { draggable: t('workspace.dragInstructions') } };
  }, [nameOf, t]);

  const handleDragStart = (event: DragStartEvent) => setActiveId(event.active.id);
  const handleDragEnd = (event: DragEndEvent) => {
    setActiveId(null);
    const { active: a, over } = event;
    takenRef.current = over && a.id !== over.id ? onMove(String(a.id), String(over.id)) : true;
  };
  // Held still, not for good: Space still belongs to the drag, not to opening the card.
  const held = canDrag && (saving || refreshing);

  return (
    <DndContext
      sensors={sensors}
      collisionDetection={closestCenter}
      autoScroll={AUTO_SCROLL}
      accessibility={accessibility}
      onDragStart={handleDragStart}
      onDragEnd={handleDragEnd}
      onDragCancel={() => setActiveId(null)}
    >
      <SortableContext items={ids} strategy={rectSortingStrategy}>
        <div
          className={GALLERY_GRID}
          data-testid="workspace-card-grid"
          data-draggable={canDrag && !held ? 'true' : undefined}
        >
          {workspaces.map((ws, index) => {
            // While a card is up, the other block's cards are not places it can land.
            const otherBlock = !!active && ws.workspace_id !== active.workspace_id
              && isEffectivelyPinned(ws) !== isEffectivelyPinned(active);
            return (
              <SortableCard
                key={ws.workspace_id}
                id={ws.workspace_id}
                draggable={canDrag && !held}
                droppable={canDrag && !saving && !otherBlock}
                dragging={activeId != null}
                held={held}
              >
                {(drag) => renderCard(ws, index, drag)}
              </SortableCard>
            );
          })}
        </div>
      </SortableContext>
      {/* On <body>: the overlay is fixed, and a transformed ancestor would
          carry it off the pointer. */}
      {createPortal(
        <DragOverlay zIndex={60}>
          {active ? renderCard(active, ids.indexOf(active.workspace_id), { state: 'lifted', dragging: true, held: false }) : null}
        </DragOverlay>,
        document.body,
      )}
    </DndContext>
  );
}

interface SortableCardProps {
  id: string;
  draggable: boolean;
  droppable: boolean;
  dragging: boolean;
  held: boolean;
  children: (drag: CardDrag) => ReactNode;
}

function SortableCard({ id, draggable, droppable, dragging, held, children }: SortableCardProps) {
  const { attributes, listeners, setNodeRef, setActivatorNodeRef, transform, transition, isDragging } = useSortable({
    id,
    // Both halves spelled out: a bare boolean leaves the card a drop target.
    disabled: { draggable: !draggable, droppable: !droppable },
  });
  const handle = draggable
    ? { ref: setActivatorNodeRef, props: { ...attributes, ...listeners } as HTMLAttributes<HTMLElement> }
    : undefined;
  return (
    <div
      ref={setNodeRef}
      className="min-w-0"
      // Translate only: every card is the same size, and a scale would stretch
      // the one sliding into a slot of another shape mid-reflow.
      style={{ transform: CSS.Translate.toString(transform), transition }}
    >
      {children({ handle, state: isDragging ? 'placeholder' : 'resting', dragging, held })}
    </div>
  );
}

export default CardGrid;
