/**
 * A list dragged into order with the mouse, a finger, or the keyboard: Space
 * or Enter on a handle picks an item up, the arrows move it, and Space or
 * Enter drops it (Escape puts it back), with each step read out.
 */
import { useMemo, useRef, type CSSProperties, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import {
  DndContext,
  KeyboardSensor,
  MouseSensor,
  TouchSensor,
  closestCenter,
  useSensor,
  useSensors,
  type Announcements,
  type DragEndEvent,
  type UniqueIdentifier,
} from '@dnd-kit/core';
import {
  SortableContext,
  rectSortingStrategy,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';

// Hoisted: dnd-kit memoizes on these objects' identity.
const MOUSE_OPTIONS = { activationConstraint: { distance: 4 } };
const TOUCH_OPTIONS = { activationConstraint: { delay: 150, tolerance: 5 } };
const KEYBOARD_OPTIONS = {
  coordinateGetter: sortableKeyboardCoordinates,
  keyboardCodes: { start: ['Space', 'Enter'], cancel: ['Escape'], end: ['Space', 'Enter'] },
};

/** A drag context over `ids`, reporting a drop as a move from one index to another. */
export function SortableArea({
  ids,
  nameOf,
  onMove,
  column = false,
  children,
}: {
  ids: string[];
  nameOf: (id: UniqueIdentifier) => string;
  onMove: (from: number, to: number) => void;
  column?: boolean;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  const sensors = useSensors(
    useSensor(MouseSensor, MOUSE_OPTIONS),
    useSensor(TouchSensor, TOUCH_OPTIONS),
    useSensor(KeyboardSensor, KEYBOARD_OPTIONS),
  );
  // Until the lifted item leaves its own slot, being over itself is not news.
  const leftSlotRef = useRef(false);
  const accessibility = useMemo(() => {
    const announcements: Announcements = {
      onDragStart: ({ active }) => {
        leftSlotRef.current = false;
        return t('common.sortable.pickedUp', { name: nameOf(active.id) });
      },
      onDragOver: ({ active, over }) => {
        if (!over) return undefined;
        if (over.id === active.id) {
          return leftSlotRef.current ? t('common.sortable.inPlace', { name: nameOf(active.id) }) : undefined;
        }
        leftSlotRef.current = true;
        return t('common.sortable.over', { name: nameOf(active.id), over: nameOf(over.id) });
      },
      onDragEnd: ({ active, over }) =>
        !over || over.id === active.id
          ? t('common.sortable.inPlace', { name: nameOf(active.id) })
          : t('common.sortable.dropped', { name: nameOf(active.id), over: nameOf(over.id) }),
      onDragCancel: ({ active }) => t('common.sortable.cancelled', { name: nameOf(active.id) }),
    };
    return { announcements, screenReaderInstructions: { draggable: t('common.sortable.instructions') } };
  }, [nameOf, t]);

  const onDragEnd = ({ active, over }: DragEndEvent) => {
    if (!over || over.id === active.id) return;
    const from = ids.indexOf(String(active.id));
    const to = ids.indexOf(String(over.id));
    if (from !== -1 && to !== -1) onMove(from, to);
  };

  return (
    <DndContext sensors={sensors} collisionDetection={closestCenter} accessibility={accessibility} onDragEnd={onDragEnd}>
      <SortableContext items={ids} strategy={column ? verticalListSortingStrategy : rectSortingStrategy}>
        {children}
      </SortableContext>
    </DndContext>
  );
}

/**
 * One item of a `SortableArea`: spread `item` on the element that moves and
 * `handle` on the one that picks it up, which is named for the drag.
 */
// eslint-disable-next-line react-refresh/only-export-components
export function useSortableItem(id: string, name: string) {
  const { t } = useTranslation();
  const { attributes, listeners, setNodeRef, setActivatorNodeRef, transform, transition, isDragging } = useSortable({ id });
  const style: CSSProperties = { transform: CSS.Translate.toString(transform), transition };
  return {
    isDragging,
    item: { ref: setNodeRef, style },
    handle: { ref: setActivatorNodeRef, ...attributes, ...listeners, 'aria-label': t('common.sortable.handle', { name }) },
  };
}
