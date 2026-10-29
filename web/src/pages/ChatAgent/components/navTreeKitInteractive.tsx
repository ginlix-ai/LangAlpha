/* eslint-disable react-refresh/only-export-components -- a kit is an object of components, chosen at runtime */
/**
 * The nav tree's interactive kit: measured expand and collapse, framer thread
 * glides, and dnd-kit workspace reordering. ./navTreeKit loads it after first
 * paint. Each piece draws what its static twin there draws, so the swap
 * changes behaviour and not pixels.
 */
import type React from 'react';
import { createPortal } from 'react-dom';
import { Disclosure, PresenceFold } from '@/components/ui/Disclosure';
import { AnimatePresence, MotionConfig, motion, type Transition } from '@/lib/framer';
import { SPRING_INDICATOR, TWEEN } from '@/lib/motion';
import {
  DndContext,
  DragOverlay,
  MeasuringStrategy,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import { SortableContext, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  DRAG_ACTIVATION_DISTANCE,
  DRAG_HANDLE_ATTR,
  SECTION_STYLE,
  type NavTreeKit,
  type SectionProps,
  type TreeRootProps,
} from './navTreeKit';

const DND_MEASURING = { droppable: { strategy: MeasuringStrategy.Always } };
// Hoisted with DND_MEASURING: dnd-kit memoizes on these objects' identity,
// and an inline literal rebuilt the drag context on every render of the
// panel, which re-rendered every sortable workspace row through it. The
// activation distance keeps plain clicks toggling expand/collapse instead of
// starting a drag (the gallery's cards use the same distance).
const POINTER_SENSOR_OPTIONS = { activationConstraint: { distance: DRAG_ACTIVATION_DISTANCE } };

function TreeRoot({ ids, onDragStart, onDragEnd, onDragCancel, clampOverlay, overlay, children }: TreeRootProps) {
  const sensors = useSensors(useSensor(PointerSensor, POINTER_SENSOR_OPTIONS));
  return (
    // The desktop sidebar sits outside every lazy route's MotionConfig, so the
    // tree brings its own.
    <MotionConfig reducedMotion="user">
      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        measuring={DND_MEASURING}
        onDragStart={onDragStart}
        onDragEnd={onDragEnd}
        onDragCancel={onDragCancel}
      >
        <SortableContext items={ids} strategy={verticalListSortingStrategy}>
          {children}
        </SortableContext>
        {/* Compact lift preview: a content-hugging header pill that follows
            the cursor, so the dragged section's real size never distorts.
            Portaled to <body>: the overlay is position:fixed, and any sidebar
            ancestor gaining a transform/filter/will-change would become its
            containing block and drift the chip off the cursor (the mount
            animation's fill mode did exactly that once). zIndex must clear
            the sidebar's 1000. */}
        {createPortal(
          <DragOverlay dropAnimation={null} zIndex={1100} modifiers={[clampOverlay]}>
            {overlay}
          </DragOverlay>,
          document.body,
        )}
      </DndContext>
    </MotionConfig>
  );
}

/**
 * Sortable wrapper for one workspace section (header row + thread sub-list).
 * The header row receives the drag listeners via the render prop, which also
 * gets `isDragging` so the section can collapse to header height while lifted.
 *
 * Translate-only (not Transform) so displaced siblings never pick up the
 * scaleX/scaleY that distorts variable-height rows; the lifted item itself is
 * hidden here and shown as a fixed-size DragOverlay chip instead.
 */
function Section({ wsId, disabled, children }: SectionProps) {
  // dnd-kit back-compat trap: a boolean `disabled` normalizes to
  // {draggable, droppable: false}, which leaves the row an active drop
  // target. Spell out both aspects so `true` really means fully disabled.
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: wsId,
    disabled: typeof disabled === 'boolean' ? { draggable: disabled, droppable: disabled } : disabled,
  });
  const dragDisabled = typeof disabled === 'boolean' ? disabled : disabled.draggable;
  const style: React.CSSProperties = {
    ...SECTION_STYLE,
    transform: CSS.Translate.toString(transform),
    transition,
    opacity: isDragging ? 0 : 1,
    zIndex: isDragging ? 5 : undefined,
  };
  return (
    <div ref={setNodeRef} style={style} data-ws-id={wsId}>
      {children({
        dragHandleProps: dragDisabled ? {} : { ...attributes, ...listeners, [DRAG_HANDLE_ATTR]: '' },
        isDragging,
      })}
    </div>
  );
}

// A workspace's thread block opens and shuts in the measured fold.
function Collapse({ show, children }: { show: boolean; children: React.ReactNode }) {
  return <Disclosure open={show}>{children}</Disclosure>;
}

// initial={false} keeps the first paint of an expanded workspace static.
function ThreadList({ children }: { children: React.ReactNode }) {
  return <AnimatePresence initial={false}>{children}</AnimatePresence>;
}

const THREAD_GLIDE: Transition = { layout: TWEEN.fold };

// layout="position": pin/unpin repartitions and chat bumps glide to their new
// slot instead of teleporting; archived rows fold out, and unarchived and new
// ones fold in.
function ThreadItem({ order, children }: { order: string; children: React.ReactNode }) {
  return (
    <motion.div
      layout="position"
      // Without a layoutDependency every panel render re-measures every row
      // (getBoundingClientRect + projection walk); the id signature scopes
      // that to genuine reorders.
      layoutDependency={order}
      transition={THREAD_GLIDE}
    >
      <PresenceFold appear>{children}</PresenceFold>
    </motion.div>
  );
}

// Keyed remount pops the glyph when pin state flips: click acknowledgment
// before the row starts its glide.
function PinGlyph({ pinned, children }: { pinned: boolean; children: React.ReactNode }) {
  return (
    <motion.span
      key={pinned ? 'pinned' : 'unpinned'}
      className="flex"
      initial={{ scale: 0.6 }}
      animate={{ scale: 1 }}
      transition={SPRING_INDICATOR}
    >
      {children}
    </motion.span>
  );
}

const interactiveKit: NavTreeKit = { TreeRoot, Section, Collapse, ThreadList, ThreadItem, PinGlyph };

export default interactiveKit;
