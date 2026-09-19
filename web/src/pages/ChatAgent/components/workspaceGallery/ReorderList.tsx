/**
 * Reorder mode: the gallery's drag-to-order list.
 *
 * Its own component because it is the one place that mirrors server rows into
 * local state, which dnd-kit needs to move a row before the write lands. The
 * mirror is confined here so the paginated gallery keeps reading React Query
 * alone, and it is fed by its own full-list query rather than the gallery's
 * page.
 */
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, GripVertical, Pin, Zap } from 'lucide-react';
import { DndContext, closestCenter, PointerSensor, useSensor, useSensors } from '@dnd-kit/core';
import type { DragEndEvent } from '@dnd-kit/core';
import { SortableContext, useSortable, arrayMove, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { useQueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';

import { useWorkspaces } from '../../../../hooks/useWorkspaces';
import { reorderWorkspaces } from '../../utils/api';
import { isEffectivelyPinned } from '../../hooks/useNavigationData';
import type { WorkspaceRecord } from './types';

interface SortableReorderRowProps {
  workspace: WorkspaceRecord;
  disabled: boolean | { draggable: boolean; droppable: boolean };
}

/** Sortable row for reorder mode: a compact single-column list item. */
function SortableReorderRow({ workspace, disabled }: SortableReorderRowProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id: workspace.workspace_id,
    // dnd-kit back-compat trap: a boolean `disabled` normalizes to
    // {draggable, droppable: false}, the row would stay an active drop
    // target. Spell out both aspects so `true` really means fully disabled.
    disabled: typeof disabled === 'boolean' ? { draggable: disabled, droppable: disabled } : disabled,
  });

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
    zIndex: isDragging ? 50 : undefined,
  };

  const isFlash = workspace.status === 'flash';

  return (
    <div
      ref={setNodeRef}
      className="flex items-center gap-3 px-4 py-3 rounded-xl border mb-2"
      style={{
        ...style,
        // Same material split as the gallery card: flash rides an elevated
        // surface, user rows keep the card wash; the Zap glyph is the accent.
        background: isFlash
          ? 'var(--color-bg-elevated)'
          : 'var(--color-bg-card-gradient, var(--color-border-muted))',
        borderColor: isFlash ? 'var(--color-border-default)' : 'var(--color-bg-card-border, var(--color-border-muted))',
      }}
    >
      {/* Flash drags too (within the pinned block), its Zap identity glyph
          doubles as the grab handle where user rows show the grip. */}
      <button
        {...listeners}
        {...attributes}
        className="flex-shrink-0 cursor-grab active:cursor-grabbing p-1 rounded"
        style={{ color: isFlash ? 'var(--color-accent-primary)' : 'var(--color-text-tertiary)' }}
      >
        {isFlash ? <Zap className="h-5 w-5" /> : <GripVertical className="h-5 w-5" />}
      </button>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          {!isFlash && workspace.is_pinned && (
            <Pin className="h-3.5 w-3.5 flex-shrink-0 rotate-45" style={{ color: 'var(--color-text-tertiary)' }} />
          )}
          <span className="font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
            {workspace.name}
          </span>
        </div>
      </div>
    </div>
  );
}

interface ReorderListProps {
  /** Flash rides in the pinned block, so it is part of the order being written. */
  flashWorkspace: WorkspaceRecord | null;
  /** Done: `true` when a drag actually landed, which is what pins 'custom' sort. */
  onDone: (didReorder: boolean) => void;
}

export function ReorderList({ flashWorkspace, onDone }: ReorderListProps) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [rows, setRows] = useState<WorkspaceRecord[]>([]);
  // Lifted row during a drag, rows across the pin boundary from it stop being
  // drop targets so the preview never shows a refused arrangement.
  const [activeId, setActiveId] = useState<string | null>(null);
  const didReorderRef = useRef(false);

  // Reorder needs every row, not the gallery's page.
  const { data: allWsData } = useWorkspaces({ limit: 100, offset: 0, sortBy: 'custom' });

  useEffect(() => {
    if (!allWsData?.workspaces) return;
    const list = allWsData.workspaces as WorkspaceRecord[];
    setRows(flashWorkspace ? [flashWorkspace, ...list] : list);
  }, [allWsData, flashWorkspace]);

  // DnD sensors -- require 8px drag distance before activating
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 8 } }),
  );

  // Pinned block first, Flash counts as always-pinned, then sort_order, then
  // recency. Sort keys are computed once per item, not per comparison, and the
  // whole sort re-runs only when the list changes, not on every drag-position
  // render.
  const sortedRows = useMemo(() => {
    const keyed = rows.map((ws) => ({
      ws,
      pinned: isEffectivelyPinned(ws) ? 1 : 0,
      order: ws.sort_order ?? 0,
      updated: new Date(ws.updated_at || 0).getTime(),
    }));
    keyed.sort((a, b) => {
      if (a.pinned !== b.pinned) return b.pinned - a.pinned;
      if (a.order !== b.order) return a.order - b.order;
      return b.updated - a.updated;
    });
    return keyed.map((k) => k.ws);
  }, [rows]);

  const sortedIds = sortedRows.map((ws) => ws.workspace_id);
  const activeWs = activeId
    ? sortedRows.find((w) => w.workspace_id === activeId) ?? null
    : null;

  const handleDragEnd = async (event: DragEndEvent) => {
    setActiveId(null);
    const { active, over } = event;
    if (!over || active.id === over.id) return;

    const oldIndex = sortedRows.findIndex((ws) => ws.workspace_id === active.id);
    const newIndex = sortedRows.findIndex((ws) => ws.workspace_id === over.id);
    if (oldIndex === -1 || newIndex === -1) return;

    const draggedWs = sortedRows[oldIndex];
    const targetWs = sortedRows[newIndex];

    // Prevent crossing the pin boundary. No flash special case: it counts as
    // pinned, so this both contains it in the pinned block and keeps
    // unpinned rows out.
    if (isEffectivelyPinned(draggedWs) !== isEffectivelyPinned(targetWs)) return;

    const reordered = arrayMove(sortedRows, oldIndex, newIndex);

    // Assign sequential sort_order. Flash is included: it's DB-pinned with a
    // real sort_order, and writing its slot is what makes "pinned workspace
    // above/below Flash" stick, omitting it leaves a sort_order tie decided
    // by updated_at, so the pinned block would reshuffle whenever Flash is used.
    const items = reordered.map((ws, i) => ({ workspace_id: ws.workspace_id, sort_order: i }));

    // Optimistic update
    const snapshot = rows;
    setRows(rows.map((ws) => {
      const item = items.find((it) => it.workspace_id === ws.workspace_id);
      return item ? { ...ws, sort_order: item.sort_order } : ws;
    }));

    try {
      await reorderWorkspaces(items);
      didReorderRef.current = true;
      queryClient.invalidateQueries({ queryKey: queryKeys.workspaces.lists() });
    } catch (err) {
      console.error('Error reordering workspaces:', err);
      setRows(snapshot); // rollback
    }
  };

  return (
    <div className="flex-1 min-h-0 flex flex-col">
      <div className="flex items-center justify-between px-1 pb-3 flex-shrink-0">
        <span className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>
          {t('workspace.dragToReorder')}
        </span>
        <button
          onClick={() => onDone(didReorderRef.current)}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium transition-opacity hover:opacity-90"
          style={{
            backgroundColor: 'var(--color-btn-primary-bg)',
            color: 'var(--color-btn-primary-text)',
          }}
        >
          <Check className="h-4 w-4" />
          {t('common.done')}
        </button>
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto px-1 pb-4">
        <DndContext
          sensors={sensors}
          collisionDetection={closestCenter}
          onDragStart={(e) => setActiveId(String(e.active.id))}
          onDragCancel={() => setActiveId(null)}
          onDragEnd={handleDragEnd}
        >
          <SortableContext items={sortedIds} strategy={verticalListSortingStrategy}>
            {sortedRows.map((ws) => {
              const crossBlock = !!activeWs && ws.workspace_id !== activeId &&
                isEffectivelyPinned(ws) !== isEffectivelyPinned(activeWs);
              return (
                <SortableReorderRow
                  key={ws.workspace_id}
                  workspace={ws}
                  disabled={crossBlock ? { draggable: false, droppable: true } : false}
                />
              );
            })}
          </SortableContext>
        </DndContext>
      </div>
    </div>
  );
}

export default ReorderList;
