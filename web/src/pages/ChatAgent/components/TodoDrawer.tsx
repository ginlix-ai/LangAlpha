import { useState, useEffect, useRef, useMemo } from 'react';
import { ChevronDown } from 'lucide-react';
import { Disclosure } from '@/components/ui/Disclosure';
import { SwapCell } from '@/components/ui/crossfade';
import { motion } from '@/lib/framer';
import { useOnClickOutside } from '@/hooks/useOnClickOutside';
import StepperList, { StepperTrack, type AgentPlanItem } from '@/components/ui/stepper-track';
import { TWEEN } from '@/lib/motion';
import { useMotionSafe } from '@/lib/reducedMotion';

export interface TodoItem {
  status: 'pending' | 'in_progress' | 'completed' | 'stale';
  activeForm?: string;
  content?: string;
  [key: string]: unknown;
}

export interface TodoData {
  todos: TodoItem[];
  total: number;
  completed: number;
  in_progress: number;
  pending: number;
}

/**
 * Get items to display in collapsed view.
 * - If any in_progress: return ALL in_progress items
 * - Otherwise fallback to single most relevant: last stale > last completed > first pending > first item
 */
export function getPreviewItems(todos: TodoItem[]): { item: TodoItem; index: number }[] {
  if (!todos || todos.length === 0) return [];

  const inProgress = todos
    .map((item, index) => ({ item, index }))
    .filter(({ item }) => item.status === 'in_progress');
  if (inProgress.length > 0) return inProgress;

  for (let i = todos.length - 1; i >= 0; i--) {
    if (todos[i].status === 'stale') return [{ item: todos[i], index: i }];
  }

  for (let i = todos.length - 1; i >= 0; i--) {
    if (todos[i].status === 'completed') return [{ item: todos[i], index: i }];
  }

  const pendingIdx = todos.findIndex(t => t.status === 'pending');
  if (pendingIdx !== -1) return [{ item: todos[pendingIdx], index: pendingIdx }];

  return [{ item: todos[0], index: 0 }];
}

/** Map TodoItem[] to AgentPlanItem[] for the UI component. */
export function toAgentPlanItems(todos: TodoItem[]): AgentPlanItem[] {
  return todos.map((todo, i) => ({
    id: todo.activeForm || todo.content || `task-${i}`,
    label: todo.activeForm || todo.content || `Task ${i + 1}`,
    status: todo.status,
  }));
}

function TodoDrawer({ todoData, historyLoading }: { todoData: TodoData | null; historyLoading: boolean }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const wasAllCompleted = useRef(false);
  const drawerRef = useRef<HTMLDivElement>(null);
  // The dot's pulse is an opacity loop, which framer's reduced-motion setting
  // leaves running.
  const motionSafe = useMotionSafe();

  // The expanded list covers the transcript, so any press outside it closes it.
  useOnClickOutside(drawerRef, () => setIsExpanded(false), isExpanded);

  const todos = Array.isArray(todoData?.todos) ? todoData.todos : undefined;

  // Todos that arrive with the thread's history appear with its transcript, as
  // they do on a cached thread; only a list the agent writes later animates in.
  const [restored, setRestored] = useState(historyLoading || !!todos?.length);
  if (restored && !historyLoading && !todos?.length) setRestored(false);
  const total = todoData?.total || 0;
  const completed = todoData?.completed || 0;

  const staleCount = todos?.filter(t => t.status === 'stale').length || 0;
  const doneCount = completed + staleCount;

  // Auto-collapse when all todos are done (completed or stale)
  useEffect(() => {
    const allDone = total > 0 && doneCount === total;
    if (allDone && !wasAllCompleted.current) {
      setIsExpanded(false);
    }
    wasAllCompleted.current = allDone;
  }, [doneCount, total]);

  const planItems = useMemo(
    () => (todos ? toAgentPlanItems(todos) : []),
    [todos],
  );

  if (!todoData || !todos || todos.length === 0) {
    return null;
  }

  const previewItems = getPreviewItems(todos);
  const previewKey = previewItems
    .map(p => `${p.index}-${p.item.status}-${p.item.activeForm || p.item.content}`)
    .join('|');

  return (
    <motion.div
      ref={drawerRef}
      className="mx-4 rounded-t-2xl border border-b-0 border-(--color-border-input) bg-(--color-bg-card) px-3 pb-4 -mb-4"
      // A tab of the composer, in its fill and hairline: inset from its edges,
      // only the top corners rounded, and the bottom pb-4 tucked under its top
      // edge (the composer is the later sibling, so it paints over the tuck),
      // so the visible tab ends where its content does.
      initial={restored ? false : { opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={TWEEN.enter}
    >
      {/* Header: stepper track + counter + chevron */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        aria-expanded={isExpanded}
        aria-label={`Task progress ${doneCount} of ${total}`}
        className="w-full flex items-center gap-2.5"
        style={{
          background: 'transparent',
          padding: '6px 0',
        }}
      >
        <StepperTrack items={planItems} />

        <span
          className="text-xs tabular-nums shrink-0"
          style={{ color: 'var(--color-text-quaternary)' }}
        >
          {doneCount}/{total}
        </span>

        <motion.div
          className="shrink-0"
          style={{ color: 'var(--color-icon-muted)' }}
          animate={{ rotate: isExpanded ? 180 : 0 }}
          transition={TWEEN.fold}
        >
          <ChevronDown className="w-3.5 h-3.5" />
        </motion.div>
      </button>

      {/* Collapsed preview: current task(s). A change of task dissolves in one
          grid cell, so the line never leaves the drawer empty between two. */}
      <SwapCell
        as="div"
        className="items-start"
        layerClassName="space-y-0.5 pb-1.5"
        swapKey={!isExpanded && previewItems.length > 0 && doneCount < total ? previewKey : null}
        shift={{ y: 4 }}
        appear
      >
        {previewItems.map(({ item, index }) => (
          <div key={`preview-${index}`} className="flex items-center gap-1.5">
            {item.status === 'in_progress' && (
              <motion.div
                className="shrink-0 rounded-full"
                style={{
                  width: 4,
                  height: 4,
                }}
                animate={motionSafe ? { opacity: [0.4, 1, 0.4] } : undefined}
                // eslint-disable-next-line no-restricted-syntax -- the pulse's loop period, not a transition between states
                transition={{ duration: 1.5, repeat: Infinity, ease: 'easeInOut' }}
              >
                <div
                  className="w-full h-full rounded-full"
                  style={{
                    background: 'var(--color-text-secondary)',
                  }}
                />
              </motion.div>
            )}

            <span
              className="text-sm truncate"
              style={{
                color: item.status === 'in_progress'
                  ? 'var(--color-text-primary)'
                  : 'var(--color-text-secondary)',
              }}
            >
              {item.activeForm || item.content || `Task ${index + 1}`}
            </span>
          </div>
        ))}
      </SwapCell>

      {/* Expanded: ticker-tape list */}
      <Disclosure open={isExpanded}>
        <div style={{ maxHeight: 320, overflowY: 'auto', padding: '0 0 4px' }}>
          <StepperList items={planItems} />
        </div>
      </Disclosure>
    </motion.div>
  );
}

export default TodoDrawer;
