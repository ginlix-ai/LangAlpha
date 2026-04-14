import { useState, useEffect, useRef, useMemo } from 'react';
import { ChevronDown } from 'lucide-react';
import { AnimatePresence, motion } from 'framer-motion';
import StepperList, { StepperTrack, EASING, type AgentPlanItem } from '@/components/ui/stepper-track';

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

function TodoDrawer({ todoData }: { todoData: TodoData | null }) {
  const [isExpanded, setIsExpanded] = useState(false);
  const wasAllCompleted = useRef(false);

  const todos = todoData?.todos;
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
      className="w-full"
      initial={{ opacity: 0, y: 4 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.35, ease: EASING }}
    >
      {/* Header: stepper track + counter + chevron */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        aria-expanded={isExpanded}
        aria-label={`Task progress ${doneCount} of ${total}`}
        className="w-full flex items-center gap-2.5"
        style={{
          background: 'transparent',
          padding: '8px 0',
          borderBottom: '1px solid var(--color-border-muted)',
        }}
      >
        <StepperTrack items={planItems} />

        <span
          className="text-xs tabular-nums flex-shrink-0"
          style={{ color: 'var(--color-text-quaternary)' }}
        >
          {doneCount}/{total}
        </span>

        <motion.div
          className="flex-shrink-0"
          style={{ color: 'var(--color-icon-muted)' }}
          animate={{ rotate: isExpanded ? 180 : 0 }}
          transition={{ duration: 0.2, ease: EASING }}
        >
          <ChevronDown className="w-3.5 h-3.5" />
        </motion.div>
      </button>

      {/* Collapsed preview: current task(s) */}
      <AnimatePresence mode="wait">
        {!isExpanded && previewItems.length > 0 && doneCount < total && (
          <motion.div
            key={previewKey}
            className="space-y-0.5"
            style={{ padding: '6px 0 2px' }}
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -4 }}
            transition={{ duration: 0.2, ease: EASING }}
          >
            {previewItems.map(({ item, index }) => (
              <div key={`preview-${index}`} className="flex items-center gap-1.5">
                {item.status === 'in_progress' && (
                  <motion.div
                    className="flex-shrink-0 rounded-full"
                    style={{
                      width: 4,
                      height: 4,
                    }}
                    animate={{ opacity: [0.4, 1, 0.4] }}
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
          </motion.div>
        )}
      </AnimatePresence>

      {/* Expanded: ticker-tape list */}
      <AnimatePresence>
        {isExpanded && (
          <motion.div
            className="overflow-hidden"
            initial={{ height: 0, opacity: 0 }}
            animate={{
              height: 'auto',
              opacity: 1,
              transition: { duration: 0.25, ease: EASING },
            }}
            exit={{
              height: 0,
              opacity: 0,
              transition: { duration: 0.2, ease: EASING },
            }}
          >
            <div style={{ maxHeight: 320, overflowY: 'auto', padding: '8px 0' }}>
              <StepperList items={planItems} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

export default TodoDrawer;
