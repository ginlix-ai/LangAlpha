import React, { useState } from 'react';
import { CheckCircle2, ChevronDown, ChevronUp, Circle, Loader2 } from 'lucide-react';

/**
 * TodoDrawer Component
 *
 * A collapsible drawer that displays todo list above the chat input.
 * Features:
 * - Shows progress summary when collapsed
 * - Shows full todo list when expanded
 * - Positioned above chat input with lower z-index
 *
 * @param {Object} props
 * @param {Object} props.todoData - Todo data from floating cards { todos, total, completed, in_progress, pending }
 */
function TodoDrawer({ todoData }) {
  const [isExpanded, setIsExpanded] = useState(false);

  // Don't render if no todo data
  if (!todoData || !todoData.todos || todoData.todos.length === 0) {
    return null;
  }

  const { todos, total, completed, in_progress, pending } = todoData;

  /**
   * Get icon for todo item based on status
   */
  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed':
        return (
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="1em"
            height="1em"
            viewBox="0 0 1024 1024"
            className="w-4 h-4"
            style={{ color: 'currentColor' }}
          >
            <path
              d="M89.6 512c0-233.301333 189.098667-422.4 422.4-422.4s422.4 189.098667 422.4 422.4-189.098667 422.4-422.4 422.4-422.4-189.098667-422.4-422.4zM512 166.4a345.6 345.6 0 1 0 0 691.2 345.6 345.6 0 1 0 0-691.2z"
              fill="currentColor"
            />
            <path
              d="M731.136 365.397333a38.4 38.4 0 0 1 0 54.272l-255.445333 255.488a38.4 38.4 0 0 1-54.314667 0l-116.138667-116.138666a38.4 38.4 0 0 1 54.314667-54.314667l88.96 89.002667 228.309333-228.309334a38.4 38.4 0 0 1 54.314667 0z"
              fill="currentColor"
            />
          </svg>
        );
      case 'in_progress':
        return <Loader2 className="w-4 h-4 animate-spin" style={{ color: 'currentColor' }} />;
      case 'pending':
      default:
        return <Circle className="w-4 h-4" style={{ color: 'currentColor' }} />;
    }
  };

  return (
    <div
      className="w-full border border-[rgba(255,255,255,0.1)] rounded-lg overflow-hidden"
      style={{
        backgroundColor: 'rgba(27, 29, 37, 0.95)',
        backdropFilter: 'blur(8px)',
      }}
    >
      {/* Header - Always visible */}
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between px-4 py-3 hover:bg-white/5 transition-colors"
      >
        <div className="flex items-center gap-2">
          {/* Robot Icon */}
          <svg
            xmlns="http://www.w3.org/2000/svg"
            width="1em"
            height="1em"
            viewBox="0 0 1024 1024"
            className="w-5 h-5"
            style={{ color: '#6155F5' }}
          >
            <path
              d="M512 132.266667c197.589333 0 291.84 21.333333 333.354667 35.84 2.986667 1.066667 8.96 3.029333 15.018666 6.4 6.698667 3.712 12.117333 8.234667 17.152 13.226666 5.418667 5.376 10.026667 11.093333 13.824 17.962667 3.285333 6.101333 5.376 12.245333 6.613334 15.701333 13.824 38.741333 36.437333 121.472 36.437333 247.978667 0 129.152-23.552 210.56-37.418667 247.253333-1.109333 2.858667-3.114667 8.362667-6.272 13.952-3.498667 6.186667-7.68 11.264-12.416 16.085334a78.506667 78.506667 0 0 1-14.933333 12.288c-5.546667 3.413333-10.794667 5.461333-13.226667 6.4-2.432 1.024-5.12 1.877333-8.021333 2.901333l-12.970667 74.325333c-1.578667 9.045333-3.242667 19.968-10.794666 33.066667-3.456 5.973333-8.192 11.434667-11.52 14.976a77.781333 77.781333 0 0 1-42.752 24.021333c-39.210667 9.130667-114.176 19.754667-252.074667 19.754667s-212.906667-10.666667-252.074667-19.754667a77.738667 77.738667 0 0 1-42.752-24.021333 84.309333 84.309333 0 0 1-11.52-14.933333c-7.552-13.141333-9.216-24.064-10.794666-33.109334l-13.013334-74.325333c-2.858667-1.024-5.546667-1.92-7.978666-2.858667-2.432-1.024-7.68-3.072-13.226667-6.4a78.506667 78.506667 0 0 1-14.933333-12.330666 77.312 77.312 0 0 1-12.416-16.085334c-3.157333-5.546667-5.162667-11.093333-6.229334-13.952-13.909333-36.693333-37.461333-118.101333-37.461333-247.253333 0-126.506667 22.613333-209.237333 36.437333-247.978667 1.28-3.456 3.328-9.6 6.613334-15.701333 3.797333-6.912 8.405333-12.586667 13.824-17.92 5.034667-5.034667 10.453333-9.557333 17.152-13.269333 6.058667-3.370667 12.032-5.333333 15.061333-6.4 41.429333-14.506667 135.68-35.84 333.312-35.84z m248.533333 656.64c-55.125333 9.685333-134.784 17.493333-248.533333 17.493333-113.792 0-193.450667-7.808-248.576-17.493333l7.082667 40.448 0.981333 5.418666 0.426667 1.792 1.024 1.28 1.237333 1.194667c0.64 0.170667 1.493333 0.426667 3.2 0.810667 31.488 7.338667 100.138667 17.749333 234.624 17.749333 134.485333 0 203.093333-10.410667 234.624-17.749333l3.157333-0.810667 1.28-1.194667 1.024-1.28 0.426667-1.834666 0.981333-5.418667 7.04-40.405333z m-11.136 50.56l0.085334-0.085334 0.085333-0.085333-0.213333 0.170667zM512 209.066667c-192.981333 0-277.76 20.992-308.010667 31.573333l-2.218666 0.768-0.469334 0.128-1.408 1.408c-0.384 0.896-0.768 2.005333-1.578666 4.266667C187.306667 278.186667 166.4 352.170667 166.4 469.333333c0 119.594667 21.76 191.744 32.469333 220.032l0.853334 2.176 0.256 0.682667 0.597333 0.64 0.682667 0.64 1.322666 0.597333c27.392 11.008 110.848 35.413333 309.418667 35.413334s282.026667-24.405333 309.418667-35.413334l1.28-0.597333 0.725333-0.64 0.597333-0.64 0.256-0.682667 0.853334-2.133333c10.666667-28.330667 32.426667-100.48 32.426666-220.074667 0-117.162667-20.821333-191.189333-31.872-222.165333l-1.621333-4.266667c-0.128-0.170667-0.341333-0.426667-0.64-0.682666l-0.768-0.725334-0.426667-0.128-2.218666-0.768c-30.293333-10.581333-115.029333-31.573333-308.010667-31.573333z m153.6 362.752v76.8H358.4v-76.8h307.2z m76.8 0h-76.8v-76.8h76.8v76.8z m-384 0H281.6v-76.8h76.8v76.8z m153.642667-76.842667h-76.8V418.133333H512v76.8z m76.8-0.042667H512L512 341.333333h76.8v153.6zM358.4 375.466667H281.6V298.666667h76.8v76.8z m384 0H665.6V298.666667h76.8v76.8z"
              fill="currentColor"
            />
          </svg>
          <span className="text-sm font-medium" style={{ color: '#FFFFFF' }}>
            Task Progress {completed}/{total}
          </span>
        </div>

        {/* Expand/Collapse Icon */}
        <div style={{ color: 'rgba(255, 255, 255, 0.6)' }}>
          {isExpanded ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </div>
      </button>

      {/* Expanded Content - Todo List */}
      {isExpanded && (
        <div
          className="border-t"
          style={{
            borderColor: 'rgba(255, 255, 255, 0.1)',
            maxHeight: '320px',
            overflowY: 'auto',
          }}
        >
          <div className="p-3 space-y-2">
            {todos.map((todo, index) => (
              <div
                key={`todo-${index}-${todo.activeForm || index}`}
                className="flex items-start gap-2"
              >
                {/* Status Icon */}
                <div
                  className="flex-shrink-0 mt-0.5"
                  style={{
                    color:
                      todo.status === 'completed'
                        ? '#0FEDBE'
                        : todo.status === 'in_progress'
                        ? '#6155F5'
                        : 'rgba(255, 255, 255, 0.5)',
                  }}
                >
                  {getStatusIcon(todo.status)}
                </div>

                {/* Todo Text */}
                <div className="flex-1 min-w-0">
                  <div
                    className="text-sm"
                    style={{
                      color: '#FFFFFF',
                      opacity: todo.status === 'completed' ? 0.7 : 0.9,
                    }}
                  >
                    {todo.activeForm || todo.content || `Task ${index + 1}`}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default TodoDrawer;
