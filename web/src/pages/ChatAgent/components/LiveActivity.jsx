import { useState, useEffect, useRef } from 'react';
import { Brain, Loader2, CheckCircle2 } from 'lucide-react';
import { getDisplayName, getToolIcon, getInProgressText } from './toolDisplayConfig';
import Markdown from './Markdown';

const MIN_EXPOSURE_MS = 5000; // minimum total time visible (active + hold)
const FADE_MS = 500;          // animating out

/**
 * LiveActivity Component
 *
 * Shows currently active items with streaming/progress indicators.
 * Completed items hold for HOLD_MS, then fade out over FADE_MS before removal.
 */
function LiveActivity({ activeReasoning, activeToolCalls }) {
  // --- Reasoning hold ---
  // phase: null | 'hold' | 'exit'
  const [reasoningPhase, setReasoningPhase] = useState(null);
  const [displayReasoning, setDisplayReasoning] = useState(null);
  const reasoningTimersRef = useRef({ hold: null, exit: null });
  const prevReasoningRef = useRef(null);
  const reasoningAppearTimeRef = useRef(null);

  useEffect(() => {
    const wasActive = prevReasoningRef.current?.isReasoning;
    const isActive = activeReasoning?.isReasoning;
    const timers = reasoningTimersRef.current;

    if (isActive) {
      // Streaming — show immediately, cancel any pending timers
      clearTimeout(timers.hold);
      clearTimeout(timers.exit);
      timers.hold = null;
      timers.exit = null;
      if (!reasoningAppearTimeRef.current) {
        reasoningAppearTimeRef.current = Date.now();
      }
      setDisplayReasoning(activeReasoning);
      setReasoningPhase(null); // active, no phase
    } else if (wasActive && !isActive) {
      // Just finished — calculate remaining hold to meet minimum exposure
      const alreadyVisible = Date.now() - (reasoningAppearTimeRef.current || Date.now());
      const holdMs = Math.max(0, MIN_EXPOSURE_MS - alreadyVisible);
      setReasoningPhase('hold');
      timers.hold = setTimeout(() => {
        setReasoningPhase('exit');
        timers.exit = setTimeout(() => {
          setDisplayReasoning(null);
          setReasoningPhase(null);
          reasoningAppearTimeRef.current = null;
          timers.hold = null;
          timers.exit = null;
        }, FADE_MS);
      }, holdMs);
    } else if (!isActive && !timers.hold && !timers.exit) {
      setDisplayReasoning(null);
      setReasoningPhase(null);
      reasoningAppearTimeRef.current = null;
    }

    prevReasoningRef.current = activeReasoning;
    return () => {
      clearTimeout(timers.hold);
      clearTimeout(timers.exit);
    };
  }, [activeReasoning]);

  // --- Tool call hold ---
  // Each fading tool call has _phase: 'hold' | 'exit'
  const [fadingToolCalls, setFadingToolCalls] = useState([]);
  const toolTimersRef = useRef({});
  const prevToolCallsRef = useRef([]);
  const toolAppearTimeRef = useRef({}); // id → Date.now() when first seen

  // Track when each tool call first appears
  useEffect(() => {
    const current = activeToolCalls || [];
    for (const tc of current) {
      const id = tc.id || tc.toolCallId;
      if (!toolAppearTimeRef.current[id]) {
        toolAppearTimeRef.current[id] = Date.now();
      }
    }
  }, [activeToolCalls]);

  useEffect(() => {
    const current = activeToolCalls || [];
    const currentIds = new Set(current.map((tc) => tc.id || tc.toolCallId));
    const prev = prevToolCallsRef.current;

    for (const tc of prev) {
      const id = tc.id || tc.toolCallId;
      if (!currentIds.has(id) && !toolTimersRef.current[id]) {
        // Calculate remaining hold time to meet minimum exposure
        const appearedAt = toolAppearTimeRef.current[id] || Date.now();
        const alreadyVisible = Date.now() - appearedAt;
        const holdMs = Math.max(0, MIN_EXPOSURE_MS - alreadyVisible);

        // Enter hold phase
        setFadingToolCalls((f) => {
          if (f.some((x) => x._fadeId === id)) return f;
          return [...f, { ...tc, _fadeId: id, _phase: 'hold' }];
        });

        // After hold → enter exit phase
        const holdTimer = setTimeout(() => {
          setFadingToolCalls((f) =>
            f.map((x) => (x._fadeId === id ? { ...x, _phase: 'exit' } : x))
          );
          // After exit animation → remove from DOM
          const exitTimer = setTimeout(() => {
            setFadingToolCalls((f) => f.filter((x) => x._fadeId !== id));
            delete toolTimersRef.current[id];
            delete toolAppearTimeRef.current[id];
          }, FADE_MS);
          toolTimersRef.current[id] = exitTimer;
        }, holdMs);

        toolTimersRef.current[id] = holdTimer;
      }
    }

    prevToolCallsRef.current = current;
    return () => {
      for (const t of Object.values(toolTimersRef.current)) clearTimeout(t);
    };
  }, [activeToolCalls]);

  const showReasoning = !!displayReasoning;
  const isReasoningStreaming = displayReasoning?.isReasoning;
  const hasActiveTools = activeToolCalls && activeToolCalls.length > 0;
  const hasFadingTools = fadingToolCalls.length > 0;

  if (!showReasoning && !hasActiveTools && !hasFadingTools) return null;

  // Reasoning opacity: streaming=1, hold=0.6, exit=0
  const reasoningOpacity = isReasoningStreaming ? 1 : reasoningPhase === 'exit' ? 0 : 0.6;

  return (
    <div className="mt-2 space-y-2">
      {/* Active reasoning */}
      {showReasoning && (
        <div
          className="px-3 overflow-hidden"
          style={{
            opacity: reasoningOpacity,
            maxHeight: reasoningPhase === 'exit' ? 0 : '500px',
            paddingTop: reasoningPhase === 'exit' ? 0 : '8px',
            paddingBottom: reasoningPhase === 'exit' ? 0 : '8px',
            marginTop: reasoningPhase === 'exit' ? 0 : undefined,
            transition: `opacity ${FADE_MS}ms ease, max-height ${FADE_MS}ms ease, padding ${FADE_MS}ms ease, margin ${FADE_MS}ms ease`,
          }}
        >
          <div
            className="flex items-center gap-2 mb-1"
            style={{ fontSize: '13px', color: 'var(--Labels-Secondary)' }}
          >
            <div className="relative flex-shrink-0">
              <Brain className="h-4 w-4" />
              {isReasoningStreaming && (
                <Loader2
                  className="h-3 w-3 absolute -top-0.5 -right-0.5 animate-spin"
                  style={{ color: 'var(--Labels-Secondary)' }}
                />
              )}
            </div>
            <span className="font-medium truncate">
              {isReasoningStreaming
                ? (displayReasoning.title
                    ? `Reasoning: ${displayReasoning.title}`
                    : 'Reasoning...')
                : 'Reasoning complete'}
            </span>
          </div>

          {displayReasoning.content && (
            <Markdown
              variant="compact"
              content={displayReasoning.content}
              className="text-xs"
              style={{ opacity: 0.8 }}
            />
          )}
        </div>
      )}

      {/* In-progress tool calls */}
      {hasActiveTools &&
        activeToolCalls.map((tc) => (
          <ToolCallLiveRow key={tc.id || tc.toolCallId} tc={tc} phase="active" />
        ))}

      {/* Fading (just-completed) tool calls */}
      {hasFadingTools &&
        fadingToolCalls.map((tc) => (
          <ToolCallLiveRow key={`fade-${tc._fadeId}`} tc={tc} phase={tc._phase} />
        ))}
    </div>
  );
}

/** phase: 'active' | 'hold' | 'exit' */
function ToolCallLiveRow({ tc, phase }) {
  const toolName = tc.toolName || '';
  const displayName = getDisplayName(toolName);
  const IconComponent = getToolIcon(toolName);
  // Truly in-progress: phase is 'active' AND the tool call hasn't completed yet
  const isInProgress = phase === 'active' && !tc.isComplete && !tc._recentlyCompleted;
  const isExit = phase === 'exit';
  const progressText = isInProgress ? getInProgressText(toolName, tc.toolCall) : null;

  return (
    <div
      className="flex items-center gap-2 px-3 rounded-md overflow-hidden"
      style={{
        backgroundColor: isInProgress ? 'rgba(97, 85, 245, 0.1)' : 'rgba(255, 255, 255, 0.04)',
        border: '1px solid rgba(255, 255, 255, 0.08)',
        fontSize: '13px',
        color: 'var(--Labels-Secondary)',
        opacity: isExit ? 0 : isInProgress ? 1 : 0.6,
        maxHeight: isExit ? 0 : '60px',
        paddingTop: isExit ? 0 : '6px',
        paddingBottom: isExit ? 0 : '6px',
        borderWidth: isExit ? 0 : '1px',
        marginTop: isExit ? 0 : undefined,
        transition: `opacity ${FADE_MS}ms ease, max-height ${FADE_MS}ms ease, padding ${FADE_MS}ms ease, border-width ${FADE_MS}ms ease, margin ${FADE_MS}ms ease, background-color 0.3s ease`,
      }}
    >
      <div className="relative flex-shrink-0">
        <IconComponent className="h-4 w-4" />
        {isInProgress ? (
          <Loader2
            className="h-3 w-3 absolute -top-0.5 -right-0.5 animate-spin"
            style={{ color: 'var(--Labels-Secondary)' }}
          />
        ) : (
          <CheckCircle2
            className="h-3 w-3 absolute -top-0.5 -right-0.5"
            style={{ color: 'rgba(34, 197, 94, 0.7)' }}
          />
        )}
      </div>
      <span className="font-medium">{displayName}</span>
      {isInProgress ? (
        <span style={{ opacity: 0.55 }}>{progressText}</span>
      ) : (
        <span style={{ opacity: 0.55 }}>done</span>
      )}
    </div>
  );
}

export default LiveActivity;
