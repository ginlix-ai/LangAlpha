import React from 'react';
import { ChevronRight } from 'lucide-react';
import { TaskStatusChip, type TaskCardStatusKind } from './taskStatusUi';

export const MONO_STACK = 'var(--font-mono)';

interface TaskCardShellProps {
  /** Lowercase type token on the left of the header rule. */
  eyebrow: React.ReactNode;
  statusKind: TaskCardStatusKind;
  /** Wire status rendered verbatim when `statusKind` is `unknown`. */
  rawStatus?: string;
  /** Trailing header affordance; defaults to the muted chevron. */
  affordance?: React.ReactNode;
  title: string;
  /** Card tooltip; only rendered when the card is interactive. */
  hint?: string;
  /** Absent on read-only surfaces (shared links) — the card then renders
   *  inert: no button role, focus stop, pointer cursor, hover response, or
   *  default chevron, so nothing promises a navigation that can't happen. */
  onOpen?: () => void;
  testId?: string;
  /** Body content below the title. */
  children?: React.ReactNode;
  /** Content for the hairline band under the body. */
  footer?: React.ReactNode;
}

/**
 * The chrome every inline task card shares: the (optionally clickable)
 * container, the type · status · affordance header rule, the title stack, and
 * the optional footer band. Callers supply only what differs — the eyebrow,
 * the status kind, and the body.
 */
export function TaskCardShell({
  eyebrow,
  statusKind,
  rawStatus,
  affordance,
  title,
  hint,
  onOpen,
  testId,
  children,
  footer,
}: TaskCardShellProps): React.ReactElement {
  const handleKeyDown = (e: React.KeyboardEvent<HTMLDivElement>): void => {
    // Ignore keystrokes that originated on a descendant control (e.g. the
    // "View subagent output" button) — the descendant handles its own
    // activation, and the keydown shouldn't double-fire as a card click.
    if (e.target !== e.currentTarget) return;
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      onOpen?.();
    }
  };

  const interactiveProps: React.HTMLAttributes<HTMLDivElement> = onOpen
    ? {
        role: 'button',
        tabIndex: 0,
        onClick: onOpen,
        onKeyDown: handleKeyDown,
        onMouseEnter: (e) => (e.currentTarget.style.borderColor = 'var(--color-border-default)'),
        onMouseLeave: (e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)'),
        title: hint,
      }
    : {};

  return (
    <div
      {...interactiveProps}
      data-testid={testId}
      style={{
        background: 'var(--color-bg-tool-card)',
        border: '1px solid var(--color-border-muted)',
        borderRadius: 12,
        overflow: 'hidden',
        cursor: onOpen ? 'pointer' : 'default',
        fontFamily: MONO_STACK,
        transition: 'border-color 0.15s',
      }}
    >
      {/* Rule: type · status · affordance */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          padding: '10px 12px 8px 14px',
          borderBottom: '1px solid var(--color-border-subtle)',
          fontSize: '0.75rem',
        }}
      >
        <span
          style={{
            color: 'var(--color-text-secondary)',
            fontWeight: 500,
            textTransform: 'lowercase',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            minWidth: 0,
            flex: '0 1 auto',
          }}
        >
          {eyebrow}
        </span>
        <span style={{ flex: 1 }} />
        <TaskStatusChip kind={statusKind} rawStatus={rawStatus} />
        {affordance ??
          (onOpen ? (
            <ChevronRight
              aria-hidden="true"
              style={{ width: 14, height: 14, flexShrink: 0, color: 'var(--color-text-quaternary)' }}
            />
          ) : null)}
      </div>

      <div style={{ padding: '12px 14px 14px' }}>
        <div
          style={{
            fontFamily: 'var(--font-ui)',
            fontSize: '0.875rem',
            fontWeight: 500,
            color: 'var(--color-text-primary)',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {title}
        </div>
        {children}
      </div>

      {footer && (
        <div
          style={{
            padding: '7px 14px',
            borderTop: '1px solid var(--color-border-subtle)',
            fontSize: '0.6875rem',
            color: 'var(--color-text-tertiary)',
            letterSpacing: '0.02em',
          }}
        >
          {footer}
        </div>
      )}
    </div>
  );
}

export default TaskCardShell;
