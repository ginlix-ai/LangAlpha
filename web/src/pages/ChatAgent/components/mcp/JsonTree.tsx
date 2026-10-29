import React, { useState } from 'react';
import { ChevronRight } from 'lucide-react';

/**
 * A collapsible view of a JSON value. Containers open by default down to
 * `openDepth` so the shape is readable at a glance while a large payload
 * (a list of positions, a nested quote) stays foldable.
 */
export function JsonTree({ value, openDepth = 2 }: { value: unknown; openDepth?: number }): React.ReactElement {
  return (
    <div className="font-mono text-xs leading-5 wrap-break-word" style={{ color: 'var(--color-text-secondary)' }}>
      <JsonNode value={value} depth={0} openDepth={openDepth} />
    </div>
  );
}

function isContainer(v: unknown): v is Record<string, unknown> | unknown[] {
  return !!v && typeof v === 'object';
}

function Scalar({ value }: { value: unknown }): React.ReactElement {
  if (value === null) return <span style={{ color: 'var(--color-text-quaternary)' }}>null</span>;
  if (typeof value === 'string') return <span style={{ color: 'var(--color-text-primary)' }}>&quot;{value}&quot;</span>;
  if (typeof value === 'number') return <span style={{ color: 'var(--color-accent-primary)' }}>{String(value)}</span>;
  if (typeof value === 'boolean') return <span style={{ color: 'var(--color-accent-primary)' }}>{String(value)}</span>;
  return <span>{String(value)}</span>;
}

function JsonNode({ value, depth, openDepth, label }: { value: unknown; depth: number; openDepth: number; label?: string }): React.ReactElement {
  const [open, setOpen] = useState(depth < openDepth);
  const keyEl = label !== undefined ? (
    <span className="mr-1" style={{ color: 'var(--color-text-tertiary)' }}>{label}:</span>
  ) : null;

  if (!isContainer(value)) {
    return (
      <div className="pl-4">
        {keyEl}
        <Scalar value={value} />
      </div>
    );
  }

  const entries: Array<[string, unknown]> = Array.isArray(value)
    ? value.map((v, i) => [String(i), v])
    : Object.entries(value);
  const [openBrace, closeBrace] = Array.isArray(value) ? ['[', ']'] : ['{', '}'];
  const count = entries.length;

  if (count === 0) {
    return (
      <div className="pl-4">
        {keyEl}
        <span>{openBrace}{closeBrace}</span>
      </div>
    );
  }

  return (
    <div className={depth === 0 ? '' : 'pl-4'}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="inline-flex items-center gap-0.5 -ml-4 hover:opacity-80 text-left"
        aria-expanded={open}
      >
        <ChevronRight
          className="h-3 w-3 shrink-0 transition-transform duration-fold"
          style={{ color: 'var(--color-text-tertiary)', transform: open ? 'rotate(90deg)' : 'rotate(0deg)' }}
        />
        <span>
          {keyEl}
          <span>{openBrace}</span>
          {!open && (
            <span style={{ color: 'var(--color-text-quaternary)' }}> … {count} </span>
          )}
          {!open && <span>{closeBrace}</span>}
        </span>
      </button>
      {open && (
        <>
          {entries.map(([k, v]) => (
            <JsonNode key={k} value={v} depth={depth + 1} openDepth={openDepth} label={k} />
          ))}
          <div>{closeBrace}</div>
        </>
      )}
    </div>
  );
}
