import React from 'react';

interface EditDiffProps {
  oldStr: string;
  newStr: string;
}

/** The two-tone old/new block under an Edit row. Lines, not a real diff: the
 *  tool sends exactly the text it replaced and the text it wrote. */
export function EditDiff({ oldStr, newStr }: EditDiffProps): React.ReactElement {
  return (
    <div className="rounded overflow-hidden" style={{ fontSize: '0.75rem', border: '1px solid var(--color-border-muted)' }}>
      {oldStr && (
        <div style={{ backgroundColor: 'var(--color-loss-soft)' }}>
          {oldStr.split('\n').map((line, i) => (
            <div key={`old-${i}`} className="flex" style={{ minHeight: '20px' }}>
              <span
                className="shrink-0 select-none text-right px-2"
                style={{ color: 'var(--color-loss-muted)', width: '20px', userSelect: 'none' }}
              >&minus;</span>
              <pre className="flex-1 font-mono whitespace-pre-wrap break-all m-0 pr-2" style={{ color: 'var(--color-loss)' }}>
                {line}
              </pre>
            </div>
          ))}
        </div>
      )}
      {newStr && (
        <div style={{ backgroundColor: 'var(--color-profit-soft)' }}>
          {newStr.split('\n').map((line, i) => (
            <div key={`new-${i}`} className="flex" style={{ minHeight: '20px' }}>
              <span
                className="shrink-0 select-none text-right px-2"
                style={{ color: 'var(--color-profit-muted)', width: '20px', userSelect: 'none' }}
              >+</span>
              <pre className="flex-1 font-mono whitespace-pre-wrap break-all m-0 pr-2" style={{ color: 'var(--color-profit)' }}>
                {line}
              </pre>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
