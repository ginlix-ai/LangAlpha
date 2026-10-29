import React from 'react';
import { ModalShell } from '@/components/ui/ModalShell';

/**
 * A detail as a dialog (an order): the house dialog shell at its wide size,
 * with the header pinned over a hairline and the body scrolling. The panel
 * follows its content, so a short detail is a short card and a long one
 * fills the viewport and scrolls inside. Exit animations need an
 * `AnimatePresence` around the call site's conditional render. The Plugins
 * page shows its details as pages instead (`DetailPage`).
 */
export function DetailOverlay({
  labelId,
  onClose,
  header,
  footer,
  children,
}: {
  labelId: string;
  onClose: () => void;
  header: React.ReactNode;
  /** Pinned action bar under the scroll body (lifecycle buttons, confirms). */
  footer?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <ModalShell
      labelId={labelId}
      onClose={onClose}
      width="wide"
      header={header}
      footer={footer || undefined}
      density="reading"
    >
      {children}
    </ModalShell>
  );
}

/** Section head inside a dialog body: small caps title + count, hairline. */
export function DetailSection({
  title,
  count,
  children,
}: {
  title: string;
  count?: number;
  children: React.ReactNode;
}) {
  return (
    <section className="flex flex-col gap-2.5">
      <div
        className="flex items-baseline gap-2 pb-1.5"
        style={{ borderBottom: '1px solid var(--color-border-muted)' }}
      >
        <h3
          className="text-[0.6875rem] font-medium uppercase tracking-wide"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {title}
        </h3>
        {typeof count === 'number' && (
          <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-quaternary)' }}>
            {count}
          </span>
        )}
      </div>
      {children}
    </section>
  );
}

/** One label/value line in a config section. Values render in mono — they
 *  are commands, URLs and var names, not prose. */
export function DetailField({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex items-baseline gap-3 text-[0.8125rem]">
      <span
        className="w-24 shrink-0"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {label}
      </span>
      <span
        className="min-w-0 break-all"
        style={{
          color: 'var(--color-text-secondary)',
          fontFamily: "'JetBrains Mono', 'Menlo', monospace",
          fontSize: '0.75rem',
        }}
      >
        {children}
      </span>
    </div>
  );
}
