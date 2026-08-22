import React from 'react';
import { useTranslation } from 'react-i18next';
import { motion, useReducedMotion } from 'framer-motion';
import { X } from 'lucide-react';
import { useDialogA11y } from '@/hooks/useDialogA11y';
import { IdentityTile } from '@/pages/ChatAgent/components/mcp/IdentityTile';

/**
 * The shared shell of the three detail overlays (server / skill / plugin):
 * house fixed-overlay dialog, identity header pinned over a hairline, body
 * scrolls. The panel is one canonical size for every kind — content never
 * dictates it, so switching between a server, a skill and a plugin presents
 * the same page, not three different popovers. Exit animations require an
 * `AnimatePresence` around the call site's conditional render.
 */

// House entrance curve (DESIGN.md § Motion).
const EASE_OUT = [0.16, 1, 0.3, 1] as const;

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
  const { t } = useTranslation();
  const dialogRef = useDialogA11y<HTMLDivElement>(onClose);
  const reducedMotion = useReducedMotion();
  return (
    <motion.div
      className="fixed inset-0 z-[60] flex items-center justify-center p-4"
      style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0, transition: { duration: 0.12 } }}
      transition={{ duration: 0.15 }}
      onClick={onClose}
    >
      <motion.div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={labelId}
        tabIndex={-1}
        className="relative w-full max-w-2xl h-[min(85vh,44rem)] rounded-lg flex flex-col"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          boxShadow: 'var(--shadow-card)',
        }}
        initial={
          reducedMotion ? { opacity: 0 } : { opacity: 0, y: 16, scale: 0.98 }
        }
        animate={{ opacity: 1, y: 0, scale: 1 }}
        exit={
          reducedMotion
            ? { opacity: 0, transition: { duration: 0.12 } }
            : { opacity: 0, y: 8, scale: 0.98, transition: { duration: 0.14 } }
        }
        transition={{ duration: 0.32, ease: EASE_OUT }}
        onClick={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          onClick={onClose}
          aria-label={t('common.close')}
          className="absolute right-4 top-4 p-1.5 rounded transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <X className="h-4 w-4" />
        </button>
        <div
          className="flex-shrink-0 px-6 pt-5 pb-4"
          style={{ borderBottom: '1px solid var(--color-border-muted)' }}
        >
          {header}
        </div>
        <div className="min-h-0 flex-1 overflow-y-auto px-6 py-5 flex flex-col gap-5">
          {children}
        </div>
        {footer && (
          <div
            className="flex-shrink-0 px-6 py-4"
            style={{ borderTop: '1px solid var(--color-border-muted)' }}
          >
            {footer}
          </div>
        )}
      </motion.div>
    </motion.div>
  );
}

/** Identity header: large tile, name + kind label, quiet meta line, and the
 *  overlay's primary control (usually the enabled toggle) on the right. */
export function DetailHeader({
  name,
  labelId,
  kind,
  meta,
  controls,
}: {
  name: string;
  labelId: string;
  /** The item's kind, spelled out ("Skill", "MCP server", "Plugin"). */
  kind: string;
  meta?: React.ReactNode;
  controls?: React.ReactNode;
}) {
  return (
    <div className="flex items-start gap-3.5 pr-8">
      <IdentityTile name={name} size="lg" />
      <div className="min-w-0 flex-1 flex flex-col gap-1">
        <div className="flex items-baseline gap-2 flex-wrap">
          <h2
            id={labelId}
            className="text-lg font-semibold leading-tight truncate"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {name}
          </h2>
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {kind}
          </span>
        </div>
        {meta && (
          <div
            className="flex items-center gap-2 flex-wrap text-[0.6875rem]"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {meta}
          </div>
        )}
      </div>
      {controls && <div className="flex items-center gap-2 flex-shrink-0">{controls}</div>}
    </div>
  );
}

/** Section head inside an overlay body: small caps title + count, hairline. */
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
    <div className="flex items-baseline gap-3 text-xs">
      <span
        className="w-24 flex-shrink-0"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {label}
      </span>
      <span
        className="min-w-0 break-all"
        style={{
          color: 'var(--color-text-secondary)',
          fontFamily: "'JetBrains Mono', 'Menlo', monospace",
          fontSize: '0.6875rem',
        }}
      >
        {children}
      </span>
    </div>
  );
}
