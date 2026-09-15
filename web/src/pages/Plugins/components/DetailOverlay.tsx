import React from 'react';
import { ModalShell } from '@/components/ui/ModalShell';
import type { BrandArt } from '@/lib/brandArt';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import type { MarkKind } from '@/pages/ChatAgent/components/mcp/KindTile';

/**
 * The shared shell of the detail overlays (server / skill / plugin / order):
 * the house dialog shell at its wide size, with the identity header pinned
 * over a hairline and the body scrolling. The panel follows its content, so
 * a two-line plugin is a short card and a 28-tool server fills the viewport
 * and scrolls inside. Exit animations require an `AnimatePresence` around
 * the call site's conditional render.
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

/** Identity header: large tile, name + kind label, quiet meta line, and the
 *  overlay's primary control (usually the enabled toggle) on the right. */
export function DetailHeader({
  name,
  labelId,
  kind,
  kindLabel,
  art,
  meta,
  controls,
}: {
  name: string;
  labelId: string;
  /** Which glyph stands in when there is no art. */
  kind: MarkKind;
  /** The item's kind, spelled out ("Skill", "MCP server", "Plugin"). */
  kindLabel: string;
  /** The item's own mark; falls back to `kind`'s glyph without it. */
  art?: BrandArt;
  meta?: React.ReactNode;
  controls?: React.ReactNode;
}) {
  return (
    <div className="flex items-start gap-3.5 pr-8">
      <BrandMark name={name} kind={kind} art={art} size="lg" />
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
            {kindLabel}
          </span>
        </div>
        {meta && (
          <div
            className="flex items-center gap-2 flex-wrap text-xs"
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
    <div className="flex items-baseline gap-3 text-[0.8125rem]">
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
          fontSize: '0.75rem',
        }}
      >
        {children}
      </span>
    </div>
  );
}
