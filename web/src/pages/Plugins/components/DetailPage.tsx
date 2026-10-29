import React, { createContext, useContext, useEffect } from 'react';
import { AlertTriangle, ChevronRight } from 'lucide-react';
import type { BrandArt } from '@/lib/brandArt';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import type { MarkKind } from '@/pages/ChatAgent/components/mcp/KindTile';
import './surface/surface.css';

/**
 * A detail on the Plugins page (server / skill / plugin / catalog entry) is a
 * page in the content column: breadcrumb, a large header over a hairline,
 * then a main column beside a narrow side column for the item's settings and
 * verbs. The page provides the breadcrumb's start (`DetailPageProvider`);
 * a detail opened from another one adds its step (`DetailTrailStep`).
 */

interface DetailPageFrame {
  /** The section the page sits under, for the breadcrumb ("Connectors"). */
  section: string;
  /** A trail between the section and the item (the plugin a connector is
   *  part of), each a step back. */
  trail: ReadonlyArray<{ label: string; onClick: () => void }>;
}

const DetailPageContext = createContext<DetailPageFrame | null>(null);

function useDetailFrame(): DetailPageFrame {
  const frame = useContext(DetailPageContext);
  if (!frame) throw new Error('A detail page renders under a DetailPageProvider.');
  return frame;
}

export function DetailPageProvider({
  value,
  children,
}: {
  value: { section: string };
  children: React.ReactNode;
}) {
  return (
    <DetailPageContext.Provider value={{ section: value.section, trail: [] }}>
      {children}
    </DetailPageContext.Provider>
  );
}

/** A step back in the breadcrumb of every detail opened under it: the
 *  catalog a plugin's page was opened from. */
export function DetailTrailStep({
  label,
  onClick,
  children,
}: {
  label: string;
  onClick: () => void;
  children: React.ReactNode;
}) {
  const frame = useDetailFrame();
  return (
    <DetailPageContext.Provider value={{ ...frame, trail: [...frame.trail, { label, onClick }] }}>
      {children}
    </DetailPageContext.Provider>
  );
}

export function DetailPage({
  labelId,
  onClose,
  header,
  footer,
  aside,
  crumb,
  lead,
  children,
}: {
  labelId: string;
  onClose: () => void;
  header: React.ReactNode;
  /** Full width between the header and the two columns, for a first step
   *  the columns below depend on (a new subagent's starting point). */
  lead?: React.ReactNode;
  /** Lifecycle verbs and confirms: the last block of the side column. */
  footer?: React.ReactNode;
  /** Settings beside the content: availability, the key, facts. */
  aside?: React.ReactNode;
  /** The item's name in the breadcrumb. */
  crumb?: string;
  children: React.ReactNode;
}) {
  const page = useDetailFrame();
  // A page opened from deep in a long list starts at its own top.
  useEffect(() => {
    document.getElementById(labelId)?.closest('.plugins-scroll')?.scrollTo({ top: 0 });
  }, [labelId]);

  const side = aside || footer;
  return (
    <article className="detail-page" aria-labelledby={labelId}>
      <nav className="detail-crumbs" aria-label="Breadcrumb">
        <button type="button" onClick={onClose}>{page.section}</button>
        {page.trail.map((step) => (
          <React.Fragment key={step.label}>
            <ChevronRight className="h-3.5 w-3.5" aria-hidden />
            <button type="button" onClick={step.onClick}>{step.label}</button>
          </React.Fragment>
        ))}
        {crumb && (
          <>
            <ChevronRight className="h-3.5 w-3.5" aria-hidden />
            <span aria-current="page">{crumb}</span>
          </>
        )}
      </nav>
      <header className="detail-page-head">{header}</header>
      {lead && <div className="detail-page-lead">{lead}</div>}
      <div className={side ? 'detail-page-body' : 'detail-page-body is-single'}>
        <div className="detail-page-main">{children}</div>
        {side && (
          <aside className="detail-page-aside">
            {aside}
            {footer && <div className="detail-page-verbs">{footer}</div>}
          </aside>
        )}
      </div>
    </article>
  );
}

/** Identity header: tile, name, quiet meta line, a paragraph about the item,
 *  and its primary control (usually the enabled toggle) on the right. A skill
 *  has no mark, so without `kind` the header opens on its name.
 *
 *  The controls share a row with the name and meta line only, so when the two
 *  do not fit side by side the controls take the next line instead of
 *  narrowing the name and the description to a column of words. */
export function DetailHeader({
  name,
  labelId,
  kind,
  art,
  meta,
  description,
  controls,
}: {
  name: string;
  labelId: string;
  /** Which glyph stands in when there is no art; absent, no tile. */
  kind?: MarkKind;
  /** The item's own mark; falls back to `kind`'s glyph without it. */
  art?: BrandArt;
  meta?: React.ReactNode;
  /** One paragraph about the item, under the meta line. */
  description?: React.ReactNode;
  controls?: React.ReactNode;
}) {
  return (
    <div className="detail-head">
      {kind && <BrandMark name={name} kind={kind} art={art} size="xl" />}
      <div className="detail-head-text">
        <div className="detail-head-top">
          <div className="detail-head-title">
            <h1 id={labelId}>{name}</h1>
            {meta && <div className="detail-head-meta">{meta}</div>}
          </div>
          {controls && <div className="detail-head-controls">{controls}</div>}
        </div>
        {description && <div className="detail-head-desc">{description}</div>}
      </div>
    </div>
  );
}

/** Section head in a detail's main column: title + count over a hairline,
 *  with an optional action on the right. */
export function DetailSection({
  title,
  count,
  action,
  children,
}: {
  title: string;
  count?: number;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="detail-section">
      <div className="surface-section-head">
        <h2>{title}</h2>
        {typeof count === 'number' && <span className="surface-count">{count}</span>}
        {action && <div className="surface-section-side">{action}</div>}
      </div>
      {children}
    </section>
  );
}

/** A titled block in the side column ("Availability", "Details"): a small
 *  heading over its content, no hairline. `side` sits at the heading's right
 *  edge, for what the block was shipped with or a menu over its rows. */
export function DetailAsideBlock({
  title,
  side,
  children,
}: {
  title: string;
  side?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="detail-aside-block">
      <h3 className={side ? 'has-side' : undefined}>
        {side ? <span>{title}</span> : title}
        {side && <span className="detail-aside-side">{side}</span>}
      </h3>
      {children}
    </section>
  );
}

/** The problem at the top of a detail and its fix: "Key rejected · Replace key". */
export function DetailCallout({
  title,
  children,
  action,
  icon,
  tone = 'warn',
  align = 'center',
}: {
  title: React.ReactNode;
  children?: React.ReactNode;
  action?: React.ReactNode;
  /** Replaces the warning triangle (a sign-in is a step, not a fault). */
  icon?: React.ReactNode;
  /** `quiet` draws the icon in the tertiary text colour instead of amber. */
  tone?: 'warn' | 'quiet';
  /** `start` pins the icon to the title, for a body tall enough (a field to
   *  fill in) that a centred icon would sit beside the middle of it. */
  align?: 'center' | 'start';
}) {
  return (
    <div
      className="detail-callout"
      role="status"
      style={align === 'start' ? { alignItems: 'flex-start' } : undefined}
    >
      <span className={tone === 'quiet' ? 'detail-callout-icon is-quiet' : 'detail-callout-icon'}>
        {icon ?? <AlertTriangle className="h-4 w-4" aria-hidden />}
      </span>
      <div className="detail-callout-text">
        <div className="detail-callout-title">{title}</div>
        {children && <div className="detail-callout-body">{children}</div>}
      </div>
      {action}
    </div>
  );
}

/** Label/value facts for the side column (version, author, source). Plain
 *  text values; a link is passed as a node. Empty values are skipped. */
export function DetailFacts({
  items,
}: {
  items: ReadonlyArray<readonly [label: string, value: React.ReactNode]>;
}) {
  const shown = items.filter(([, value]) => value !== null && value !== undefined && value !== '');
  if (shown.length === 0) return null;
  return (
    <dl className="detail-facts">
      {shown.map(([label, value]) => (
        <React.Fragment key={label}>
          <dt>{label}</dt>
          <dd title={typeof value === 'string' ? value : undefined}>{value}</dd>
        </React.Fragment>
      ))}
    </dl>
  );
}
