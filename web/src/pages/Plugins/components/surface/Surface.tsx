import {
  Children,
  createContext,
  useContext,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { Link } from 'react-router';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, Check, ChevronDown, ChevronRight, MoreHorizontal } from 'lucide-react';
import { cn } from '@/lib/utils';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import './surface.css';

/**
 * The Plugins page's list vocabulary: sections of compact cells in a grid that
 * takes as many ~340px columns as the width allows, each cell carrying its
 * whole state in one trailing slot. A toggle never sits on a cell; on/off,
 * workspaces and removal live in the cell's menu, and the detail page carries
 * the full controls.
 */

/** A titled group of cells: "Needs attention 2", "wind 6  Open plugin ›". */
export function SurfaceSection({
  title,
  count,
  note,
  attention = false,
  action,
  children,
  className,
}: {
  title: ReactNode;
  count?: number;
  /** Quiet words after the count ("Built in", "From the catalog"). */
  note?: ReactNode;
  /** Hang a warning dot in the gutter beside the title. */
  attention?: boolean;
  /** Right side of the header: a filter, an "Open plugin ›" link. */
  action?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={cn('surface-section', className)}>
      <div className="surface-section-head">
        {attention && <span className="surface-dot surface-dot-attn surface-section-dot" aria-hidden />}
        <h2>{title}</h2>
        {typeof count === 'number' && <span className="surface-count">{count}</span>}
        {note && <span className="surface-note">{note}</span>}
        {action && <div className="surface-section-side">{action}</div>}
      </div>
      {children}
    </section>
  );
}

/** The header link of a group that belongs to something with its own page. */
export function SectionLink({ children, onClick }: { children: ReactNode; onClick: () => void }) {
  return (
    <button type="button" className="surface-section-link" onClick={onClick}>
      {children}
      <ChevronRight className="h-3.5 w-3.5" aria-hidden />
    </button>
  );
}

/**
 * The cell grid, folded to `rows` rows when it holds more than fit. The fold
 * counts the columns the grid actually laid out, so the same list shows two
 * rows of two at 1440 and two rows of three at 1920, and the "See …" line names
 * the first cells it hides rather than a bare number.
 */
export function SurfaceGrid({
  children,
  rows,
  hiddenNames,
  expanded: expandedProp,
  onExpandedChange,
  reveal = false,
  className,
}: {
  children: ReactNode;
  /** Fold after this many rows; absent, show everything. */
  rows?: number;
  /** Names of the cells in order, for the "See X, Y and N more" line. */
  hiddenNames?: string[];
  /** Controlled fold, for a "Show all" that lives outside the grid. */
  expanded?: boolean;
  onExpandedChange?: (expanded: boolean) => void;
  /**
   * Unfold for good the first time this is true, as if "See …" had been
   * pressed (uncontrolled grids only). For a group holding what was just
   * added: the mark asking for it fades after a few seconds, and folding
   * then would pull the new cell out from under the pointer.
   */
  reveal?: boolean;
  className?: string;
}) {
  const { t } = useTranslation();
  const ref = useRef<HTMLDivElement>(null);
  const [columns, setColumns] = useState(2);
  const [expandedState, setExpandedState] = useState(reveal);
  // Adjusted in render rather than in an effect, so the folded grid is never
  // painted between the mark arriving and the unfold.
  if (reveal && !expandedState) setExpandedState(true);
  const expanded = expandedProp ?? expandedState;
  const setExpanded = (next: boolean) => {
    setExpandedState(next);
    onExpandedChange?.(next);
  };
  const items = Children.toArray(children);

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el || !rows) return;
    const read = () => {
      const tracks = getComputedStyle(el).gridTemplateColumns.split(' ').filter(Boolean).length;
      setColumns(Math.max(1, tracks));
    };
    read();
    const ro = new ResizeObserver(read);
    ro.observe(el);
    return () => ro.disconnect();
  }, [rows]);

  const limit = rows ? rows * columns : items.length;
  const folded = !expanded && items.length > limit;
  const shown = folded ? items.slice(0, limit) : items;
  const hidden = folded ? items.length - limit : 0;
  const names = hiddenNames?.slice(limit) ?? [];

  return (
    <>
      <div ref={ref} className={cn('surface-grid', className)}>
        {shown}
      </div>
      {folded && (
        <button type="button" className="surface-more" onClick={() => setExpanded(true)}>
          <ChevronDown className="h-3.5 w-3.5" aria-hidden />
          {names.length >= 3
            ? t('plugins.surface.seeMoreNamed', { first: names[0], second: names[1], count: hidden - 2 })
            : names.length === 2
              ? t('plugins.surface.seeTwo', { first: names[0], second: names[1] })
              : names.length === 1
                ? t('plugins.surface.seeOne', { first: names[0] })
                : t('plugins.surface.seeMore', { count: hidden })}
        </button>
      )}
    </>
  );
}

/**
 * What the list around a cell says about it rather than the cell itself:
 * that select mode is on and whether it is chosen, and that it was added a
 * moment ago. Carried by context so every kind of cell (a connector's, a
 * plugin's, a skill's) takes part without each threading the props through.
 */
export interface CellMode {
  select?: { checked: boolean; onToggle: () => void };
  justAdded?: boolean;
}

const CellModeContext = createContext<CellMode>({});

export function CellModeProvider({ value, children }: { value: CellMode; children: ReactNode }) {
  return <CellModeContext.Provider value={value}>{children}</CellModeContext.Provider>;
}

/**
 * One cell: mark, name with quiet meta, one line, and the trailing state. The
 * whole cell opens the item; the name is the button and stretches over the
 * cell, so the trailing controls stay separately clickable above it.
 *
 * In select mode the same stretched button is the cell's checkbox, so the
 * whole cell is the target (44px and more on a phone); the menu goes, and the
 * trailing state stays to read but takes no clicks.
 */
export function SurfaceCell({
  mark,
  name,
  meta,
  line,
  lineTone = 'default',
  off = false,
  trailing,
  menu,
  onOpen,
  label,
}: {
  /** The logo node, or nothing: a skill has no mark and starts on its name. */
  mark?: ReactNode;
  name: ReactNode;
  /** Beside the name: "7 connectors", a `/command` chip, "2 of 5 workspaces". */
  meta?: ReactNode;
  /** The one line under the name; a problem replaces the description here. */
  line?: ReactNode;
  lineTone?: 'default' | 'warn';
  /** Switched off: the mark dims and the text quiets. */
  off?: boolean;
  trailing?: ReactNode;
  /** A `CellMenu`; it shows on hover, on focus, and while open. */
  menu?: ReactNode;
  onOpen?: () => void;
  /** Accessible name for the open button when `name` is not plain text. */
  label?: string;
}) {
  const { select, justAdded = false } = useContext(CellModeContext);
  const ref = useRef<HTMLDivElement>(null);

  // "It went here": the list a dialog closed onto may be long, and the new
  // cell anywhere in it.
  useEffect(() => {
    if (!justAdded) return;
    const still = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
    ref.current?.scrollIntoView?.({ block: 'nearest', behavior: still ? 'auto' : 'smooth' });
  }, [justAdded]);

  const textMeta = typeof meta === 'string' || typeof meta === 'number';
  return (
    <div
      ref={ref}
      className={cn(
        'surface-cell',
        off && 'is-off',
        select?.checked && 'is-selected',
        select && 'is-selecting',
        justAdded && 'is-just-added',
      )}
    >
      {select && (
        <span className="surface-cell-check" aria-hidden>
          <Check className="h-3 w-3" strokeWidth={3} />
        </span>
      )}
      {mark && <div className="surface-cell-mark">{mark}</div>}
      <div className="surface-cell-text">
        <div className={cn('surface-cell-l1', textMeta && 'has-text-meta')}>
          {select ? (
            <button
              type="button"
              role="checkbox"
              aria-checked={select.checked}
              className="surface-cell-open"
              onClick={select.onToggle}
              aria-label={label}
            >
              <span className="surface-cell-name">{name}</span>
            </button>
          ) : onOpen ? (
            <button type="button" className="surface-cell-open" onClick={onOpen} aria-label={label}>
              <span className="surface-cell-name">{name}</span>
            </button>
          ) : (
            <span className="surface-cell-name">{name}</span>
          )}
          {meta && (
            <span className="surface-cell-meta">
              {/* Plain text in a flex box is an anonymous item no rule can
                  reach, so it is cut off without an ellipsis; a span takes one. */}
              {textMeta ? <span>{meta}</span> : meta}
            </span>
          )}
        </div>
        {line && (
          <div className={cn('surface-cell-l2', lineTone === 'warn' && 'is-warn')}>{line}</div>
        )}
      </div>
      {select
        ? trailing && (
            <div className="surface-cell-end" inert>
              {trailing}
            </div>
          )
        : (menu || trailing) && (
            <div className="surface-cell-end">
              {menu && <div className="surface-cell-menu">{menu}</div>}
              {trailing}
            </div>
          )}
    </div>
  );
}

/** The trailing state: the only state signal a cell carries. */
export function CellState({
  state,
  label,
}: {
  state: 'on' | 'off' | 'warn';
  /** Accessible text; "Off" is also drawn. */
  label?: string;
}) {
  const { t } = useTranslation();
  if (state === 'off') {
    return <span className="surface-state-off">{label ?? t('plugins.surface.off')}</span>;
  }
  if (state === 'warn') {
    return (
      <span className="surface-state-warn" title={label} aria-label={label} role="img">
        <AlertTriangle className="h-4 w-4" />
      </span>
    );
  }
  return (
    <span className="surface-state-on" title={label} aria-label={label ?? t('plugins.surface.on')} role="img">
      <Check className="h-4 w-4" />
    </span>
  );
}

/** An outlined trailing action: Connect, Sign in, Add key. With `to` it is a
 *  link, for a fix that lives on another page. */
export function CellAction({
  children,
  onClick,
  to,
  disabled,
  icon,
  describedBy,
  testId,
}: {
  children: ReactNode;
  onClick?: () => void;
  to?: string;
  disabled?: boolean;
  icon?: ReactNode;
  /** The id of the note saying why it is held, when it is. */
  describedBy?: string;
  testId?: string;
}) {
  if (to) {
    return (
      <Link to={to} className="surface-cell-action" data-testid={testId}>
        {icon}
        {children}
      </Link>
    );
  }
  return (
    <button
      type="button"
      className="surface-cell-action"
      onClick={onClick}
      disabled={disabled}
      aria-describedby={describedBy}
      data-testid={testId}
    >
      {icon}
      {children}
    </button>
  );
}

/** The round "+" of a catalog card. `reason` says why it is disabled. */
export function CellAdd({
  label,
  onClick,
  disabled,
  reason,
}: {
  label: string;
  onClick: () => void;
  disabled?: boolean;
  reason?: string;
}) {
  return (
    <button
      type="button"
      className="surface-cell-add"
      onClick={onClick}
      aria-label={label}
      title={disabled && reason ? reason : label}
      disabled={disabled}
    >
      <span aria-hidden>+</span>
    </button>
  );
}

/** The ⋯ menu of a cell; children are `DropdownMenuItem`s. */
export function CellMenu({ label, children }: { label: string; children: ReactNode }) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className="surface-cell-kebab" aria-label={label} title={label}>
          <MoreHorizontal className="h-4 w-4" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-[200px]">
        {children}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/** A small status dot: rail entries, the summary line. */
export function StatusDot({ tone }: { tone: 'attn' | 'on' | 'off' }) {
  return <span className={cn('surface-dot', `surface-dot-${tone}`)} aria-hidden />;
}

/** A `/command` chip; mono, quiet. */
export function CommandChip({ command }: { command: string }) {
  return <code className="surface-command">/{command}</code>;
}
