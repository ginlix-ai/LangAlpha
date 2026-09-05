import React from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from 'framer-motion';
import { Check, Download, MoreVertical, Plus } from 'lucide-react';
import { Loader } from '@/components/ui/loader';

/**
 * Shared building blocks for the two MCP server surfaces — the global
 * Plugins page (user level) and the workspace settings MCP tab. Both lists
 * must read as one system: same row anatomy (name line → status line →
 * detail), same toggle, same chrome. Anything visual that exists on both
 * sides lives here; the surfaces keep only their own semantics.
 */

// Matches the spring used across the chat UI (ActivityBlock) so motion feels
// consistent. The toggle knob's travel IS the state change, so it springs.
export const SPRING_SNAPPY = { type: 'spring' as const, stiffness: 200, damping: 22 };

// House entrance curve (DESIGN.md § Motion): ease-out, no overshoot.
const EASE_OUT = [0.16, 1, 0.3, 1] as const;

// Rows never travel. A filter answers a question — which servers match — and
// the answer is the list, not a journey to it; sliding a card in from a
// position it never occupied narrates a move the user did not make. So no
// `layout` prop and no y-offset: membership changes read as fade in, close
// up. The one vertical motion left is a leaving row collapsing its own
// height, which is the list closing the gap it just made, in place.
const FADE_IN = { duration: 0.15, ease: EASE_OUT };

// Exits tween rather than spring: a spring's settle tail makes a batch of
// leaving rows hold phantom height for ~350ms and then vanish in one frame
// (the presence wrapper waits for every spring to rest). A short
// deterministic tween ends all exits together, so a filter swap reads as one
// clean motion.
const EXIT_TWEEN = { duration: 0.15, ease: EASE_OUT };

/** Row container: identity tile + content column left, actions column right.
 *  In select mode (`selecting`) the whole row becomes the checkbox — a leading
 *  box reflects `selected`, the row's own controls go inert so a click
 *  anywhere toggles selection instead of firing a toggle or menu. `onOpen`
 *  makes the content column a pointer target for the row's detail view; the
 *  keyboard path is the name button inside `ServerNameLine`, so this click
 *  surface stays out of the accessibility tree. */
export function ServerRowShell({
  testid,
  tile,
  main,
  actions,
  selecting = false,
  selected = false,
  onSelectToggle,
  onOpen,
}: {
  testid: string;
  tile?: React.ReactNode;
  main: React.ReactNode;
  actions: React.ReactNode;
  selecting?: boolean;
  selected?: boolean;
  onSelectToggle?: () => void;
  onOpen?: () => void;
}) {
  const selectable = selecting && !!onSelectToggle;
  const openable = !!onOpen && !selectable;
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{
        opacity: 0,
        height: 0,
        marginTop: 0,
        paddingTop: 0,
        paddingBottom: 0,
        transition: EXIT_TWEEN,
      }}
      transition={FADE_IN}
      className={`flex items-start justify-between gap-3 py-2.5 px-3 rounded-lg overflow-hidden bg-[var(--color-bg-card)] ${
        selectable ? 'cursor-pointer' : ''
      }${
        // The fill lives in the class (not style) so the hover twin can win;
        // only rows that open a detail view invite the pointer.
        openable ? ' transition-colors duration-150 hover:bg-[var(--color-bg-card-hover)]' : ''
      }`}
      style={{
        // Always set (never conditionally spread): motion.div applies style
        // imperatively and leaves a vanished key painted on the element.
        boxShadow:
          selectable && selected ? 'inset 0 0 0 1px var(--color-accent-primary)' : 'none',
      }}
      data-testid={testid}
      {...(selectable
        ? {
            role: 'checkbox' as const,
            'aria-checked': selected,
            tabIndex: 0,
            onClick: onSelectToggle,
            onKeyDown: (e: React.KeyboardEvent) => {
              if (e.key === ' ' || e.key === 'Enter') {
                e.preventDefault();
                onSelectToggle?.();
              }
            },
          }
        : {})}
    >
      {selectable && (
        <span
          aria-hidden
          className="flex-shrink-0 mt-2 inline-flex h-4 w-4 items-center justify-center rounded"
          style={{
            border: selected ? 'none' : '1px solid var(--color-border-muted)',
            backgroundColor: selected ? 'var(--color-accent-primary)' : 'transparent',
          }}
        >
          {selected && (
            <Check className="h-3 w-3" style={{ color: 'var(--color-btn-primary-text)' }} />
          )}
        </span>
      )}
      {tile && <div className="flex-shrink-0 mt-0.5">{tile}</div>}
      <div
        className={`min-w-0 flex flex-col gap-1 flex-1 ${
          selectable ? 'pointer-events-none select-none' : ''
        }${openable ? ' cursor-pointer' : ''}`}
        {...(openable ? { onClick: onOpen } : {})}
      >
        {main}
      </div>
      <div
        className={`flex items-center gap-2 flex-shrink-0 ${
          selectable ? 'pointer-events-none opacity-40' : ''
        }`}
      >
        {actions}
      </div>
    </motion.div>
  );
}

/** Identity line: name + state badges. The visual identity mark is the
 *  row's `IdentityTile` (via `ServerRowShell`'s tile slot), not an icon here.
 *  With `onOpen` the name renders as a real button — the keyboard/AT path to
 *  the row's detail view (the row body is only a pointer convenience). */
export function ServerNameLine({
  name,
  onOpen,
  children,
}: {
  name: string;
  onOpen?: () => void;
  children?: React.ReactNode;
}) {
  const nameEl = onOpen ? (
    <button
      type="button"
      onClick={(e) => {
        // The row body is its own click-through to the same detail view.
        e.stopPropagation();
        onOpen();
      }}
      className="text-sm font-medium truncate text-left hover:underline underline-offset-2"
      style={{ color: 'var(--color-text-primary)' }}
    >
      {name}
    </button>
  ) : (
    <span className="text-sm font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
      {name}
    </span>
  );
  return (
    <div className="flex items-center gap-2 flex-wrap">
      {nameEl}
      {children}
    </div>
  );
}

/** The small uppercase tag beside a server name (origin / transport). `soft`
 *  drops the border + uppercase for secondary annotations. */
export function TagBadge({
  title,
  soft = false,
  tone = 'muted',
  children,
}: {
  title?: string;
  soft?: boolean;
  /** `warning` borrows the pill's colour and not its fill, the same rule
   *  `RowNote` follows: a tinted background here would make this the pill it
   *  is deliberately not. For a badge naming something that moves real money. */
  tone?: 'muted' | 'warning';
  children: React.ReactNode;
}) {
  const warn = tone === 'warning';
  return (
    <span
      className={
        soft
          ? 'text-[0.625rem] px-1.5 py-0.5 rounded'
          : 'text-[0.625rem] px-1.5 py-0.5 rounded uppercase tracking-wide'
      }
      style={{
        color: warn ? 'var(--color-warning)' : 'var(--color-text-tertiary)',
        backgroundColor: 'var(--color-bg-tag)',
        ...(soft
          ? {}
          : {
              border: `1px solid ${
                warn ? 'var(--color-warning)' : 'var(--color-border-muted)'
              }`,
            }),
      }}
      title={title}
    >
      {children}
    </span>
  );
}

/** Quiet metadata on a row's second line (transport, version, counts,
 *  provenance) — plain tertiary text, deliberately not a badge: badges are
 *  reserved for state that needs attention. */
export function MetaText({
  title,
  children,
}: {
  title?: string;
  children: React.ReactNode;
}) {
  return (
    <span
      className="text-[0.6875rem]"
      style={{ color: 'var(--color-text-tertiary)' }}
      title={title}
    >
      {children}
    </span>
  );
}

/** The base status pill. Status vocabularies (lifecycle, OAuth) map their
 *  states onto this one shape so the two surfaces stay pixel-identical. */
export function StatusPill({
  icon: Icon,
  label,
  color,
  bg,
  title,
  testid,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  color: string;
  bg: string;
  title?: string;
  testid?: string;
}) {
  return (
    <span
      className="inline-flex items-center gap-1 text-[0.6875rem] px-1.5 py-0.5 rounded font-medium"
      style={{ color, backgroundColor: bg }}
      title={title}
      data-testid={testid}
    >
      <Icon className="h-3 w-3" />
      {label}
    </span>
  );
}

/** The enabled switch. Same spring on both surfaces — this knob IS the product
 *  feel of turning a server on. */
export function EnabledToggle({
  enabled,
  name,
  disabled = false,
  onToggle,
}: {
  enabled: boolean;
  name: string;
  disabled?: boolean;
  onToggle: () => void;
}) {
  const { t } = useTranslation();
  return (
    <button
      type="button"
      role="switch"
      aria-checked={enabled}
      aria-label={enabled ? t('mcp.row.disableAria', { name }) : t('mcp.row.enableAria', { name })}
      disabled={disabled}
      onClick={onToggle}
      className="relative inline-flex h-5 w-9 items-center rounded-full transition-colors"
      style={{
        backgroundColor: enabled ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
      }}
    >
      <motion.span
        className="inline-block h-4 w-4 rounded-full bg-white"
        animate={{ x: enabled ? 18 : 2 }}
        transition={SPRING_SNAPPY}
      />
    </button>
  );
}

/** Kebab trigger for the row actions dropdown (forwardRef for Radix asChild). */
export const KebabTrigger = React.forwardRef<
  HTMLButtonElement,
  { busy?: boolean } & React.ButtonHTMLAttributes<HTMLButtonElement>
>(function KebabTrigger({ busy = false, ...props }, ref) {
  return (
    <button
      ref={ref}
      type="button"
      className="p-1.5 rounded transition-colors hover:bg-foreground/10"
      style={{ color: 'var(--color-text-tertiary)' }}
      {...props}
    >
      {busy ? <Loader size={16} className="text-current" /> : <MoreVertical className="h-4 w-4" />}
    </button>
  );
});

/** Small caps label above a group of rows (`Platform servers`, `Your skills`). */
export function SectionHeader({ children }: { children: React.ReactNode }) {
  return (
    <h3
      className="text-[0.6875rem] font-medium uppercase tracking-wide"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </h3>
  );
}

/** List header: icon + title + `n / max` counter on the left, actions right. */
export function ListHeader({
  icon: Icon,
  title,
  count,
  max,
  children,
}: {
  icon: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
  title: string;
  count: number;
  max: number;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-2">
        <Icon className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
        <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
          {title}
        </span>
        <span
          className="text-xs px-1.5 py-0.5 rounded"
          style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-card)' }}
        >
          {count} / {max}
        </span>
      </div>
      {children && <div className="flex items-center gap-1.5">{children}</div>}
    </div>
  );
}

/** Header action button: `primary` (Add), `secondary` (Import), `ghost` (links). */
export function HeaderButton({
  variant = 'secondary',
  icon: Icon,
  children,
  ...props
}: {
  variant?: 'primary' | 'secondary' | 'ghost';
  icon?: React.ComponentType<{ className?: string }>;
  children: React.ReactNode;
} & React.ButtonHTMLAttributes<HTMLButtonElement>) {
  const style: React.CSSProperties =
    variant === 'primary'
      ? { color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }
      : variant === 'secondary'
        ? { color: 'var(--color-text-secondary)', border: '1px solid var(--color-border-muted)' }
        : { color: 'var(--color-text-tertiary)' };
  return (
    <button
      type="button"
      className={
        variant === 'ghost'
          ? 'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors hover:bg-foreground/10'
          : 'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md transition-colors disabled:opacity-50'
      }
      style={style}
      {...props}
    >
      {Icon && <Icon className="h-3 w-3" />}
      {children}
    </button>
  );
}

/**
 * The header both server lists wear: counter on the left, Import + Add on the
 * right, and the at-cap explanation that has to appear on BOTH buttons (it was
 * spelled out four times, and the two lists had already stopped agreeing on
 * which of them said what). `children` takes a surface's own extra action.
 */
export function ListToolbar({
  icon,
  title,
  count,
  max,
  atCap,
  onImport,
  onAdd,
  children,
}: {
  icon: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
  title: string;
  count: number;
  max: number;
  atCap: boolean;
  onImport: () => void;
  onAdd: () => void;
  children?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const atCapHint = atCap ? t('mcp.list.atCap', { max }) : undefined;
  return (
    <ListHeader icon={icon} title={title} count={count} max={max}>
      {children}
      <HeaderButton
        variant="secondary"
        icon={Download}
        onClick={onImport}
        disabled={atCap}
        title={atCapHint ?? t('mcp.list.importHint')}
      >
        {t('mcp.list.importJson')}
      </HeaderButton>
      <HeaderButton variant="primary" icon={Plus} onClick={onAdd} disabled={atCap} title={atCapHint}>
        {t('mcp.list.addServer')}
      </HeaderButton>
    </ListHeader>
  );
}

/** Inline confirm strip (delete / overwrite): message left, verdict buttons right. */
export function ConfirmStrip({
  message,
  confirmLabel,
  confirmVariant = 'destructive',
  cancelLabel,
  pending = false,
  onConfirm,
  onCancel,
}: {
  message: React.ReactNode;
  confirmLabel: string;
  confirmVariant?: 'destructive' | 'primary';
  cancelLabel: string;
  pending?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div
      className="flex items-center justify-between gap-3 text-[0.6875rem] p-2 rounded"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-muted)',
      }}
    >
      <span className="min-w-0">{message}</span>
      <div className="flex items-center gap-1.5 flex-shrink-0">
        <button
          type="button"
          onClick={onConfirm}
          disabled={pending}
          className="px-2 py-1 rounded disabled:opacity-50"
          style={
            confirmVariant === 'destructive'
              ? { color: 'var(--color-loss)' }
              : { color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }
          }
        >
          {confirmLabel}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-2 py-1 rounded hover:bg-foreground/10"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {cancelLabel}
        </button>
      </div>
    </div>
  );
}

/** Loading placeholder rows. */
export function ListSkeleton({ rows = 3 }: { rows?: number }) {
  return (
    <div className="flex flex-col gap-2">
      {Array.from({ length: rows }, (_, i) => (
        <div
          key={i}
          className="h-14 rounded-lg animate-pulse"
          style={{ backgroundColor: 'var(--color-bg-card)' }}
        />
      ))}
    </div>
  );
}

export function ListError({ children }: { children: React.ReactNode }) {
  return (
    <div
      className="text-xs p-2 rounded"
      style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}
    >
      {children}
    </div>
  );
}

export function ListEmpty({ children }: { children: React.ReactNode }) {
  return (
    <div className="py-8 text-center text-sm" style={{ color: 'var(--color-text-tertiary)' }}>
      {children}
    </div>
  );
}
