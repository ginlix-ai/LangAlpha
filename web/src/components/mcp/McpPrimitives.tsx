import React, { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from '@/lib/framer';
import { Download, MoreVertical, Plus } from 'lucide-react';
import { PresenceFold } from '@/components/ui/Disclosure';
import { Loader } from '@/components/ui/loader';
import { SPRING_INDICATOR, TWEEN } from '@/lib/motion';
import { cn } from '@/lib/utils';

/**
 * Shared building blocks for the list surfaces: the Plugins page, the
 * workspace settings MCP tab, and the Orders page. They must read as one
 * system: same row anatomy (name line, status line, detail), same toggle,
 * same chrome. Anything visual that exists on more than one of them lives
 * here; the surfaces keep only their own semantics.
 */

// The toggle knob's travel IS the state change, so it springs, on the house
// indicator spring so it settles on arrival instead of overshooting.
// Rows never travel. A filter answers a question — which servers match — and
// the answer is the list, not a journey to it; sliding a card in from a
// position it never occupied narrates a move the user did not make. So no
// `layout` prop and no y-offset: membership changes read as fade in, close
// up. The one vertical motion left is a leaving row collapsing its own
// height, which is the list closing the gap it just made, in place.
const FADE_IN = TWEEN.quick;

const NO_SIBLING_MARGIN = { marginTop: 0 };

/** Row container: identity tile + content column left, actions column right.
 *  `onOpen` makes the content column a pointer target for the row's detail
 *  view; the keyboard path is the name button inside `ServerNameLine`, so this
 *  click surface stays out of the accessibility tree. */
export function ServerRowShell({
  testid,
  tile,
  main,
  actions,
  onOpen,
}: {
  testid: string;
  tile?: React.ReactNode;
  main: React.ReactNode;
  actions: React.ReactNode;
  onOpen?: () => void;
}) {
  // The fold collapses; the row inside never changes shape. It shuts over the
  // exit beat, a tween rather than a spring: a spring's settle tail made a
  // batch of leaving rows hold phantom height for ~350ms and then vanish in
  // one frame, since the list waits for every row to finish. The list's gap
  // moves inside the fold, so the collapse swallows it: the box takes no
  // sibling margin, and a row after the first pads its own top instead.
  return (
    <PresenceFold
      className="clips-focus-ring [:not(:first-child)>&]:pt-1.5"
      style={NO_SIBLING_MARGIN}
    >
      <motion.div
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={FADE_IN}
        className={`flex items-start justify-between gap-3 py-2.5 px-3 rounded-lg overflow-hidden bg-(--color-bg-card)${
          // The fill lives in the class (not style) so the hover twin can win;
          // only rows that open a detail view invite the pointer.
          onOpen ? ' transition-colors hover:bg-(--color-bg-card-hover)' : ''
        }`}
        data-testid={testid}
      >
        {tile && <div className="shrink-0 mt-0.5">{tile}</div>}
        <div
          className={`min-w-0 flex flex-col gap-1 flex-1${onOpen ? ' cursor-pointer' : ''}`}
          {...(onOpen ? { onClick: onOpen } : {})}
        >
          {main}
        </div>
        <div className="flex items-center gap-2 shrink-0">{actions}</div>
      </motion.div>
    </PresenceFold>
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
 *  states onto this one shape so the two surfaces stay pixel-identical.
 *  `capsule` is for a pill nested in a fully rounded control, whose corners
 *  it then follows. */
export function StatusPill({
  icon: Icon,
  label,
  color,
  bg,
  title,
  testid,
  capsule = false,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  color: string;
  bg: string;
  title?: string;
  testid?: string;
  capsule?: boolean;
}) {
  return (
    <span
      // The glyph sits on the first line of the label rather than at the
      // middle of the pill, so a label that wraps keeps it beside its words.
      className={`inline-flex items-start gap-1 text-[0.6875rem] leading-4 py-0.5 font-medium ${
        capsule ? 'rounded-full px-2' : 'rounded px-1.5'
      }`}
      style={{ color, backgroundColor: bg }}
      title={title}
      data-testid={testid}
    >
      <Icon className="h-3 w-3 mt-0.5 shrink-0" />
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
        transition={SPRING_INDICATOR}
      />
    </button>
  );
}

/** Kebab trigger for the row actions dropdown; the ref reaches the button for Radix asChild. */
export function KebabTrigger({
  busy = false,
  ...props
}: { busy?: boolean } & React.ComponentProps<'button'>) {
  return (
    <button
      type="button"
      className="p-1.5 rounded transition-colors hover:bg-foreground/10"
      style={{ color: 'var(--color-text-tertiary)' }}
      {...props}
    >
      {busy ? <Loader size={16} className="text-current" /> : <MoreVertical className="h-4 w-4" />}
    </button>
  );
}

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
  /** The per-account cap; a list with none shows the bare count. */
  max?: number;
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
          {max === undefined ? count : `${count} / ${max}`}
        </span>
      </div>
      {children && <div className="flex items-center gap-1.5">{children}</div>}
    </div>
  );
}

const HEADER_BUTTON = {
  primary:
    'text-(color:--color-btn-primary-text) bg-(color:--color-btn-primary-bg) disabled:opacity-50',
  secondary:
    'text-(color:--color-text-secondary) border-(color:--color-border-muted) disabled:opacity-50',
  ghost: 'text-(color:--color-text-tertiary) hover:bg-foreground/10 disabled:opacity-50 disabled:hover:bg-transparent',
};

/** Header action button: `primary` (Add), `secondary` (Import), `ghost` (links).
 *  Its colors are classes, so a caller's `className` can restyle a state. */
export function HeaderButton({
  variant = 'secondary',
  icon: Icon,
  className,
  children,
  ...props
}: {
  variant?: keyof typeof HEADER_BUTTON;
  icon?: React.ComponentType<{ className?: string }>;
  children: React.ReactNode;
} & React.ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      type="button"
      className={cn(
        // Every variant takes the secondary's 1px edge, unpainted where it
        // draws none, so mixed variants in one row stand the same height.
        'inline-flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-md border border-transparent transition-colors',
        HEADER_BUTTON[variant],
        className,
      )}
      {...props}
    >
      {Icon && <Icon className="h-3 w-3" />}
      {children}
    </button>
  );
}

/**
 * A server list's header: counter on the left, Import + Add on the right.
 * `children` takes a surface's own extra action. `cap` is the account's count
 * and limit, passed apart from `count` because a list showing one workspace's
 * selection cannot count against it; at the limit both adds are held, with
 * the reason on each, before a form opens only to be refused.
 */
export function ListToolbar({
  icon,
  title,
  count,
  cap,
  onImport,
  onAdd,
  children,
}: {
  icon: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
  title: string;
  count: number;
  cap?: { used: number; max: number };
  onImport: () => void;
  onAdd: () => void;
  children?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const atCapHint = cap && cap.used >= cap.max ? t('mcp.list.atCap', { max: cap.max }) : undefined;
  return (
    <ListHeader icon={icon} title={title} count={count}>
      {children}
      <HeaderButton
        variant="secondary"
        icon={Download}
        onClick={onImport}
        disabled={!!atCapHint}
        title={atCapHint ?? t('mcp.list.importHint')}
      >
        {t('mcp.list.importJson')}
      </HeaderButton>
      <HeaderButton
        variant="primary"
        icon={Plus}
        onClick={onAdd}
        disabled={!!atCapHint}
        title={atCapHint}
      >
        {t('mcp.list.addServer')}
      </HeaderButton>
    </ListHeader>
  );
}

/** Inline confirm strip (delete / overwrite): message left, verdict buttons
 *  right, in the house order of Cancel then the action. Focus moves onto
 *  Cancel when it appears, so a keyboard user lands on the strip and Escape
 *  backs out of it. */
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
  const cancelRef = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    cancelRef.current?.focus({ preventScroll: true });
  }, []);
  return (
    <div
      role="group"
      aria-live="polite"
      onKeyDown={(e) => {
        if (e.key !== 'Escape') return;
        e.stopPropagation();
        onCancel();
      }}
      className="flex items-center justify-between gap-3 text-[0.6875rem] p-2 rounded"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        color: 'var(--color-text-secondary)',
        border: '1px solid var(--color-border-muted)',
      }}
    >
      <span className="min-w-0">{message}</span>
      <div className="flex items-center gap-1.5 shrink-0">
        <button
          ref={cancelRef}
          type="button"
          onClick={onCancel}
          className="px-2 py-1 rounded transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {cancelLabel}
        </button>
        <button
          type="button"
          onClick={onConfirm}
          disabled={pending}
          className="px-2 py-1 rounded transition-colors hover:bg-foreground/10 disabled:opacity-50 disabled:pointer-events-none"
          style={
            confirmVariant === 'destructive'
              ? { color: 'var(--color-loss)' }
              : { color: 'var(--color-btn-primary-text)', backgroundColor: 'var(--color-btn-primary-bg)' }
          }
        >
          {confirmLabel}
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

export function ListError({
  children,
  onRetry,
}: {
  children: React.ReactNode;
  /** A load that failed is usually worth one more try, offered where it is read. */
  onRetry?: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div
      className="text-xs p-2 rounded"
      style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-loss)' }}
    >
      {children}
      {onRetry && (
        <>
          {' '}
          <button type="button" onClick={onRetry} className="underline underline-offset-2">
            {t('common.retry')}
          </button>
        </>
      )}
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

/**
 * A labelled field in a form on these surfaces: the MCP add/edit modal and the
 * vault's own secret forms, which had a copy each. The label sits above rather
 * than inside, because a placeholder that empties on the first keystroke is no
 * longer there to say which field this is; `htmlFor` is what makes the pair a
 * real label for a screen reader, and the hint reads under the control it
 * qualifies.
 */
export function Field({
  label,
  hint,
  htmlFor,
  children,
}: {
  label: string;
  hint?: string;
  htmlFor?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-1">
      <label
        htmlFor={htmlFor}
        className="text-xs font-medium"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {label}
      </label>
      {children}
      {hint && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
          {hint}
        </p>
      )}
    </div>
  );
}

/** The validation line under a field, absent when the field is fine. */
export function FieldError({ error }: { error?: { message: string } }) {
  if (!error) return null;
  return (
    <p className="text-[0.6875rem]" style={{ color: 'var(--color-loss)' }}>
      {error.message}
    </p>
  );
}
