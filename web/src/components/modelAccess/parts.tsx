import { useEffect, useId, useRef, useState } from 'react';
import { AlertTriangle, Check, Copy, ExternalLink } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { Button } from '@/components/ui/button';
import { cn } from '@/lib/utils';
import { copyText } from '@/lib/clipboard';

export type AccessStatus = 'connected' | 'needs_reauth' | 'working' | 'unknown';

/** One connection's live state, worded the same on every surface. */
export function StatusLabel({ status, className }: { status: AccessStatus; className?: string }) {
  const { t } = useTranslation();
  if (status === 'working') {
    return (
      <span className={cn('inline-flex items-center gap-1.5 text-xs', className)} style={{ color: 'var(--color-text-tertiary)' }}>
        <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />
        {t('modelAccess.status.working')}
      </span>
    );
  }
  if (status === 'needs_reauth') {
    return (
      <span className={cn('inline-flex items-center gap-1 text-xs', className)} style={{ color: 'var(--color-warning)' }}>
        <AlertTriangle aria-hidden="true" className="h-3 w-3 shrink-0" />
        {t('modelAccess.status.needsReauth')}
      </span>
    );
  }
  if (status === 'unknown') {
    return (
      <span className={cn('text-xs', className)} style={{ color: 'var(--color-text-tertiary)' }}>
        {t('modelAccess.status.unknown')}
      </span>
    );
  }
  return (
    <span className={cn('inline-flex items-center gap-1.5 text-xs', className)} style={{ color: 'var(--color-text-secondary)' }}>
      <span aria-hidden="true" className="h-1.5 w-1.5 rounded-full" style={{ background: 'var(--color-success)' }} />
      {t('modelAccess.status.connected')}
    </span>
  );
}

/** A labelled block in a connect flow's Before panel. */
export function FactRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <span className="text-xs font-medium" style={{ color: 'var(--color-text-tertiary)' }}>{label}</span>
      <div className="text-sm" style={{ color: 'var(--color-text-primary)' }}>{children}</div>
    </div>
  );
}

/** Model names as quiet chips. */
export function ModelChips({ names, emptyText }: { names: string[]; emptyText?: string }) {
  if (names.length === 0) {
    return emptyText ? <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{emptyText}</span> : null;
  }
  return (
    <ul className="flex flex-wrap gap-1.5">
      {names.map((name) => (
        <li
          key={name}
          className="rounded-full px-2 py-0.5 text-xs"
          style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)' }}
        >
          {name}
        </li>
      ))}
    </ul>
  );
}

/** Copies `text`, and says so for two seconds. */
export function CopyButton({
  text,
  className,
  onCopied,
}: {
  text: string;
  className?: string;
  onCopied?: () => void;
}) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (timer.current) clearTimeout(timer.current); }, []);

  const copy = async () => {
    try {
      await copyText(text);
      setCopied(true);
      onCopied?.();
      if (timer.current) clearTimeout(timer.current);
      timer.current = setTimeout(() => setCopied(false), 2000);
    } catch {
      // The code stays selectable on screen.
    }
  };

  return (
    <Button type="button" variant="outline" size="sm" onClick={copy} className={cn('gap-1.5', className)}>
      {copied ? <Check aria-hidden="true" className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} /> : <Copy aria-hidden="true" className="h-3.5 w-3.5" />}
      {copied ? t('modelAccess.copied') : t('modelAccess.copy')}
    </Button>
  );
}

/** A flow's failure: what happened, then what to do about it. */
export function FlowFailure({
  title,
  detail,
  action,
}: {
  title: string;
  detail?: string;
  action?: React.ReactNode;
}) {
  return (
    <div
      role="alert"
      className="flex flex-col gap-3 rounded-lg p-4"
      style={{ background: 'var(--color-loss-soft)', border: '1px solid var(--color-border-loss)' }}
    >
      <div className="flex items-start gap-2">
        <AlertTriangle aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-loss)' }} />
        <div className="flex flex-col gap-1">
          <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{title}</p>
          {detail && <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{detail}</p>}
        </div>
      </div>
      {action && <div className="flex flex-wrap items-center gap-2 pl-6">{action}</div>}
    </div>
  );
}

/** A quiet one-line note under a list or form. */
export function QuietNote({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <p className={cn('text-xs', className)} style={{ color: 'var(--color-text-tertiary)' }}>
      {children}
    </p>
  );
}

/** The endpoint is not one we can vouch for: what it may get wrong, as a warning. */
export function UnverifiedNote({ children }: { children: React.ReactNode }) {
  const { t } = useTranslation();
  return (
    <div role="note" className="flex gap-2.5 rounded-lg px-3 py-2.5" style={{ background: 'var(--color-warning-soft)' }}>
      <AlertTriangle aria-hidden="true" className="mt-0.5 h-4 w-4 shrink-0" style={{ color: 'var(--color-warning)' }} />
      <p className="text-xs leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
        <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>{t('setup.customUnverifiedTitle')}</span>{' '}
        {children}
      </p>
    </div>
  );
}

/** One step of a sign-in flow: done, in progress, or still to come. */
export function StepRow({ state, children }: { state: 'done' | 'active' | 'todo'; children: React.ReactNode }) {
  return (
    <li className="flex items-start gap-2.5 text-sm">
      <span aria-hidden="true" className="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center">
        {state === 'done' && <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} />}
        {state === 'active' && <Loader size={13} style={{ color: 'var(--color-accent-primary)' }} />}
        {state === 'todo' && <span className="h-1.5 w-1.5 rounded-full" style={{ background: 'var(--color-text-quaternary)' }} />}
      </span>
      <span style={{ color: state === 'todo' ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' }}>{children}</span>
    </li>
  );
}

/**
 * The provider's sign-in page, opened by the user's own click: no popup
 * blocker stands in the way, and nothing leaves before they are ready.
 * Primary until it has been used once, then quiet.
 */
export function OpenProviderLink({
  href,
  used,
  onOpen,
  children,
}: {
  href: string;
  used: boolean;
  onOpen: () => void;
  children: React.ReactNode;
}) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      onClick={onOpen}
      className={cn(
        'inline-flex h-9 items-center gap-1.5 rounded-md px-3 text-sm font-medium transition-colors',
        used
          ? 'border border-input bg-background hover:bg-(--color-bg-hover)'
          : 'bg-(--color-btn-primary-bg) text-(--color-btn-primary-text) hover:opacity-90',
      )}
    >
      {children}
      <ExternalLink aria-hidden="true" className="h-3.5 w-3.5" />
    </a>
  );
}

/** A small one-of-several choice, as a row of joined buttons. */
export function Segmented<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: readonly { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className="clips-focus-ring inline-flex overflow-hidden rounded-md"
      style={{ border: '1px solid var(--color-border-default)' }}
    >
      {options.map((o, i) => {
        const on = o.value === value;
        return (
          <button
            key={o.value}
            type="button"
            aria-pressed={on}
            onClick={() => onChange(o.value)}
            className="px-3 py-1 text-xs font-medium transition-colors"
            style={{
              background: on ? 'var(--color-bg-tag)' : 'transparent',
              color: on ? 'var(--color-text-primary)' : 'var(--color-text-secondary)',
              borderLeft: i > 0 ? '1px solid var(--color-border-default)' : undefined,
            }}
          >
            {o.label}
          </button>
        );
      })}
    </div>
  );
}

/** The input modalities a model can be marked with beyond text, which every
 *  model reads and so shows as a fixed tag. */
const EXTRA_MODALITIES = ['image', 'pdf'] as const;

/**
 * The "Capabilities:" row under a model: the fixed text tag, then a pressed
 * toggle for each extra modality in `active`. `className` places the row (its
 * padding or indent where it sits).
 */
export function ModalityChips({
  active,
  onToggle,
  className,
}: {
  active: ReadonlySet<string>;
  onToggle: (modality: string) => void;
  className?: string;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  return (
    <div role="group" aria-labelledby={labelId} className={cn('flex flex-wrap items-center gap-1.5', className)}>
      <span
        id={labelId}
        className="text-[0.625rem] uppercase tracking-wider"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {t('setup.capabilities', { defaultValue: 'Capabilities' })}:
      </span>
      <span
        className="inline-flex items-center rounded-full px-2 py-0.5 text-[0.625rem] font-medium"
        style={{ background: 'var(--color-bg-tag)', color: 'var(--color-text-secondary)' }}
      >
        {t('setup.modalityText')}
      </span>
      {EXTRA_MODALITIES.map((mod) => {
        const on = active.has(mod);
        return (
          <button
            key={mod}
            type="button"
            aria-pressed={on}
            onClick={() => onToggle(mod)}
            className="inline-flex items-center rounded-full px-2 py-0.5 text-[0.625rem] font-medium transition-colors"
            style={{
              background: on ? 'var(--color-bg-tag)' : 'transparent',
              color: on ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
              border: `1px solid ${on ? 'var(--color-text-primary)' : 'var(--color-border-default)'}`,
            }}
          >
            <span>{mod === 'image' ? t('setup.modalityImage') : t('setup.modalityPdf')}</span>
          </button>
        );
      })}
    </div>
  );
}
