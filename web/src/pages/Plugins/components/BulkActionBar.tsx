import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';

/**
 * The floating action bar of select mode: selected count, per-action buttons
 * whose labels carry the count of rows the action actually applies to, and
 * the exit affordance (X or Escape). A destructive action confirms inline in
 * the bar itself; while a run is in flight the bar becomes the progress
 * readout, so the user watches the fan-out instead of wondering.
 */

export interface BulkAction {
  id: string;
  /** Full button label, count included ("Enable 3"). */
  label: string;
  destructive?: boolean;
  /** Present = the action swaps the bar into an inline confirm first. */
  confirmMessage?: string;
  disabled?: boolean;
  run: () => void;
}

export function BulkActionBar({
  count,
  actions,
  progress,
  onExit,
}: {
  count: number;
  actions: BulkAction[];
  progress: { done: number; total: number } | null;
  onExit: () => void;
}) {
  const { t } = useTranslation();
  const [confirming, setConfirming] = useState<BulkAction | null>(null);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key !== 'Escape') return;
      if (confirming) setConfirming(null);
      else onExit();
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [confirming, onExit]);

  const running = progress !== null;

  return (
    <div
      className="fixed bottom-6 left-1/2 -translate-x-1/2 z-40 flex items-center gap-3 px-3 py-2 rounded-lg"
      style={{
        backgroundColor: 'var(--color-bg-elevated)',
        border: '1px solid var(--color-border-muted)',
        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.12)',
      }}
      data-testid="bulk-action-bar"
    >
      {running ? (
        <span
          className="inline-flex items-center gap-2 text-xs"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          <Loader size={14} className="text-current" />
          {t('plugins.bulk.progress', { done: progress.done, total: progress.total })}
        </span>
      ) : confirming ? (
        <>
          <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {confirming.confirmMessage}
          </span>
          <button
            type="button"
            onClick={() => {
              const action = confirming;
              setConfirming(null);
              action.run();
            }}
            className="px-2 py-1 text-xs rounded"
            style={{ color: 'var(--color-loss)' }}
          >
            {confirming.label}
          </button>
          <button
            type="button"
            onClick={() => setConfirming(null)}
            className="px-2 py-1 text-xs rounded hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('common.cancel')}
          </button>
        </>
      ) : (
        <>
          <span className="text-xs font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {t('plugins.bulk.selected', { count })}
          </span>
          {actions.map((action) => (
            <button
              key={action.id}
              type="button"
              disabled={action.disabled}
              onClick={() => {
                if (action.confirmMessage) setConfirming(action);
                else action.run();
              }}
              className="px-2 py-1 text-xs rounded transition-colors hover:bg-foreground/10 disabled:opacity-40 disabled:hover:bg-transparent"
              style={{
                color: action.destructive
                  ? 'var(--color-loss)'
                  : 'var(--color-text-primary)',
              }}
            >
              {action.label}
            </button>
          ))}
          <button
            type="button"
            aria-label={t('common.cancel')}
            onClick={onExit}
            className="p-1 rounded transition-colors hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </>
      )}
    </div>
  );
}
