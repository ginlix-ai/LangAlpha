import { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { BulkScopeMenu, type BulkScopeSpec } from './BulkScopeMenu';

/**
 * The floating action bar of select mode: selected count, per-action buttons
 * whose labels carry the count of rows the action actually applies to, and
 * the exit affordance (X or Escape). A destructive action confirms inline in
 * the bar itself; while a run is in flight the bar becomes the progress
 * readout, so the user watches the fan-out instead of wondering.
 *
 * With nothing selected there are no counts to carry, so the bar asks for a
 * selection instead of offering a row of actions that all read zero. A button
 * labelled "Delete 0" is not an action, and four of them are a wall the user
 * has to read before finding out none of it applies yet.
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
  selectionKey,
  actions,
  scope,
  progress,
  onExit,
}: {
  count: number;
  /** Changes whenever the chosen set does (`bulkSelectionKey`). The confirm is
   *  armed against rows, not against a count, so this is what disarms it. */
  selectionKey: string;
  actions: BulkAction[];
  /** Present = the bar offers the bulk scope menu (Skills and MCP tabs). */
  scope?: BulkScopeSpec;
  progress: { done: number; total: number } | null;
  onExit: () => void;
}) {
  const { t } = useTranslation();
  // The id, never the action: an action closes over the rows it was built for,
  // so holding the object armed "Delete 2" against whatever was selected at
  // arm time and ran it against those rows however the selection moved after.
  // Re-resolving each render means the strip can only run today's action, and
  // an action the tab has withdrawn or disabled resolves to nothing at all.
  const [confirmingId, setConfirmingId] = useState<string | null>(null);
  const armed = confirmingId ? actions.find((a) => a.id === confirmingId) : undefined;
  const confirming = armed && !armed.disabled ? armed : null;
  const running = progress !== null;

  const exit = (
    <button
      type="button"
      aria-label={t('common.cancel')}
      onClick={onExit}
      className="p-1 rounded transition-colors hover:bg-foreground/10"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      <X className="h-3.5 w-3.5" />
    </button>
  );

  // Changing the selection out from under an open confirm disarms it. Emptying
  // it leaves the bar armed behind the empty-selection arm, so picking a row
  // again would drop the user straight back into a confirm they never
  // re-initiated; swapping which rows are chosen is worse, because the strip
  // stays on screen still reading "Delete 2" about two rows that are gone.
  useEffect(() => {
    setConfirmingId(null);
  }, [selectionKey]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key !== 'Escape') return;
      if (confirmingId) setConfirmingId(null);
      // Escape dismisses the bar, and the bar is what renders the progress
      // readout — leaving mid-run means the rest of the fan-out completes with
      // nothing on screen saying so. The run is not cancellable, so the honest
      // answer is to ignore Escape until it finishes.
      else if (!running) onExit();
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [confirmingId, onExit, running]);

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
      ) : count === 0 ? (
        <>
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('plugins.bulk.pickRows')}
          </span>
          {exit}
        </>
      ) : confirming ? (
        <>
          <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {confirming.confirmMessage}
          </span>
          <button
            type="button"
            onClick={() => setConfirmingId(null)}
            className="px-2 py-1 text-xs rounded hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('common.cancel')}
          </button>
          <button
            type="button"
            onClick={() => {
              const action = confirming;
              setConfirmingId(null);
              action.run();
            }}
            className="px-2 py-1 text-xs rounded"
            style={{ color: 'var(--color-loss)' }}
          >
            {confirming.label}
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
                if (action.confirmMessage) setConfirmingId(action.id);
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
          {scope && <BulkScopeMenu {...scope} />}
          {exit}
        </>
      )}
    </div>
  );
}
