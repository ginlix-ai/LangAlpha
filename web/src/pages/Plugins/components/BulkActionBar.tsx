import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useToastInset } from '@/components/ui/use-toast';
import { BulkScopeMenu, type BulkScopeSpec } from './BulkScopeMenu';
import './surface/surface.css';

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
  note,
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
  /** Why some of the selection is left out of an action ("2 came with a
   *  plugin and can't be removed"), said here rather than found out after. */
  note?: string | null;
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

  // Toasts rise above the bar while it is up: a result or a sign-in offer
  // landing in the bottom corner otherwise sits on the bar's own buttons.
  const barRef = useRef<HTMLDivElement>(null);
  useToastInset(barRef);

  const exit = (
    <button type="button" aria-label={t('common.cancel')} onClick={onExit} className="bulk-bar-exit">
      <X className="h-4 w-4" />
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
      // A press something else already answered (the search clearing itself,
      // a menu closing) is spent: it must not also throw the selection away.
      if (e.key !== 'Escape' || e.defaultPrevented) return;
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
    <>
      {/* Room under the last row where the bar floats (a phone, above the
          tab bar), so it never sits on the cells it acts on. */}
      <div className="bulk-bar-spacer" aria-hidden />
      <div
        ref={barRef}
        className="bulk-bar"
        role="toolbar"
        aria-label={t('plugins.bulk.barLabel')}
        data-testid="bulk-action-bar"
      >
        {running ? (
          <span className="bulk-bar-status">
            <Loader size={14} className="text-current" />
            {t('plugins.bulk.progress', { done: progress.done, total: progress.total })}
          </span>
        ) : count === 0 ? (
          <>
            <span className="bulk-bar-status">{t('plugins.bulk.pickRows')}</span>
            {exit}
          </>
        ) : confirming ? (
          <>
            <span className="bulk-bar-status is-ask">{confirming.confirmMessage}</span>
            <div className="bulk-bar-actions">
              <button type="button" onClick={() => setConfirmingId(null)} className="bulk-bar-btn">
                {t('common.cancel')}
              </button>
              <button
                type="button"
                onClick={() => {
                  const action = confirming;
                  setConfirmingId(null);
                  action.run();
                }}
                className="bulk-bar-btn is-destructive"
              >
                {confirming.label}
              </button>
            </div>
          </>
        ) : (
          <>
            <span className="bulk-bar-count">{t('plugins.bulk.selected', { count })}</span>
            <div className="bulk-bar-actions">
              {actions.map((action) => (
                <button
                  key={action.id}
                  type="button"
                  disabled={action.disabled}
                  onClick={() => {
                    if (action.confirmMessage) setConfirmingId(action.id);
                    else action.run();
                  }}
                  className={action.destructive ? 'bulk-bar-btn is-destructive' : 'bulk-bar-btn'}
                >
                  {action.label}
                </button>
              ))}
              {scope && <BulkScopeMenu {...scope} />}
            </div>
            {exit}
            {note && <p className="bulk-bar-note">{note}</p>}
          </>
        )}
      </div>
    </>
  );
}
