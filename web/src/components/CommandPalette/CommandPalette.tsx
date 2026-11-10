import { useEffect, useId, useMemo, useRef, type KeyboardEvent } from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';
import { useIsMobile } from '@/hooks/useIsMobile';
import { isImeComposing } from '@/lib/ime';
import { focusWithoutRing, lastInputWasPointer } from '@/lib/inputModality';
import { cn } from '@/lib/utils';
import { PaletteField } from './PaletteField';
import { PaletteFooter } from './PaletteFooter';
import { PaletteRow } from './PaletteRow';
import { PrefixHint } from './PrefixHint';
import { PreviewPane } from './PreviewPane';
import { ResultGroups } from './ResultGroups';
import { fieldLabel, lensLabel, placeholder, scopeChips } from './labels';
import { canEnter, scopeOf } from './scope';
import type { PaletteAction, PaletteItem } from './types';
import type { Verbs } from './rows';
import { useLocalSources } from './useLocalSources';
import { usePaletteView, type PaletteDispatch, type Wait } from './usePaletteView';
import { useArchiveThread, useVerbs } from './useVerbs';
import { useViewResult } from './useViewResult';
import './CommandPalette.css';

/** How long Enter waits on a server that may still put rows above Ask, before it takes what leads. */
const SERVER_LEAD_WAIT_MS = 2000;

interface CommandPaletteProps {
  open: boolean;
  onClose: () => void;
  /** What was typed after ⌘K before the field had focus. */
  typed: string;
  /** Where focus was when the palette was asked to open, to hand it back on close. */
  returnFocus: () => HTMLElement | null;
}

/**
 * The ⌘K command palette: one field that finds threads, what was said in
 * them, files, tickers, workspaces, skills, pages, settings and actions, and
 * hands what was typed to the agent. A prefix typed first narrows it to one
 * kind (`$ / # " @ >`), and Tab on a workspace or thread to that place.
 * Pinned near the top so it grows downward without the field moving.
 */
export default function CommandPalette({ open, onClose, typed, returnFocus }: CommandPaletteProps) {
  const { t } = useTranslation();
  // Radix hands focus back only to a Dialog.Trigger, which the palette has
  // none of, so it hands it back itself, to where its host saw it last.
  const returnRef = useRef<HTMLElement | null>(null);
  const { archiveThread, dialog: archiveConfirm } = useArchiveThread();

  return (
    <>
      <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
        <DialogContent
          variant="command"
          className="command-palette"
          onOpenAutoFocus={(e) => {
            returnRef.current = returnFocus();
            // ⌘K opens a place to type, not a walk through the page with the
            // keyboard, so the field takes focus as a clicked one would: no ring.
            e.preventDefault();
            focusWithoutRing((e.currentTarget as HTMLElement | null)?.querySelector('input'));
          }}
          onCloseAutoFocus={(e) => {
            e.preventDefault();
            // Focus goes back where it was unless something took it since: the
            // page a row led to (a composer it filled), or a click elsewhere.
            // The palette's own field, removed with it, leaves it on the body.
            const now = document.activeElement;
            if ((!now || now === document.body) && !lastInputWasPointer()) focusWithoutRing(returnRef.current);
          }}
          onEscapeKeyDown={(e) => {
            // Escape in an IME composition drops the candidates, nothing more.
            // In the field (where focus lives) the field answers it: it steps
            // back out of a row's actions or a view before it closes. Radix
            // hears the key first, on the document's capture phase, so the
            // field could not stop it from there.
            if (isImeComposing(e) || (e.target as Element | null)?.closest?.('.command-palette-field')) e.preventDefault();
          }}
        >
          <DialogTitle className="sr-only">{t('palette.title')}</DialogTitle>
          <PaletteBody open={open} onClose={onClose} typed={typed} archiveThread={archiveThread} />
        </DialogContent>
      </Dialog>
      {archiveConfirm}
    </>
  );
}

/**
 * An action that runs closes the palette first; one that opens a view opens
 * it in the palette, and one that edits the row opens the edit in the field.
 */
function performAction(action: PaletteAction, dispatch: PaletteDispatch, close: () => void) {
  if (action.effect === 'open') {
    dispatch({ type: 'open', lens: action.view });
    return;
  }
  if (action.effect === 'edit') {
    dispatch({ type: 'edit', action });
    return;
  }
  close();
  action.run();
}

interface BodyProps {
  open: boolean;
  onClose: () => void;
  typed: string;
  archiveThread: Verbs['archiveThread'];
}

/** Mounted with the dialog's content, so every open starts from an empty field. */
function PaletteBody({ open, onClose, typed, archiveThread }: BodyProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const listId = useId();
  const listRef = useRef<HTMLDivElement>(null);
  const { frames, top, drilled, actionsOf: actionsOfId, actionIndex, edit, waiting, waitedOut, dispatch } = usePaletteView(open, typed);
  const { spec, query, chosenId, lead } = top;
  const verbs = useVerbs(archiveThread);
  const view = useViewResult(spec, query, verbs, useLocalSources(verbs));
  const placePending = verbs.newWork.pending;

  const flat = useMemo(() => view.groups.flatMap((group) => group.items), [view.groups]);
  const chosenAt = chosenId ? flat.findIndex((item) => item.id === chosenId) : -1;
  const leadAt = lead?.query === query ? flat.findIndex((item) => item.id === lead.id) : -1;
  // Pinned once per view and query. A lead whose row is gone for a moment
  // (the results on their way back from a drill-in) keeps its claim: the
  // first row stands in until it returns.
  if (chosenAt < 0 && lead?.query !== query && flat.length > 0 && !view.rowsPending && !view.serverMayLead && (frames.length > 1 || query.trim() !== '')) {
    dispatch({ type: 'lead', id: flat[0].id });
  }
  const index = chosenAt >= 0 ? chosenAt : Math.max(leadAt, 0);
  const leadingId = chosenAt >= 0 ? chosenId : leadAt >= 0 ? flat[leadAt].id : null;
  const active: PaletteItem | undefined = flat[index];
  const actionsOf = actionsOfId ? (flat.find((item) => item.id === actionsOfId) ?? null) : null;
  // The row's action editing it in the field, as it was picked: it outlives
  // the row, should a refetch drop it from the list meanwhile.
  const editing = edit ? { ...edit.action, text: edit.text } : null;
  // Tab narrows to the selected row's workspace or thread, where that is somewhere new.
  const tabPlace = !actionsOf && canEnter(frames, active?.place) ? active.place : undefined;
  // At a scope's own results, Backspace in the empty field, Shift+Tab and Escape leave it.
  const atScope = frames.length > 1 && !drilled && !actionsOf;

  // A picked row or action runs only if it is still there: rows that arrive
  // with the place may replace it, and another is not what was asked for.
  const target = (wait: Wait): PaletteAction | undefined => {
    if (wait.kind === 'ask') return view.ask?.actions[0];
    if (wait.kind === 'action') return flat.find((item) => item.id === wait.of)?.actions.find((action) => action.id === wait.action);
    return (wait.id ? flat.find((item) => item.id === wait.id) : flat[0])?.actions[0];
  };
  const perform = (action: PaletteAction) => performAction(action, dispatch, onClose);
  // Enter on the first row waits for rows that may still arrive above it (the
  // chart a typed ticker most likely means, a thread it names), then an action
  // that starts a thread waits out the place's lookup. Then it runs on this
  // render's rows, unless the palette closed meanwhile (it stays mounted for
  // its exit). Every action that runs closes the palette, so a wait fires at
  // most once an open. A wait on a server that may only add rows above Ask
  // lasts SERVER_LEAD_WAIT_MS at most, then takes the row that leads.
  const mustWait = (wait: Wait) => {
    const action = target(wait);
    const rowsComing = view.rowsPending || (view.serverMayLead && !(waitedOut && wait === waiting));
    return (wait.kind === 'row' && !wait.id && rowsComing) || (placePending && action?.effect === 'run' && !!action.startsThread);
  };
  const runWhenReady = (wait: Wait) => {
    const action = target(wait);
    if (mustWait(wait)) dispatch({ type: 'wait', wait });
    else if (action) perform(action);
  };
  const firedRef = useRef(false);
  useEffect(() => {
    if (open) firedRef.current = false;
  }, [open]);
  useEffect(() => {
    if (!waiting || waitedOut) return;
    const timer = window.setTimeout(() => dispatch({ type: 'waitedOut', wait: waiting }), SERVER_LEAD_WAIT_MS);
    return () => window.clearTimeout(timer);
  }, [waiting, waitedOut, dispatch]);
  const ready = !!waiting && !mustWait(waiting);
  const readyTarget = waiting ? target(waiting) : undefined;
  if (ready && !readyTarget) dispatch({ type: 'wait', wait: null });
  useEffect(() => {
    if (!open || !ready || !readyTarget || firedRef.current) return;
    // A view opened leaves the palette open, and the wait cleared with it.
    firedRef.current = readyTarget.effect === 'run';
    performAction(readyTarget, dispatch, onClose);
  }, [open, ready, readyTarget, dispatch, onClose]);

  const back = () => {
    if (actionsOf) dispatch({ type: 'actions', of: null });
    else dispatch({ type: 'back' });
  };

  // The selected row stays in view as the keys move it.
  const shownIndex = actionsOf ? actionIndex : index;
  useEffect(() => {
    listRef.current
      ?.querySelector<HTMLElement>(`[data-row="${shownIndex}"]`)
      ?.scrollIntoView?.({ block: 'nearest' });
  }, [shownIndex, actionsOf]);

  const move = (step: number) => {
    if (actionsOf) {
      const count = actionsOf.actions.length;
      dispatch({ type: 'actionIndex', index: (actionIndex + step + count) % count });
    } else if (flat.length > 0) {
      dispatch({ type: 'choose', id: flat[(index + step + flat.length) % flat.length].id });
    }
  };

  const openActions = (item: PaletteItem | undefined) => {
    if (!item || item.actions.length < 2) return false;
    dispatch({ type: 'actions', of: item.id });
    return true;
  };

  // In an edit the field is a text field: Enter saves a value that changed
  // (an empty one saves nothing), and Esc, Shift+Tab and Backspace in the
  // field already empty go back to the row's actions. A held Backspace's
  // repeats only clear the field: going back on one would hand the rest to
  // the search under it. The arrows move the caret.
  const onEditKeyDown = (e: KeyboardEvent<HTMLInputElement>, { value, text, save }: NonNullable<typeof editing>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      const next = text.trim();
      if (!next) return;
      onClose();
      if (next !== value.trim()) save(next);
    } else if (e.key === 'Tab') {
      e.preventDefault();
      e.stopPropagation();
      if (e.shiftKey) dispatch({ type: 'endEdit' });
    } else if (e.key === 'Escape' || (e.key === 'Backspace' && !e.repeat && text === '')) {
      e.preventDefault();
      dispatch({ type: 'endEdit' });
    }
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    // Keys that commit an IME composition edit the query; they pick nothing.
    if (isImeComposing(e)) return;
    if (editing) {
      onEditKeyDown(e, editing);
      return;
    }
    const field = e.currentTarget;
    const caretAtEnd = field.selectionStart === field.value.length;
    const caretAtStart = field.selectionStart === 0 && field.selectionEnd === 0;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      move(1);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      move(-1);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const action = actionsOf?.actions[actionIndex];
      if (e.metaKey || e.ctrlKey) runWhenReady({ kind: 'ask' });
      else if (!actionsOf) runWhenReady({ kind: 'row', id: leadingId });
      else if (action) runWhenReady({ kind: 'action', of: actionsOf.id, action: action.id });
    } else if (e.key === 'Escape') {
      // The dialog leaves Escape in the field to it: back a step, or close.
      e.preventDefault();
      if (actionsOf || frames.length > 1) back();
      else onClose();
    } else if (e.key === 'Tab') {
      // Focus stays in the field whatever Tab does: the dialog's own trap
      // would hand it to whatever else in the dialog takes focus.
      e.preventDefault();
      e.stopPropagation();
      if (e.shiftKey) {
        if (atScope) dispatch({ type: 'leave' });
      } else if (tabPlace) dispatch({ type: 'enter', place: tabPlace });
    } else if (e.key === 'Backspace' && atScope && field.value === '') {
      e.preventDefault();
      dispatch({ type: 'leave' });
    } else if (e.key === 'ArrowRight' && caretAtEnd && !actionsOf) {
      // A row that opens a view (Show all) opens it on → as on Enter.
      const first = active?.actions[0];
      if (first?.effect === 'open') {
        e.preventDefault();
        perform(first);
      } else if (openActions(active)) e.preventDefault();
    } else if (e.key === 'ArrowLeft' && (actionsOf || (drilled && caretAtStart))) {
      e.preventDefault();
      back();
    }
  };

  const optionId = (i: number) => `${listId}-${i}`;
  const describedId = `${listId}-keys`;
  // A paged view asks for its next page as the list nears its end.
  const { loadMore } = view;
  const onScroll = loadMore
    ? () => {
        const list = listRef.current;
        if (list && list.scrollHeight - list.scrollTop - list.clientHeight < 160) loadMore();
      }
    : undefined;
  // An edit names only what it does: the field holds its value, not a search.
  const chips = editing ? [editing.label] : scopeChips(scopeOf(frames), t);
  const rowName = actionsOf ? (actionsOf.symbol ?? actionsOf.title) : null;
  const trailing = editing ? null : (rowName ?? (drilled && spec.lens ? lensLabel(spec.lens, t) : null));
  const paneTarget = (actionsOf ?? active)?.preview ?? view.preview?.fallback;

  return (
    <>
      <PaletteField
        query={editing ? editing.text : query}
        onText={(text) => dispatch(editing ? { type: 'editText', text } : { type: 'type', text })}
        onKeyDown={onKeyDown}
        placeholder={editing ? editing.placeholder : placeholder(spec, t)}
        label={editing ? t('palette.editLabel', { action: editing.label, name: editing.value }) : fieldLabel(chips, t)}
        selected={!!editing}
        maxLength={editing?.maxLength}
        chips={chips}
        onLeave={() => dispatch(editing ? { type: 'endEdit' } : { type: 'leave' })}
        trailing={trailing}
        busy={view.rowsPending || view.serverMayLead || view.pending || (!!waiting && placePending)}
        combobox={{
          'aria-describedby': describedId,
          'aria-controls': listId,
          'aria-activedescendant': (actionsOf ? actionsOf.actions.length : flat.length) > 0 ? optionId(shownIndex) : undefined,
        }}
      />

      <div className={cn('command-palette-main', view.preview && !isMobile && 'is-paned')}>
        <div
          ref={listRef}
          id={listId}
          role="listbox"
          aria-label={actionsOf ? t('palette.actionsFor', { name: rowName }) : (trailing ?? chips.at(-1) ?? t('palette.title'))}
          className="command-palette-list"
          onScroll={onScroll}
        >
          {!actionsOf && view.note && <p className="command-palette-note">{view.note}</p>}
          {actionsOf ? (
            actionsOf.actions.map((action, i) => (
              <PaletteRow
                key={action.id}
                id={optionId(i)}
                index={i}
                active={i === actionIndex}
                icon={action.icon}
                accent={actionsOf.accent && i === 0}
                title={action.label}
                // The pointer passing over the actions leaves an edit be.
                onHover={() => !editing && dispatch({ type: 'actionIndex', index: i })}
                onPick={() => {
                  dispatch({ type: 'actionIndex', index: i });
                  runWhenReady({ kind: 'action', of: actionsOf.id, action: action.id });
                }}
              />
            ))
          ) : (
            <ResultGroups
              groups={view.groups}
              flat={flat}
              index={index}
              optionId={optionId}
              onOpen={(lens) => dispatch({ type: 'open', lens })}
              onHover={(item) => dispatch({ type: 'choose', id: item.id })}
              onPick={(item) => {
                dispatch({ type: 'choose', id: item.id });
                runWhenReady({ kind: 'row', id: item.id });
              }}
              onMore={(item) => {
                dispatch({ type: 'choose', id: item.id });
                openActions(item);
              }}
            />
          )}
        </div>
        {view.preview && !isMobile && <PreviewPane target={paneTarget} words={view.words} />}
      </div>
      {frames.length === 1 && !actionsOf && query === '' && <PrefixHint />}

      <PaletteFooter
        describedId={describedId}
        keys={{
          enter: editing ? (editing.text.trim() ? t('common.save') : undefined) : actionsOf ? t('palette.do.run') : active?.actions[0].label,
          move: !editing,
          actions: !actionsOf && !!active && active.actions.length > 1,
          ask: !!view.ask && !actionsOf,
          back: !editing && (!!actionsOf || drilled),
          scope: !!tabPlace,
          leave: atScope ? (chips.at(-1) ?? null) : null,
          edit: !!editing,
        }}
      />
    </>
  );
}
