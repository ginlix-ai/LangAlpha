import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type MutableRefObject, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight, Search } from 'lucide-react';
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';
import { Loader } from '@/components/ui/loader';
import { useLocale } from '@/hooks/useLocale';
import { clientOs } from '@/lib/clientOs';
import { grouped2, signedFixed2 } from '@/lib/format';
import { isImeComposing } from '@/lib/ime';
import { focusWithoutRing, lastInputWasPointer } from '@/lib/inputModality';
import { quoteKey, snapshotToStockPrice, useQuotes } from '@/lib/quotes';
import { cn } from '@/lib/utils';
import type { PaletteAction, PaletteItem } from './types';
import { usePaletteGroups } from './usePaletteGroups';
import './CommandPalette.css';

interface CommandPaletteProps {
  open: boolean;
  onClose: () => void;
  /** What was typed after ⌘K before the field had focus. */
  typed: string;
}

/**
 * The ⌘K command palette: one field that finds threads, tickers, workspaces,
 * skills, pages, settings and actions, and hands what was typed to the agent.
 * Pinned near the top so it grows downward without the field moving.
 */
export default function CommandPalette({ open, onClose, typed }: CommandPaletteProps) {
  const { t } = useTranslation();
  // Where focus was when the palette opened. Radix hands focus back only to a
  // Dialog.Trigger, which the palette has none of, so it hands it back itself.
  const returnRef = useRef<HTMLElement | null>(null);
  // The body answers Escape first: inside a row's actions it steps back.
  const escapeRef = useRef<(() => boolean) | null>(null);

  return (
    <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
      <DialogContent
        variant="command"
        className="command-palette"
        onOpenAutoFocus={(e) => {
          returnRef.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
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
          if (isImeComposing(e) || escapeRef.current?.()) e.preventDefault();
        }}
      >
        <DialogTitle className="sr-only">{t('palette.title')}</DialogTitle>
        <PaletteBody
          open={open}
          onClose={onClose}
          typed={typed}
          escapeRef={escapeRef}
        />
      </DialogContent>
    </Dialog>
  );
}

interface BodyProps {
  open: boolean;
  onClose: () => void;
  typed: string;
  escapeRef: MutableRefObject<(() => boolean) | null>;
}

/**
 * What a key or click asked to run, by what it picked, so a key or pointer
 * moving the selection while it waits runs nothing else: a row (none for the
 * row that leads once the rows settle), Ask (⌘↵), or one of a row's actions.
 */
type Wait = { kind: 'row'; id: string | null } | { kind: 'ask' } | { kind: 'action'; of: string; label: string };

/** Mounted with the dialog's content, so every open starts from an empty field. */
function PaletteBody({ open, onClose, typed, escapeRef }: BodyProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const listId = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  // Keys typed before the field had focus are its first text, and any that
  // arrive after it mounted but before it took focus join them.
  const [query, setQuery] = useState(typed);
  const [seenTyped, setSeenTyped] = useState(typed);
  if (typed !== seenTyped) {
    setSeenTyped(typed);
    if (typed.startsWith(seenTyped)) setQuery((q) => q + typed.slice(seenTyped.length));
  }
  // The row the reader moved to, by id, so rows arriving above it never move
  // the selection out from under them. Until they move, the first row leads.
  const [chosenId, setChosenId] = useState<string | null>(null);
  // The row whose actions are open (→), or null for the results.
  // By id, so its actions are this render's: one that starts a thread names
  // the place as now known, not as it was when → was pressed.
  const [actionsOfId, setActionsOfId] = useState<string | null>(null);
  const [actionIndex, setActionIndex] = useState(0);
  // What Enter (or ⌘↵) asked for while a typed ticker, or the place new work
  // goes, was still being looked up.
  const [waiting, setWaiting] = useState<Wait | null>(null);
  // Reopened before its close finished, the body was never unmounted: a new
  // open starts afresh all the same.
  const [shownOpen, setShownOpen] = useState(open);
  if (open !== shownOpen) {
    setShownOpen(open);
    if (open) {
      setQuery(typed);
      setSeenTyped(typed);
      setChosenId(null);
      setActionsOfId(null);
      setWaiting(null);
    }
  }

  const { groups, ask, rowsPending, placePending } = usePaletteGroups(query, onClose);
  const flat = useMemo(() => groups.flatMap((group) => group.items), [groups]);
  const chosenAt = chosenId ? flat.findIndex((item) => item.id === chosenId) : -1;
  const index = chosenAt >= 0 ? chosenAt : 0;
  const active: PaletteItem | undefined = flat[index];
  const actionsOf = actionsOfId ? (flat.find((item) => item.id === actionsOfId) ?? null) : null;

  const symbols = useMemo(() => flat.flatMap((item) => (item.symbol ? [item.symbol] : [])), [flat]);
  const { quotes } = useQuotes(symbols, { refetchInterval: false });

  // A picked row or action runs only if it is still there: rows that arrive
  // with the place may replace it, and another is not what was asked for.
  const target = (wait: Wait): PaletteAction | undefined => {
    if (wait.kind === 'ask') return ask?.actions[0];
    if (wait.kind === 'action') {
      return flat.find((item) => item.id === wait.of)?.actions.find((action) => action.label === wait.label);
    }
    return (wait.id ? flat.find((item) => item.id === wait.id) : flat[0])?.actions[0];
  };
  // Enter on the first row waits for rows that may still arrive above it (the
  // chart a typed ticker most likely means, a thread it names), then an action
  // that starts a thread waits out the place's lookup. Then it runs on this
  // render's rows, unless the palette closed meanwhile (it stays mounted for
  // its exit). Every action closes the palette, so a wait fires at most once an open.
  const mustWait = (wait: Wait) =>
    (wait.kind === 'row' && !wait.id && rowsPending) || (placePending && !!target(wait)?.startsThread);
  const runWhenReady = (wait: Wait) => {
    if (mustWait(wait)) setWaiting(wait);
    else target(wait)?.run();
  };
  const firedRef = useRef(false);
  useEffect(() => {
    if (open) firedRef.current = false;
  }, [open]);
  const ready = !!waiting && !mustWait(waiting);
  const readyTarget = waiting ? target(waiting) : undefined;
  if (ready && !readyTarget) setWaiting(null);
  useEffect(() => {
    if (!open || !ready || firedRef.current) return;
    firedRef.current = true;
    readyTarget?.run();
  }, [open, ready, readyTarget]);

  useEffect(() => {
    escapeRef.current = () => {
      if (!actionsOf) return false;
      setActionsOfId(null);
      return true;
    };
  }, [actionsOf, escapeRef]);

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
      setActionIndex((actionIndex + step + count) % count);
      return;
    }
    if (flat.length === 0) return;
    setChosenId(flat[(index + step + flat.length) % flat.length].id);
  };

  const openActions = (item: PaletteItem | undefined) => {
    if (!item || item.actions.length < 2) return false;
    setActionsOfId(item.id);
    setActionIndex(0);
    return true;
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    // Keys that commit an IME composition edit the query; they pick nothing.
    if (isImeComposing(e)) return;
    const caretAtEnd = e.currentTarget.selectionStart === e.currentTarget.value.length;
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
      else if (!actionsOf) runWhenReady({ kind: 'row', id: chosenAt >= 0 ? chosenId : null });
      else if (action) runWhenReady({ kind: 'action', of: actionsOf.id, label: action.label });
    } else if (e.key === 'ArrowRight' && caretAtEnd && !actionsOf) {
      if (openActions(active)) e.preventDefault();
    } else if (e.key === 'ArrowLeft' && actionsOf) {
      e.preventDefault();
      setActionsOfId(null);
    }
  };

  const optionId = (i: number) => `${listId}-${i}`;
  const enterLabel = actionsOf ? t('palette.do.run') : active?.actions[0].label;

  return (
    <>
      {/* The row owns its field's focus: the keyboard ring goes round the
          row, and the always-focused field draws no edge of its own. */}
      <div className="command-palette-field rings-within owns-its-edge">
        <Search className="command-palette-field-icon" aria-hidden />
        <input
          ref={inputRef}
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setChosenId(null);
            setActionsOfId(null);
            setWaiting(null);
          }}
          onKeyDown={onKeyDown}
          placeholder={t('palette.placeholder')}
          aria-label={t('palette.label')}
          role="combobox"
          aria-expanded
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={(actionsOf ? actionsOf.actions.length : flat.length) > 0 ? optionId(shownIndex) : undefined}
          autoComplete="off"
          spellCheck={false}
        />
        {(rowsPending || (waiting && placePending)) && <Loader size={12} className="text-current" />}
        {actionsOf && (
          <span className="command-palette-scope">
            {actionsOf.symbol ?? actionsOf.title}
            <ChevronRight aria-hidden />
          </span>
        )}
      </div>

      <div
        ref={listRef}
        id={listId}
        role="listbox"
        aria-label={actionsOf ? t('palette.actionsFor', { name: actionsOf.symbol ?? actionsOf.title }) : t('palette.title')}
        className="command-palette-list"
      >
        {actionsOf
          ? actionsOf.actions.map((action, i) => (
              <Row
                key={action.label}
                id={optionId(i)}
                index={i}
                active={i === actionIndex}
                icon={action.icon}
                accent={actionsOf.accent && i === 0}
                title={action.label}
                onHover={() => setActionIndex(i)}
                onPick={() => {
                  setActionIndex(i);
                  runWhenReady({ kind: 'action', of: actionsOf.id, label: action.label });
                }}
              />
            ))
          : groups.map((group) => (
              <div key={group.id} role="group" aria-label={group.label}>
                <div className="command-palette-group" aria-hidden>
                  {group.label}
                </div>
                {group.items.map((item) => {
                  const i = flat.indexOf(item);
                  const quote = item.symbol ? quotes[quoteKey(item.symbol)] : undefined;
                  const price = item.symbol && quote ? snapshotToStockPrice(item.symbol, quote) : null;
                  return (
                    <Row
                      key={item.id}
                      id={optionId(i)}
                      index={i}
                      active={i === index}
                      icon={item.icon}
                      accent={item.accent}
                      symbol={item.symbol}
                      title={item.title || (quote?.name ?? '')}
                      spans={item.spans}
                      meta={item.meta}
                      trailing={
                        price?.quoteAvailable ? (
                          <span className="command-palette-quote">
                            <span>{grouped2(price.price, locale)}</span>
                            <span className={price.change > 0 ? 'is-up' : price.change < 0 ? 'is-down' : undefined}>
                              {signedFixed2(price.changePercent, locale)}%
                            </span>
                          </span>
                        ) : null
                      }
                      onHover={() => setChosenId(item.id)}
                      onPick={() => {
                        setChosenId(item.id);
                        runWhenReady({ kind: 'row', id: item.id });
                      }}
                    />
                  );
                })}
              </div>
            ))}
      </div>

      <div className="command-palette-footer" aria-hidden>
        <span><kbd>↑↓</kbd>{t('palette.do.move')}</span>
        {enterLabel && <span><kbd>↵</kbd>{enterLabel}</span>}
        {ask && !actionsOf && <span><kbd>{clientOs() === 'mac' ? '⌘↵' : 'Ctrl ↵'}</kbd>{t('palette.do.ask')}</span>}
        {!actionsOf && active && active.actions.length > 1 && <span><kbd>→</kbd>{t('palette.do.actions')}</span>}
        {actionsOf && <span><kbd>←</kbd>{t('palette.do.back')}</span>}
        <span className="command-palette-footer-end">
          <kbd>esc</kbd>
          {actionsOf ? t('palette.do.back') : t('palette.do.close')}
        </span>
      </div>
    </>
  );
}

interface RowProps {
  id: string;
  index: number;
  active: boolean;
  icon: PaletteItem['icon'];
  accent?: boolean;
  symbol?: string;
  title: string;
  spans?: [number, number][];
  meta?: string;
  trailing?: ReactNode;
  onHover: () => void;
  onPick: () => void;
}

function Row({ id, index, active, icon: Icon, accent, symbol, title, spans, meta, trailing, onHover, onPick }: RowProps) {
  return (
    <div
      id={id}
      data-row={index}
      role="option"
      aria-selected={active}
      className={cn('command-palette-row', active && 'is-active')}
      // Moving, not entering: rows arriving under a still pointer take nothing.
      onMouseMove={active ? undefined : onHover}
      onMouseDown={(e) => e.preventDefault()}
      onClick={onPick}
    >
      <Icon className={cn('command-palette-row-icon', accent && 'is-accent')} aria-hidden />
      {symbol && <span className="command-palette-symbol">{symbol}</span>}
      {title && (
        <span className="command-palette-title">
          <Highlighted text={title} spans={spans} />
        </span>
      )}
      {meta && <span className="command-palette-meta">{meta}</span>}
      {/* No ↵ on the selected row: the footer names what Enter does, and a
          key drawn here would push the row's quote aside as the keys move. */}
      {trailing && <span className="command-palette-trailing">{trailing}</span>}
    </div>
  );
}

function Highlighted({ text, spans }: { text: string; spans?: [number, number][] }) {
  if (!spans?.length) return <>{text}</>;
  const parts: ReactNode[] = [];
  let at = 0;
  spans.forEach(([start, end], i) => {
    if (start > at) parts.push(text.slice(at, start));
    parts.push(<mark key={i}>{text.slice(start, end)}</mark>);
    at = end;
  });
  if (at < text.length) parts.push(text.slice(at));
  return <>{parts}</>;
}
