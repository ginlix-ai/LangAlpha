import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { motion } from '@/lib/framer';
import { ChevronRight } from 'lucide-react';
import { useHomeTimezone } from '@/hooks/useHomeTimezone';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { Automation } from '@/types/automation';
import type { WatchedReading } from '../hooks/useWatchedReadings';
import { distanceLabel } from '../utils/price';
import { scheduleLine } from '../utils/schedule';
import { automationStatusUi, GROUP_LABEL_KEY, rowTrailing, type AutomationGroup } from '../utils/status';
import { usePrefetchExecutions } from '../hooks/useExecutions';
import type { OrderedGroup } from '../hooks/useOrderedGroups';
import { INSTANT, SELECTION_SPRING } from '../utils/motion';
import { StatusGlyph } from './StatusMark';
import './AutomationList.css';

/** A finished group longer than this starts folded when anything else is on
 *  the list: a pile of one-time runs that already happened should not push
 *  the live ones below the fold. */
const FOLD_FINISHED_OVER = 3;

interface AutomationListProps {
  groups: OrderedGroup[];
  readings: Map<string, WatchedReading>;
  selectedId: string | null;
  onSelect: (id: string) => void;
  /** ↑/↓ step through the list, from a row or with nothing focused. */
  keyboardNav?: boolean;
}

export function AutomationList({ groups, readings, selectedId, onSelect, keyboardNav = false }: AutomationListProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const homeZone = useHomeTimezone();
  const finished = groups.find((g) => g.group === 'finished');
  const [finishedOpen, setFinishedOpen] = useState(
    () => !finished || finished.items.length <= FOLD_FINISHED_OVER || groups.length === 1,
  );
  // Alone on the list the finished group has no fold control, so it has to
  // show whatever the state was when the other groups were still there.
  const showFinished = finishedOpen || groups.length === 1;

  const navRef = useRef<HTMLElement>(null);
  const prefetch = usePrefetchExecutions();
  // How the selection last moved. A step from the keyboard slides the fill to
  // the next row, reading as a cursor; a click just fades it in where the
  // pointer already is, since a slide across the list would only get in the way.
  const [viaKeys, setViaKeys] = useState(false);

  const order = useMemo(
    () => groups.flatMap(({ group, items }) => (group !== 'finished' || showFinished ? items.map((a) => a.automation_id) : [])),
    [groups, showFinished],
  );

  // The rows either side of the selection are where the next step lands, so
  // their runs are fetched ahead and the pane arrives whole.
  useEffect(() => {
    const i = selectedId ? order.indexOf(selectedId) : -1;
    if (i < 0) return;
    for (const id of [order[i - 1], order[i + 1]]) if (id) prefetch(id);
  }, [selectedId, order, prefetch]);

  const step = useCallback(
    (delta: 1 | -1) => {
      if (order.length === 0) return;
      const i = selectedId ? order.indexOf(selectedId) : -1;
      const next = i < 0 ? (delta > 0 ? 0 : order.length - 1) : Math.min(Math.max(i + delta, 0), order.length - 1);
      if (next === i) return;
      const id = order[next];
      setViaKeys(true);
      onSelect(id);
      const row = navRef.current?.querySelector<HTMLElement>(`[data-id="${id}"]`);
      row?.focus({ preventScroll: true });
      row?.scrollIntoView({ block: 'nearest' });
    },
    [order, selectedId, onSelect],
  );

  const onArrow = useCallback(
    (e: KeyboardEvent | React.KeyboardEvent) => {
      if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) return false;
      if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return false;
      e.preventDefault();
      step(e.key === 'ArrowDown' ? 1 : -1);
      return true;
    },
    [step],
  );

  useEffect(() => {
    if (!keyboardNav) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.target === document.body) onArrow(e);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [keyboardNav, onArrow]);

  const tabStop = selectedId && order.includes(selectedId) ? selectedId : order[0];

  const trailing = (a: Automation, group: AutomationGroup): string => {
    const row = rowTrailing(a, group);
    if (!row) return '';
    switch (row.kind) {
      case 'state':
        return t(row.labelKey);
      case 'reading':
        return distanceLabel(readings.get(a.automation_id)?.reading ?? null, t, locale);
      default:
        return relativeTime(row.at, locale, now);
    }
  };

  return (
    <nav
      ref={navRef}
      className="automations-list"
      aria-label={t('automation.automations')}
      data-nav={viaKeys ? 'keys' : undefined}
      onKeyDown={keyboardNav ? onArrow : undefined}
    >
      {groups.map(({ group, items }) => {
        const foldable = group === 'finished' && groups.length > 1;
        const open = group !== 'finished' || showFinished;
        return (
          <div key={group} className="automations-list-group">
            {foldable ? (
              <button
                type="button"
                className="automations-kicker automations-list-heading automations-list-fold"
                aria-expanded={open}
                onClick={() => setFinishedOpen((v) => !v)}
              >
                <ChevronRight className={cn('h-3 w-3 transition-transform', open && 'rotate-90')} />
                {t(GROUP_LABEL_KEY[group])}
                <span className="automations-list-count">{items.length}</span>
              </button>
            ) : (
              <h2 className="automations-kicker automations-list-heading">
                {t(GROUP_LABEL_KEY[group])}
                <span className="automations-list-count">{items.length}</span>
              </h2>
            )}
            {open &&
              items.map((a) => {
                const ui = automationStatusUi(a);
                const selected = a.automation_id === selectedId;
                return (
                  <button
                    key={a.automation_id}
                    type="button"
                    className="automations-list-row"
                    data-id={a.automation_id}
                    aria-current={selected ? 'true' : undefined}
                    tabIndex={keyboardNav && a.automation_id !== tabStop ? -1 : undefined}
                    onClick={() => {
                      setViaKeys(false);
                      onSelect(a.automation_id);
                    }}
                    onPointerEnter={() => prefetch(a.automation_id)}
                  >
                    {selected && (
                      <motion.span
                        layoutId="automations-list-selection"
                        layoutCrossfade={false}
                        className="automations-list-highlight"
                        style={{ borderRadius: 8, opacity: viaKeys ? 1 : 0 }}
                        transition={viaKeys ? SELECTION_SPRING : INSTANT}
                        aria-hidden="true"
                      />
                    )}
                    <span className="automations-list-glyph">
                      <StatusGlyph ui={ui} label={t(ui.labelKey)} size={14} />
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="automations-list-name">{a.name}</span>
                      <span className="automation-mono automations-list-sub">{scheduleLine(a, t, homeZone, locale)}</span>
                    </span>
                    <span className="automation-mono automations-list-trailing">{trailing(a, group)}</span>
                  </button>
                );
              })}
          </div>
        );
      })}
    </nav>
  );
}
