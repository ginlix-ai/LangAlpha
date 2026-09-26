import React, { useLayoutEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from 'framer-motion';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useScrollMemory } from '@/lib/scrollMemory';
import type { Automation } from '@/types/automation';
import type { OrderedGroup } from '../hooks/useOrderedGroups';
import { useScrollReveal } from '../hooks/useScrollReveal';
import type { WatchedReading } from '../hooks/useWatchedReadings';
import { PANE_CROSSFADE } from '../utils/motion';
import AutomationInlineForm, { type AutomationInlineFormProps } from './AutomationInlineForm';
import AutomationInspector from './AutomationInspector';
import { AutomationList } from './AutomationList';
import './ManageView.css';

/** The open form: the props it mounts with, and the key that remounts it
 *  for another automation or a fresh start. */
export interface FormHost {
  key: string;
  props: AutomationInlineFormProps;
}

/**
 * What the list marks and the pane shows. A choice the list does not hold
 * (deleted, or past the first page it loads) is `missing`, and the pane says
 * so rather than show another automation in its place. With no choice a row
 * stands in, the first one the pane opened on for as long as the list holds
 * it, so the pane is never an empty frame; only a choice,
 * `chosen` or `missing`, opens the phone sheet. `runId` is the run a link
 * opened, which only a choice carries.
 */
export type ManageSelection =
  | { kind: 'chosen'; automation: Automation; runId: string | null }
  | { kind: 'first'; automation: Automation }
  | { kind: 'missing'; runId: string | null }
  | { kind: 'none' };

interface ManageViewProps {
  groups: OrderedGroup[];
  readings: Map<string, WatchedReading>;
  selection: ManageSelection;
  onSelect: (id: string | null) => void;
  onOpenRun: (automationId: string, runId: string | null) => void;
  form: FormHost | null;
  onEdit: (a: Automation) => void;
  onDelete: (a: Automation) => void;
}

/**
 * The list and the thing it lists, side by side: the whole set on the left,
 * one automation (or the form that makes or changes one) on the right. On a
 * phone the list is the page and the automation opens as a sheet over it.
 */
export default function ManageView({
  groups,
  readings,
  selection,
  onSelect,
  onOpenRun,
  form,
  onEdit,
  onDelete,
}: ManageViewProps) {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const revealListScroll = useScrollReveal();
  const revealPaneScroll = useScrollReveal();
  const shown = selection.kind === 'chosen' || selection.kind === 'first' ? selection.automation : null;
  const missing = selection.kind === 'missing';

  const formPane = form && (
    <div className="automation-form-pane">
      <h2 className="title-font automation-form-title">
        {t(form.props.original ? 'automation.editAutomation' : 'automation.newAutomation')}
      </h2>
      <AnimatePresence initial={false} mode="wait">
        <AutomationInlineForm key={form.key} {...form.props} />
      </AnimatePresence>
    </div>
  );

  const inspector = shown ? (
    <AutomationInspector
      key={shown.automation_id}
      automation={shown}
      reading={readings.get(shown.automation_id)}
      runId={selection.kind === 'chosen' ? selection.runId : null}
      onOpenRun={(run) => onOpenRun(shown.automation_id, run)}
      onEdit={onEdit}
      onDelete={onDelete}
    />
  ) : selection.kind === 'missing' ? (
    <p className="automations-quiet-note">{t(selection.runId ? 'automation.runLinkNotFound' : 'automation.notFound')}</p>
  ) : null;

  // What the pane holds. On a phone the list's scroller is the pane and only
  // a form opens there, above the list; a row opens a sheet instead.
  const paneKey = form ? `form:${form.key}` : isMobile ? undefined : shown?.automation_id ?? (missing ? 'missing' : undefined);

  // Leaving the page and coming back finds both columns where they were. The
  // keys name the layout, so the phone's single scroller and the two desktop
  // ones never restore each other's depth, and the pane's names what it
  // held, so a link to another automation does not land at this one's depth.
  const listRef = useRef<HTMLDivElement>(null);
  const paneRef = useRef<HTMLDivElement>(null);
  const layout = isMobile ? 'mobile' : 'desktop';
  useScrollMemory(listRef, `page:automations:manage:${layout}:list`);
  useScrollMemory(paneRef, `page:automations:manage:${layout}:pane:${paneKey ?? 'list'}`);

  // Within a visit, a different automation (or a form) opens at its top, not
  // at the depth it was last read to. The phone's list, back from a form, is
  // the memory's to place, and so is the first render.
  const placedKey = useRef(paneKey);
  useLayoutEffect(() => {
    if (placedKey.current === paneKey) return;
    placedKey.current = paneKey;
    if (paneKey !== undefined && paneRef.current) paneRef.current.scrollTop = 0;
  }, [paneKey]);

  if (isMobile) {
    return (
      <div ref={paneRef} className="automations-manage-mobile">
        {formPane}
        <AutomationList
          groups={groups}
          readings={readings}
          selectedId={selection.kind === 'chosen' ? selection.automation.automation_id : null}
          onSelect={onSelect}
        />
        <MobileBottomSheet open={!form && (selection.kind === 'chosen' || missing)} onClose={() => onSelect(null)} height="88vh">
          <div className="px-1 pb-6">{inspector}</div>
        </MobileBottomSheet>
      </div>
    );
  }

  return (
    <div className="automations-manage">
      <motion.div
        ref={listRef}
        layoutScroll
        className="automations-manage-list automations-scroller"
        onScroll={revealListScroll}
      >
        <AutomationList
          groups={groups}
          readings={readings}
          selectedId={form ? null : shown?.automation_id ?? null}
          onSelect={onSelect}
          keyboardNav
        />
      </motion.div>
      <div ref={paneRef} className="automations-manage-pane automations-scroller" onScroll={revealPaneScroll}>
        {/* The outgoing pane is lifted out of flow and dissolves over the
            incoming one, so a switch never passes through an empty frame. */}
        <AnimatePresence initial={false} mode="popLayout">
          {formPane ? (
            <motion.div key="form" {...PANE_CROSSFADE}>
              {formPane}
            </motion.div>
          ) : (
            inspector && (
              <motion.div key={shown?.automation_id ?? 'missing'} {...PANE_CROSSFADE}>
                {inspector}
              </motion.div>
            )
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}
