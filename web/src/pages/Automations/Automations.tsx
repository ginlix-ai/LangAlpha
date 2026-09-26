import React, { useCallback, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useHomeTimezone } from '@/hooks/useHomeTimezone';
import { ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { useScrollMemory } from '@/lib/scrollMemory';
import ConfirmDialog from '@/pages/Dashboard/components/ConfirmDialog';
import AutomationsHeader, { type AutomationsView } from './components/AutomationsHeader';
import AutomationInlineForm, { type FormSubmission } from './components/AutomationInlineForm';
import ConfirmDeleteDialog from './components/ConfirmDeleteDialog';
import FeedView from './components/FeedView';
import ManageView, { type FormHost, type ManageSelection } from './components/ManageView';
import Starters from './components/Starters';
import { useAutomations } from './hooks/useAutomations';
import { useOrderedGroups } from './hooks/useOrderedGroups';
import { useAutomationMutations } from './hooks/useAutomationMutations';
import { useWatchedReadings } from './hooks/useWatchedReadings';
import { automationToFormState } from './utils/form';
import { type TemplateId, applyTemplate } from './utils/templates';
import type { Automation } from '@/types/automation';
import './Automations.css';

const VIEW_STORAGE_KEY = 'automations:view';

function readStoredView(): AutomationsView | null {
  try {
    const v = localStorage.getItem(VIEW_STORAGE_KEY);
    return v === 'feed' || v === 'manage' ? v : null;
  } catch {
    return null;
  }
}

function storeView(view: AutomationsView): void {
  try {
    localStorage.setItem(VIEW_STORAGE_KEY, view);
  } catch {
    // Private mode or blocked storage: the view just isn't remembered.
  }
}

type FormMode = { kind: 'create'; template: TemplateId; nonce: number } | { kind: 'edit'; automationId: string };

/** The feed's scroller, which comes back where it was left. Its own
 *  component so it mounts with the feed, after the loading state, and the
 *  memory attaches to the element that actually scrolls. */
function RememberedScroll({ memoryKey, children }: { memoryKey: string; children: React.ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  useScrollMemory(ref, memoryKey);
  return (
    <div ref={ref} className="automations-scroll">
      {children}
    </div>
  );
}

/**
 * /automations: the same set of automations read two ways. The feed is what
 * they found, newest first, beside what is coming and what needs a hand; the
 * manage view is the list itself with one automation open beside it.
 *
 * The view, the open automation and the run its report shows live in the URL
 * (`?view=`, `?id=`, `?run=`), so a link to one automation, or to one of its
 * runs, is a link somebody can send, and the dashboard's `?id=` deep link
 * lands on it. Without a `view` the last one used wins, which is a
 * per-browser convenience and so lives in local storage.
 */
export default function Automations() {
  const { t } = useTranslation();
  const { automations, loading, error } = useAutomations();
  // The page awaits these to close the form or the dialog. `mutateAsync`
  // keeps one identity across renders where its mutation object does not.
  const {
    create: { mutateAsync: createAutomation },
    update: { mutateAsync: updateAutomation },
    remove: { mutateAsync: removeAutomation },
    busy,
  } = useAutomationMutations();
  const homeZone = useHomeTimezone();
  const readings = useWatchedReadings(automations);
  const [searchParams, setSearchParams] = useSearchParams();

  const [storedView, setStoredView] = useState(readStoredView);
  const selectedId = searchParams.get('id');
  const runId = searchParams.get('run');
  const urlView = searchParams.get('view');
  const view: AutomationsView =
    urlView === 'feed' || urlView === 'manage' ? urlView : selectedId || runId ? 'manage' : storedView ?? 'feed';

  const [form, setForm] = useState<FormMode | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Automation | null>(null);
  // An open form with changes in it is not closed by a move elsewhere on the
  // page without asking; an untouched one closes quietly.
  const [draftDirty, setDraftDirty] = useState(false);
  const [pendingLeave, setPendingLeave] = useState<(() => void) | null>(null);
  const leaveForm = useCallback(
    (proceed: () => void) => {
      if (form && draftDirty) setPendingLeave(() => proceed);
      else proceed();
    },
    [form, draftDirty],
  );

  // Crossing views is a navigation the back button should undo; moving
  // within one is not. A run is only ever open within its automation.
  const navigateTo = useCallback(
    (next: AutomationsView, id: string | null, run: string | null = null) => {
      const params = new URLSearchParams(searchParams);
      params.set('view', next);
      if (id) params.set('id', id);
      else params.delete('id');
      if (id && run) params.set('run', run);
      else params.delete('run');
      storeView(next);
      setStoredView(next);
      setSearchParams(params, { replace: next === view });
    },
    [searchParams, setSearchParams, view],
  );

  const openAutomation = useCallback(
    (id: string | null) =>
      leaveForm(() => {
        setForm(null);
        navigateTo('manage', id);
      }),
    [leaveForm, navigateTo],
  );

  const openRun = useCallback(
    (id: string, run: string | null) =>
      leaveForm(() => {
        setForm(null);
        navigateTo('manage', id, run);
      }),
    [leaveForm, navigateTo],
  );

  const changeView = useCallback(
    (next: AutomationsView) =>
      leaveForm(() => {
        setForm(null);
        navigateTo(next, next === 'manage' ? selectedId : null);
      }),
    [leaveForm, navigateTo, selectedId],
  );

  const startCreate = useCallback(
    (template: TemplateId) =>
      leaveForm(() => {
        setForm({ kind: 'create', template, nonce: Date.now() });
        if (view !== 'manage') navigateTo('manage', selectedId);
      }),
    [leaveForm, navigateTo, selectedId, view],
  );

  const byId = useMemo(() => new Map(automations.map((a) => [a.automation_id, a])), [automations]);
  const groups = useOrderedGroups(automations);
  const selection = useMemo((): ManageSelection => {
    // A link to an automation the list does not hold (deleted, or past the
    // page it loads) must not quietly show a different one in its place. A
    // link naming only a run finds its automation by that newest run.
    if (selectedId || runId) {
      const id = selectedId ?? automations.find((a) => a.last_execution?.automation_execution_id === runId)?.automation_id;
      const chosen = id ? byId.get(id) : undefined;
      return chosen ? { kind: 'chosen', automation: chosen, runId } : { kind: 'missing', runId };
    }
    const first = groups[0]?.items[0];
    return first ? { kind: 'first', automation: first } : { kind: 'none' };
  }, [selectedId, runId, automations, byId, groups]);

  const editing = form?.kind === 'edit' ? byId.get(form.automationId) ?? null : null;

  const handleSubmit = useCallback(
    async (submission: FormSubmission) => {
      try {
        if (submission.kind === 'edit' && form?.kind === 'edit') {
          await updateAutomation({ id: form.automationId, data: submission.payload });
          setForm(null);
        } else if (submission.kind === 'create') {
          const created = await createAutomation(submission.payload);
          setForm(null);
          if (created?.automation_id) navigateTo('manage', created.automation_id);
        }
      } catch {
        // The mutation hook already told the user; the form stays open with
        // what they typed.
      }
    },
    [form, createAutomation, updateAutomation, navigateTo],
  );

  const handleConfirmDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await removeAutomation(deleteTarget.automation_id);
      if (deleteTarget.automation_id === selectedId) navigateTo('manage', null);
      setDeleteTarget(null);
    } catch {
      // Reported by the mutation hook.
    }
  }, [deleteTarget, removeAutomation, navigateTo, selectedId]);

  const formHost: FormHost | null = useMemo(() => {
    if (!form) return null;
    const handlers = { onSubmit: handleSubmit, onCancel: () => setForm(null), onDirtyChange: setDraftDirty, loading: busy };
    if (form.kind === 'edit') {
      if (!editing) return null;
      return {
        key: `edit:${editing.automation_id}`,
        props: { ...handlers, initialValues: automationToFormState(editing, homeZone), original: editing },
      };
    }
    return {
      key: `create:${form.template}:${form.nonce}`,
      props: { ...handlers, initialValues: applyTemplate(form.template, homeZone), original: null },
    };
  }, [form, editing, handleSubmit, busy, homeZone]);

  let body: React.ReactNode;
  if (error && automations.length === 0) {
    body = (
      <div className="automations-scroll">
        <div className="automations-frame">
          <ListError>{t('automation.loadFailed')}</ListError>
        </div>
      </div>
    );
  } else if (loading) {
    body = (
      <div className="automations-scroll">
        <div className="automations-frame">
          <ListSkeleton rows={5} />
        </div>
      </div>
    );
  } else if (automations.length === 0) {
    body = (
      <div className="automations-scroll">
        <div className="automations-frame">
          {formHost ? (
            <div className="automation-form-pane automations-zero-form">
              <h2 className="title-font automation-form-title">{t('automation.newAutomation')}</h2>
              <AutomationInlineForm key={formHost.key} {...formHost.props} />
            </div>
          ) : (
            <Starters onPick={startCreate} />
          )}
        </div>
      </div>
    );
  } else if (view === 'feed') {
    body = (
      <RememberedScroll memoryKey="page:automations:feed">
        <div className="automations-frame">
          <FeedView
            automations={automations}
            readings={readings}
            onOpenAutomation={openAutomation}
            onOpenRun={openRun}
            onManage={() => changeView('manage')}
            onNew={startCreate}
          />
        </div>
      </RememberedScroll>
    );
  } else {
    body = (
      <div className="automations-frame automations-frame-fill">
        <ManageView
          groups={groups}
          readings={readings}
          selection={selection}
          onSelect={openAutomation}
          onOpenRun={openRun}
          form={formHost}
          onEdit={(a) => setForm({ kind: 'edit', automationId: a.automation_id })}
          onDelete={setDeleteTarget}
        />
      </div>
    );
  }

  return (
    <div className="automations-page">
      {/* Doubles as the window titlebar in the desktop shell; inert elsewhere. */}
      <div className="chrome-drag-strip" aria-hidden="true" />
      <div className="automations-frame">
        <AutomationsHeader automations={automations} view={view} onViewChange={changeView} onNew={startCreate} />
      </div>
      <div className="automations-body">{body}</div>

      <ConfirmDeleteDialog
        open={!!deleteTarget}
        onOpenChange={(open: boolean) => !open && setDeleteTarget(null)}
        onConfirm={handleConfirmDelete}
        automationName={deleteTarget?.name}
        loading={busy}
      />

      <ConfirmDialog
        open={!!pendingLeave}
        title={t('automation.discardDraftTitle')}
        message={t('automation.discardDraftMessage')}
        confirmLabel={t('automation.discardDraft')}
        onConfirm={() => {
          const proceed = pendingLeave;
          setPendingLeave(null);
          setDraftDirty(false);
          proceed?.();
        }}
        onOpenChange={(open) => {
          if (!open) setPendingLeave(null);
        }}
      />
    </div>
  );
}
