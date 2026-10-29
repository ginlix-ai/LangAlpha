import { useId, useState, type ReactNode } from 'react';
import { Link } from 'react-router';
import { Trans, useTranslation } from 'react-i18next';
import { Copy, Info, Lock, RotateCcw, Trash2 } from 'lucide-react';
import { ConfirmStrip } from '@/components/mcp/McpPrimitives';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import {
  useDeleteSubagent,
  usePatchSubagent,
  useSaveSubagentText,
  type SubagentEdit,
} from '@/hooks/useSubagents';
import {
  formatApiErrorDetail,
  isSubagentVersionConflict,
  subagentFieldErrors,
  type SubagentFieldError,
  type SubagentsResponse,
  type SubagentView,
} from '@/pages/ChatAgent/utils/api';
import { formatBytes, formatList, relativeTime } from '@/lib/format';
import {
  DetailAsideBlock,
  DetailCallout,
  DetailFacts,
  DetailHeader,
  DetailPage,
} from '../DetailPage';
import { MetaLine, OnOffControl, PageButton } from '../pluginsPage/parts';
import { SkillDocument } from '../pluginsPage/SkillDocument';
import {
  AvailableBlock,
  BrokeragesBlock,
  EffortBlock,
  MemoryBlock,
  ModelBlock,
  PreferredBlock,
  ToolsBlock,
  type SubagentModels,
} from './SubagentAside';
import {
  audienceFacts,
  baseBannerText,
  byteLength,
  capitalize,
  fieldsPhrase,
  isCustom,
  isOrphan,
  isResettable,
  promptSectionsAsMarkdown,
  removalCopy,
  resetLabel,
  sourceText,
  untouchedNote,
  textLimits,
  type SubagentText,
} from './subagentText';
import { DescriptionField, InstructionsField } from './TextFields';

/**
 * A subagent's page. A custom one is the user's text, edited here and saved
 * on Save; a base's text is langalpha's and reads only, so the way to own it
 * is a copy. Either way the side column tunes it, one field per control,
 * each written as it changes.
 */
export function SubagentDetail({
  view,
  data,
  models,
  onClose,
  onDuplicate,
  onOpenPlugin,
}: {
  view: SubagentView;
  data: SubagentsResponse;
  models: SubagentModels;
  onClose: () => void;
  onDuplicate: () => void;
  onOpenPlugin: (name: string) => void;
}) {
  const { t } = useTranslation();
  const labelId = useId();
  const patch = usePatchSubagent();
  const custom = isCustom(view);
  const orphan = isOrphan(view);
  const write = (edit: SubagentEdit) => patch.mutate({ name: view.name, edit });
  const aside = { view, data, models, write };
  const provenance = useProvenance(view);

  // An override left behind by a plugin that is gone keeps its name, but
  // there is no plugin page to open.
  const source =
    view.source === 'plugin' && view.plugin.installed !== false ? (
      // One span: the meta line is a flex row, which would split the text
      // around the link into items and drop the spaces beside it.
      <span>
        <Trans
          i18nKey="plugins.subagents.source.pluginLink"
          values={{ plugin: view.plugin.display_name }}
          components={{
            open: (
              <button
                type="button"
                className="pp-note-link pp-meta-link"
                onClick={() => onOpenPlugin(view.plugin.name)}
              />
            ),
          }}
        />
      </span>
    ) : (
      sourceText(t, view)
    );

  const callouts: ReactNode[] = [];
  if (orphan) {
    callouts.push(
      <DetailCallout key="orphan" title={t('plugins.subagents.orphanTitle')}>
        {t('plugins.subagents.orphanBody')}
      </DetailCallout>,
    );
  }
  const collision = view.problems.find((p) => p.code === 'name_collision');
  if (collision) {
    callouts.push(
      <DetailCallout key="collision" title={t('plugins.subagents.collisionTitle')}>
        {t('plugins.subagents.collisionBody')}
      </DetailCallout>,
    );
  }

  return (
    <DetailPage
      labelId={labelId}
      onClose={onClose}
      crumb={view.name}
      header={
        <DetailHeader
          name={view.name}
          labelId={labelId}
          kind="subagent"
          meta={<MetaLine parts={[<span key="source">{source}</span>, provenance]} />}
          description={!custom && view.description ? view.description : undefined}
          controls={
            <OnOffControl
              enabled={view.enabled && !orphan}
              name={view.name}
              disabled={orphan}
              onToggle={() => write({ enabled: !view.enabled })}
            />
          }
        />
      }
      aside={
        orphan ? (
          <InfoBlock view={view} />
        ) : (
          <>
            <ModelBlock {...aside} />
            <EffortBlock {...aside} />
            <ToolsBlock {...aside} />
            {(data.brokerages.connected || view.brokerages !== 'off') && <BrokeragesBlock {...aside} />}
            <MemoryBlock view={view} write={write} />
            <PreferredBlock {...aside} />
            {custom && <AvailableBlock view={view} write={write} />}
            <InfoBlock view={view} />
          </>
        )
      }
      footer={<Footer view={view} onDuplicate={onDuplicate} />}
    >
      {callouts}
      {custom ? (
        <TextFields view={view} data={data} />
      ) : (
        <>
          {!orphan && (
            // A note, not a callout: nothing is wrong, so it reads as body text.
            <div className="sa-banner" role="note">
              <Info aria-hidden />
              <p>{baseBannerText(t, view)}</p>
              <button type="button" className="sa-banner-action" onClick={onDuplicate}>
                {t('plugins.subagents.duplicateToEdit')}
              </button>
            </div>
          )}
          <div>
            <div className="sa-field-head">
              <h3>{t('plugins.subagents.instructions')}</h3>
              <span className="sa-readonly">
                <Lock aria-hidden />
                {t('plugins.subagents.readOnly')}
              </span>
            </div>
            <SkillDocument
              path={`${view.name}.md`}
              content={view.instructions}
              preview={promptSectionsAsMarkdown}
              size={byteLength(view.instructions)}
              loading={false}
              failed={false}
            />
          </div>
        </>
      )}
    </DetailPage>
  );
}

/** Who changed it last, and where: the agent edits these files mid-thread,
 *  so its change names the thread it was asked in. A hook rather than a
 *  component: the meta line drops a null part with its separator, and an
 *  element that renders nothing is not null, so it left a stray dot. */
function useProvenance(view: SubagentView): ReactNode {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const when = relativeTime(view.updated_at, locale, now);
  if (!view.updated_by || !when) return null;
  const fields = view.edited ? fieldsPhrase(t, view.user_fields, locale, formatList) : null;
  const named = fields ? capitalize(fields, locale) : null;
  if (view.updated_by === 'agent' && view.updated_thread_id) {
    return (
      <span>
        <Trans
          i18nKey={named ? 'plugins.subagents.provenance.fieldsAgentIn' : 'plugins.subagents.provenance.agentIn'}
          values={{
            fields: named,
            when,
            thread: view.updated_thread_title || t('plugins.subagents.provenance.aThread'),
          }}
          components={{
            thread: <Link to={`/chat/t/${view.updated_thread_id}`} className="pp-note-link pp-meta-link" />,
          }}
        />
      </span>
    );
  }
  if (view.updated_by === 'agent') {
    return named
      ? t('plugins.subagents.provenance.fieldsAgent', { fields: named, when })
      : t('plugins.subagents.provenance.agent', { when });
  }
  return named
    ? t('plugins.subagents.provenance.fieldsUser', { fields: named, when })
    : t('plugins.subagents.provenance.user', { when });
}

function InfoBlock({ view }: { view: SubagentView }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const when = relativeTime(view.updated_at, locale, now);
  const custom = isCustom(view);
  const changed = view.edited
    ? fieldsPhrase(t, view.user_fields, locale, (items) => items.join(t('plugins.shell.summary.sep')))
    : null;
  const updated = !custom
    ? null
    : !when
      ? null
      : view.updated_by === 'agent'
        ? t('plugins.subagents.updatedByAgent', { when })
        : view.updated_by === 'user'
          ? t('plugins.subagents.updatedByYou', { when })
          : when;
  return (
    <DetailAsideBlock title={t('plugins.subagents.info')}>
      <div className="sa-facts">
        <DetailFacts
          items={[
            [t('plugins.detail.sourceLabel'), sourceText(t, view)],
            [
              t('plugins.subagents.availableTo'),
              !custom ? audienceFacts(t, view.available_to) : null,
            ],
            [t('plugins.subagents.file'), view.file ? <span key="file" className="sa-file">{view.file}</span> : null],
            [t('plugins.detail.updated'), updated],
            [t('plugins.subagents.changed'), changed ? capitalize(changed, locale) : null],
          ]}
        />
      </div>
    </DetailAsideBlock>
  );
}

/**
 * The page's last block. A custom subagent copies or goes; a base the user
 * changed resets, which drops only their row, never the base; an untouched
 * base has nothing to undo and says why it cannot be removed.
 */
function Footer({ view, onDuplicate }: { view: SubagentView; onDuplicate: () => void }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const remove = useDeleteSubagent();
  const [asking, setAsking] = useState(false);
  const custom = isCustom(view);
  const orphan = isOrphan(view);
  const fields = fieldsPhrase(t, view.user_fields, locale, formatList);

  if (asking) {
    return (
      <ConfirmStrip
        {...removalCopy(t, view, fields, remove.isPending)}
        cancelLabel={t('plugins.skills.deleteConfirmNo')}
        pending={remove.isPending}
        onConfirm={() => remove.mutate(view.name, { onSettled: () => setAsking(false) })}
        onCancel={() => setAsking(false)}
      />
    );
  }

  if (custom || orphan) {
    return (
      <div className="pp-verbs">
        {custom && (
          <PageButton icon={<Copy />} onClick={onDuplicate}>
            {t('plugins.subagents.duplicate')}
          </PageButton>
        )}
        <PageButton variant="danger" icon={<Trash2 />} onClick={() => setAsking(true)}>
          {t('plugins.subagents.removeNamed', { name: view.name })}
        </PageButton>
        <p className="pp-note">
          {orphan ? t('plugins.subagents.removeOrphanNote') : t('plugins.subagents.removeNote')}
        </p>
      </div>
    );
  }
  if (isResettable(view)) {
    return (
      <div className="pp-verbs">
        <PageButton icon={<RotateCcw />} onClick={() => setAsking(true)}>
          {resetLabel(t, view)}
        </PageButton>
        <p className="pp-note">
          {fields
            ? t('plugins.subagents.resetNote', { fields })
            : t('plugins.subagents.resetNotePlain')}
        </p>
      </div>
    );
  }
  return <p className="pp-note">{untouchedNote(t, view)}</p>;
}

const sameText = (a: SubagentText, b: SubagentText) => a.description === b.description && a.instructions === b.instructions;

/**
 * A custom subagent's own words, saved on Save. The draft remembers the text
 * it started from: while the draft is untouched it follows the saved text,
 * and once both have moved (the agent rewrote the file mid-edit) it keeps the
 * draft and says so, rather than either overwriting the other unseen.
 */
function TextFields({ view, data }: { view: SubagentView; data: SubagentsResponse }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const save = useSaveSubagentText();
  const server: SubagentText = { description: view.description, instructions: view.instructions };
  const [origin, setOrigin] = useState<SubagentText>(server);
  const [draft, setDraft] = useState<SubagentText>(server);
  const [errors, setErrors] = useState<SubagentFieldError[]>([]);
  const [failure, setFailure] = useState<string | null>(null);

  const dirty = !sameText(draft, origin);
  const moved = !sameText(server, origin);
  // Adjusted in render: an untouched draft, or one the server now agrees
  // with, simply takes the saved text.
  if (moved && (!dirty || sameText(server, draft))) {
    setOrigin(server);
    setDraft(server);
  }
  const conflict = moved && dirty && !sameText(server, draft);
  // A refused save whose text moved underneath is the conflict box's to
  // explain: "the page now shows the latest version" would contradict its
  // "your draft is kept". The line stays for a refusal the text survived.
  const shownFailure = conflict ? null : failure;

  const { descUsed, descMax, descOver, instrUsed, instrMax, instrOver } = textLimits(draft, data.limits);
  // The agent routes on the description, so it can't go; the instructions can.
  const blank = !draft.description.trim();
  const matches = view.problems.find((p) => p.code === 'description_matches');
  const errorFor = (field: string) => errors.find((e) => e.field === field)?.message ?? null;

  function submit() {
    setFailure(null);
    const sent = draft;
    save.mutate(
      { name: view.name, edit: { description: sent.description, instructions: sent.instructions } },
      {
        onSuccess: (saved) => {
          const text = { description: saved.description, instructions: saved.instructions };
          setOrigin(text);
          // A field typed into while the save was in flight keeps the newer words.
          setDraft((now) => ({
            description: now.description === sent.description ? text.description : now.description,
            instructions: now.instructions === sent.instructions ? text.instructions : now.instructions,
          }));
          setErrors([]);
        },
        onError: (err) => {
          const fieldErrors = subagentFieldErrors(err);
          setErrors(fieldErrors);
          if (fieldErrors.length > 0) return;
          setFailure(
            isSubagentVersionConflict(err) ? t('plugins.subagents.conflictBody') : formatApiErrorDetail(err),
          );
        },
      },
    );
  }

  const set = (patch: Partial<SubagentText>) => {
    setDraft((prev) => ({ ...prev, ...patch }));
    setFailure(null);
  };

  return (
    <div className="sa-fields">
      <DescriptionField
        value={draft.description}
        onChange={(description) => set({ description })}
        used={descUsed}
        max={descMax}
        over={descOver}
        sameAs={matches ? (matches.detail ?? '') : null}
        error={errorFor('description')}
      />
      <InstructionsField
        value={draft.instructions}
        onChange={(instructions) => set({ instructions })}
        over={instrOver}
        aside={t('plugins.subagents.markdownSize', {
          used: formatBytes(instrUsed, locale),
          max: formatBytes(instrMax, locale),
        })}
        error={errorFor('instructions')}
      />
      {conflict && (
        <div className="sa-conflict" role="status">
          {/* The saved text is the newer write, so its writer is who moved it:
              the agent mid-thread, or the user in another tab or the API. */}
          {view.updated_by === 'agent'
            ? t('plugins.subagents.draftConflict')
            : t('plugins.subagents.draftConflictElsewhere')}
          <div className="sa-conflict-actions">
            <PageButton onClick={submit} disabled={save.isPending || descOver || instrOver || blank}>
              {t('plugins.subagents.saveMine')}
            </PageButton>
            <PageButton
              onClick={() => {
                setOrigin(server);
                setDraft(server);
                setErrors([]);
              }}
            >
              {t('plugins.subagents.loadSaved')}
            </PageButton>
          </div>
        </div>
      )}
      <div className="sa-savebar">
        <PageButton
          variant="primary"
          onClick={submit}
          disabled={!dirty || conflict || save.isPending || descOver || instrOver || blank}
        >
          {save.isPending ? t('common.saving') : t('common.save')}
        </PageButton>
        <span className={shownFailure ? 'sa-savebar-note is-warn' : 'sa-savebar-note'}>
          {shownFailure ?? t('plugins.subagents.appliesNextTurn')}
        </span>
      </div>
    </div>
  );
}
