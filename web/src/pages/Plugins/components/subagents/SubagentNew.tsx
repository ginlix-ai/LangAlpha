import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Plus } from 'lucide-react';
import { useLocale } from '@/hooks/useLocale';
import { useCreateSubagent } from '@/hooks/useSubagents';
import { formatBytes } from '@/lib/format';
import { effortLabelFor } from '@/lib/modelTuning';
import { cn } from '@/lib/utils';
import { BrandMark } from '@/pages/ChatAgent/components/mcp/BrandMark';
import {
  formatApiErrorDetail,
  isSubagentVersionConflict,
  subagentFieldErrors,
  type SubagentAudience,
  type SubagentFieldError,
  type SubagentsResponse,
  type SubagentTool,
  type SubagentView,
} from '@/pages/ChatAgent/utils/api';
import { DetailAsideBlock, DetailFacts, DetailHeader, DetailPage } from '../DetailPage';
import { PageButton } from '../pluginsPage/parts';
import type { SubagentModels } from './SubagentAside';
import {
  AUDIENCES,
  NAME_PATTERN,
  RESERVED_NAMES,
  audienceFacts,
  brokerageText,
  copiedModel,
  countCustom,
  effortOrSetting,
  isCustom,
  isOrphan,
  memoryText,
  modelMeta,
  modelName,
  toolsSummary,
  textLimits,
  type SubagentText,
} from './subagentText';
import { DescriptionField, InstructionsField } from './TextFields';

/**
 * A new custom subagent: Blank, or a copy of one the user already has. A copy
 * takes the source's effective values, the user's own edits to it included,
 * which is what `cp` does in the folder, so the pane and the agent copy the
 * same way. Only the name and the two texts are asked here; the rest is
 * tuned on its page once it exists.
 */

/** Blank's starting point: the server's for a custom file that names no
 *  tools, the Analyst set. */
const BLANK_TOOLS: readonly SubagentTool[] = ['web', 'market_data', 'code', 'shell'];
// The fields the form shows an error under; any other is said by the button.
const PLACED_FIELDS = new Set(['name', 'description', 'instructions', 'start_from']);

export function SubagentNew({
  data,
  models,
  startFrom,
  onClose,
  onCreated,
}: {
  data: SubagentsResponse;
  models: SubagentModels;
  /** The subagent to copy, or null for Blank. */
  startFrom: string | null;
  onClose: () => void;
  onCreated: (name: string) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const labelId = useId();
  const nameId = useId();
  const create = useCreateSubagent();

  // Bases the user can copy, and the one custom subagent being duplicated.
  const sources = data.subagents.filter(
    (s) => !isOrphan(s) && (!isCustom(s) || s.name === startFrom),
  );
  const find = (name: string | null) => (name ? (sources.find((s) => s.name === name) ?? null) : null);
  const seed = (name: string | null): SubagentText => {
    const source = find(name);
    return source ? { description: source.description, instructions: source.instructions } : { description: '', instructions: '' };
  };

  const [from, setFrom] = useState<string | null>(find(startFrom)?.name ?? null);
  const [name, setName] = useState('');
  const [draft, setDraft] = useState<SubagentText>(() => seed(find(startFrom)?.name ?? null));
  const [errors, setErrors] = useState<SubagentFieldError[]>([]);
  const [failure, setFailure] = useState<string | null>(null);
  const source = find(from);

  // A field the user has not touched follows the pick; one they wrote keeps
  // their words.
  function pick(next: string | null) {
    const before = seed(from);
    const after = seed(next);
    setDraft((d) => ({
      description: d.description === before.description ? after.description : d.description,
      instructions: d.instructions === before.instructions ? after.instructions : d.instructions,
    }));
    setFrom(next);
    setErrors((es) => es.filter((e) => e.field !== 'start_from'));
  }

  const trimmed = name.trim();
  const nameError = !trimmed
    ? null
    : !NAME_PATTERN.test(trimmed)
      ? t('plugins.subagents.nameInvalid')
      : RESERVED_NAMES.has(trimmed)
        ? t('plugins.subagents.nameReserved', { name: trimmed })
        : data.subagents.some((s) => s.name === trimmed)
          ? t('plugins.subagents.nameTaken', { name: trimmed })
          : null;
  const serverError = (field: string) => errors.find((e) => e.field === field)?.message ?? null;

  const { descUsed, descMax, descOver, instrUsed, instrMax, instrOver } = textLimits(draft, data.limits);
  const maxCustom = data.limits.max_custom;
  const full = countCustom(data) >= maxCustom;
  const ready =
    !!trimmed &&
    !nameError &&
    !!draft.description.trim() &&
    !descOver &&
    !instrOver &&
    !full &&
    !create.isPending;
  // A copy keeps its source's switch and audience, as `cp` keeps a file's.
  const note =
    failure ??
    (full
      ? t('plugins.subagents.atLimit', { count: maxCustom })
      : source && !source.enabled
        ? t('plugins.subagents.copyStartsOff', { name: source.name })
        : t('plugins.subagents.availableNextTurn'));

  function submit() {
    setFailure(null);
    create.mutate(
      { name: trimmed, startFrom: from, fields: { description: draft.description, instructions: draft.instructions } },
      {
        onSuccess: (view) => onCreated(view.name),
        onError: (err) => {
          // Someone (the agent, another tab) took the name since the list was
          // read; the re-read list then says so under the field as well.
          if (isSubagentVersionConflict(err)) {
            setErrors([{ field: 'name', message: t('plugins.subagents.nameTaken', { name: trimmed }) }]);
            return;
          }
          // A field this form has no place for still gets said, by the button.
          const fieldErrors = subagentFieldErrors(err);
          const unplaced = fieldErrors.filter((e) => !PLACED_FIELDS.has(e.field));
          setErrors(fieldErrors);
          if (fieldErrors.length === 0) setFailure(formatApiErrorDetail(err));
          else if (unplaced.length > 0) setFailure(unplaced.map((e) => e.message).join(' '));
        },
      },
    );
  }

  return (
    <div className={cn('sa-new', !trimmed && 'is-unnamed')}>
      <DetailPage
        labelId={labelId}
        onClose={onClose}
        crumb={t('plugins.subagents.newTitle')}
        header={
          <DetailHeader
            name={trimmed && !nameError ? trimmed : t('plugins.subagents.newTitle')}
            labelId={labelId}
            kind="subagent"
            description={t('plugins.subagents.newLead')}
          />
        }
        lead={
          <section>
            <h3 className="sa-start-title">{t('plugins.subagents.startFrom')}</h3>
            <div className="sa-start" role="radiogroup" aria-label={t('plugins.subagents.startFrom')}>
              <StartCard
                checked={from === null}
                onPick={() => pick(null)}
                mark={
                  <span className="sa-blank-mark" aria-hidden>
                    <Plus />
                  </span>
                }
                name={t('plugins.subagents.blank')}
                line={t('plugins.subagents.blankLine')}
              />
              {sources.map((s) => (
                <StartCard
                  key={s.name}
                  checked={from === s.name}
                  onPick={() => pick(s.name)}
                  mark={<BrandMark name={s.name} kind="subagent" size="md" />}
                  name={s.name}
                  meta={startMeta(t, s, models, locale)}
                  line={
                    s.source === 'plugin'
                      ? t('plugins.subagents.fromPluginLine', { plugin: s.plugin.display_name, description: s.description })
                      : s.description
                  }
                />
              ))}
            </div>
            {/* The source was deleted or its plugin switched off since the list was read. */}
            {serverError('start_from') && <p className="sa-field-error">{serverError('start_from')}</p>}
          </section>
        }
        aside={
          <DetailAsideBlock
            title={source ? t('plugins.subagents.copiedFrom', { name: source.name }) : t('plugins.subagents.startsWith')}
          >
            <CopyFacts source={source} models={models} />
            <p className="sa-hint">
              {source && (source.edited || isCustom(source))
                ? t('plugins.subagents.copyIncludesEdits', { name: source.name })
                : t('plugins.subagents.copyChangeLater')}
            </p>
          </DetailAsideBlock>
        }
      >
        <div className="sa-fields">
          <div>
            <div className="sa-field-head">
              <label htmlFor={nameId}>{t('plugins.subagents.name')}</label>
              <span className="sa-field-aside">{t('plugins.subagents.nameHint')}</span>
            </div>
            <input
              id={nameId}
              className="sa-input is-name"
              value={name}
              placeholder={t('plugins.subagents.namePlaceholder')}
              autoComplete="off"
              spellCheck={false}
              maxLength={48}
              aria-invalid={!!nameError || undefined}
              onChange={(e) => setName(e.target.value)}
            />
            {(nameError ?? serverError('name')) && (
              <p className="sa-field-error">{nameError ?? serverError('name')}</p>
            )}
          </div>
          <DescriptionField
            value={draft.description}
            onChange={(description) => setDraft((d) => ({ ...d, description }))}
            used={descUsed}
            max={descMax}
            over={descOver}
            sameAs={source && draft.description.trim() === source.description.trim() ? source.name : null}
            error={serverError('description')}
            placeholder={t('plugins.subagents.whenToUsePlaceholder')}
          />
          <InstructionsField
            value={draft.instructions}
            onChange={(instructions) => setDraft((d) => ({ ...d, instructions }))}
            over={instrOver}
            aside={
              <>
                {source && draft.instructions === source.instructions
                  ? t('plugins.subagents.copiedMarkdown', { name: source.name })
                  : t('plugins.subagents.markdown')}
                {instrOver &&
                  `${t('plugins.subagents.sep')}${t('plugins.subagents.sizeOf', {
                    used: formatBytes(instrUsed, locale),
                    max: formatBytes(instrMax, locale),
                  })}`}
              </>
            }
            error={serverError('instructions')}
            placeholder={t('plugins.subagents.instructionsPlaceholder')}
          />
          <div className="sa-savebar">
            <PageButton variant="primary" onClick={submit} disabled={!ready}>
              {create.isPending ? t('common.loading') : t('plugins.subagents.create')}
            </PageButton>
            <span className={failure || full ? 'sa-savebar-note is-warn' : 'sa-savebar-note'}>{note}</span>
          </div>
        </div>
      </DetailPage>
    </div>
  );
}

type T = ReturnType<typeof useTranslation>['t'];

/** "Haiku 5.5 · Low · Market data, code, shell": what a pick would bring,
 *  the model only where it pins one. */
function startMeta(t: T, s: SubagentView, models: SubagentModels, locale: string): string {
  const parts = s.model ? modelMeta(t, s, models.metadata) : [];
  if (!s.model) {
    const effort = effortLabelFor(t, s.effort);
    if (effort) parts.push(effort);
  }
  parts.push(toolsSummary(t, s.tools, locale));
  return parts.join(' · ');
}

function StartCard({
  checked,
  onPick,
  mark,
  name,
  meta,
  line,
}: {
  checked: boolean;
  onPick: () => void;
  mark: React.ReactNode;
  name: string;
  meta?: string;
  line: string;
}) {
  return (
    <button type="button" role="radio" aria-checked={checked} className="sa-start-card" onClick={onPick}>
      {mark}
      <span className="sa-start-text">
        <span className="sa-start-l1">
          <span className="sa-start-name">{name}</span>
          {meta && <span className="sa-start-meta">{meta}</span>}
        </span>
        <span className="sa-start-l2">{line}</span>
      </span>
    </button>
  );
}

/** What the copy brings beyond its text. Orders never come along: the copy
 *  gets access at most, and orders are asked for again on its own page. */
function CopyFacts({
  source,
  models,
}: {
  source: SubagentView | null;
  models: SubagentModels;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const pinned = copiedModel(source, models.lockedModels);
  const effort = effortOrSetting(t, source?.effort ?? null, pinned);
  const audiences: readonly SubagentAudience[] = source ? source.available_to : AUDIENCES;
  return (
    <div className="sa-facts">
      <DetailFacts
        items={[
          [t('plugins.subagents.model'), pinned ? modelName(pinned, models.metadata) : t('plugins.subagents.threadsModel')],
          [t('plugins.subagents.effort'), effort],
          [t('plugins.subagents.tools'), toolsSummary(t, source ? source.tools : BLANK_TOOLS, locale)],
          // A blank one gets every connector and connected brokerages, as
          // every subagent of the user's own starts.
          [
            t('plugins.subagents.connectors'),
            !source || source.connectors ? t('plugins.subagents.connectorsAll') : t('plugins.subagents.connectorsNone'),
          ],
          [
            t('plugins.subagents.brokerages'),
            !source || source.brokerages !== 'off' ? brokerageText(t, 'on') : null,
          ],
          // A blank one reads memory, as every subagent of the user's own starts.
          [t('plugins.subagents.memory'), memoryText(t, source ? source.memory : true)],
          [t('plugins.subagents.availableTo'), audienceFacts(t, audiences)],
        ]}
      />
    </div>
  );
}
