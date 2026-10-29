import { useMemo } from 'react';
import { Link } from 'react-router';
import { Trans, useTranslation } from 'react-i18next';
import { Check, ChevronDown, Plus, X } from 'lucide-react';
import { CheckboxChoice } from '@/components/ui/choice';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { CompactSelect, EffortSelect } from '@/components/model/EffortSelect';
import { ModelSelector } from '@/components/model/ModelSelector';
import type { useAllModels } from '@/hooks/useAllModels';
import { useLocale } from '@/hooks/useLocale';
import { usePreferences } from '@/hooks/usePreferences';
import { useSearchTierAllows } from '@/hooks/useSearchTierAllows';
import { useSkills } from '@/hooks/useSkills';
import { cn } from '@/lib/utils';
import { modelPrefs, modelProfile } from '@/lib/modelPreferences';
import { EFFORT_LABELS, EFFORT_ORDER, effortLabelFor, hasEffortOptions, resolveTuning } from '@/lib/modelTuning';
import type { SubagentEdit } from '@/hooks/useSubagents';
import type {
  SubagentAudience,
  SubagentsResponse,
  SubagentTool,
  SubagentView,
} from '@/pages/ChatAgent/utils/api';
import { DetailAsideBlock } from '../DetailPage';
import { SwitchRow } from '../ToolAccess';
import {
  AUDIENCES,
  PRESETS,
  TOOL_COPY,
  TOOL_ORDER,
  addableSkills,
  audienceChoice,
  baseSide,
  brokerageText,
  effortOrSetting,
  hasProblem,
  memoryText,
  modelName,
  offerBasePin,
  orderedTools,
  preferredSkillOptions,
  sameTools,
  showsDepth,
  toolsSummary,
  type PreferredSkillOption,
} from './subagentText';

/**
 * The side column of a subagent's page. Every control writes its own field
 * and draws what the query cache holds, so a write the server refuses snaps
 * the control back and an edit the agent makes in the file shows on refocus.
 *
 * On a base, each heading carries what the base ships with on its right,
 * which is how an edit reads without a diff view, and a control set back to
 * that value drops the user's key rather than pinning a copy of it.
 */

export type SubagentModels = ReturnType<typeof useAllModels>;

export interface AsideProps {
  view: SubagentView;
  data: SubagentsResponse;
  models: SubagentModels;
  write: (edit: SubagentEdit) => void;
}

export function ModelBlock({ view, models, write }: AsideProps) {
  const { t } = useTranslation();
  const { metadata } = models;
  const unavailable = hasProblem(view, 'model_unavailable');
  const base = view.base;
  const offered = offerBasePin(models.models, models.lockedModels, base?.model ?? null);

  // As a provider change does the depth: a set effort stays if the new model
  // offers it and goes if not, since a save holds it to that model's ladder.
  // Following the thread keeps any level, which the turn steps down.
  function pickModel(next: string) {
    const keep = !next || !view.effort || (metadata[next]?.reasoning_efforts ?? []).includes(view.effort);
    write({ model: next || null, ...(keep ? {} : { effort: null }) });
  }

  return (
    <DetailAsideBlock
      title={t('plugins.subagents.model')}
      side={
        base
          ? baseSide(t, view, base.model ? modelName(base.model, metadata) : t('plugins.subagents.threadsModel'))
          : undefined
      }
    >
      <ModelSelector
        ariaLabel={t('plugins.subagents.modelAria', { name: view.name })}
        nameOf={(m) => modelName(m, metadata)}
        value={view.model ?? ''}
        onChange={pickModel}
        models={offered.models}
        placeholder={t('plugins.subagents.threadsModel')}
        payers={models.payers}
        metadata={metadata}
        disabledModels={offered.locked}
        // The server says whether a saved model still runs; one this picker
        // doesn't list (a key the user can't pick today) may well still run.
        missingLabel={(m) =>
          unavailable
            ? t('plugins.subagents.modelUnavailableOption', { model: modelName(m, metadata) })
            : modelName(m, metadata)
        }
      />
      {unavailable ? (
        <p className="sa-hint is-warn">{t('plugins.subagents.modelUnavailableHint')}</p>
      ) : (
        !base && <p className="sa-hint">{t('plugins.subagents.modelHint')}</p>
      )}
    </DetailAsideBlock>
  );
}

/**
 * The first option names what an untouched control runs: the pinned model's
 * own setting, or the thread's when the model follows the thread. A pinned
 * model offers only its own ladder; following the thread offers every level,
 * and the server steps a level down to what the thread's model supports.
 * A pin that is unavailable runs on the thread's model, so it reads as one
 * that follows the thread rather than naming a model that never runs. Its
 * levels are still the pin's own: the server holds a set level to the model
 * it runs on once the pin is back, so that is all a save can hold.
 */
export function EffortBlock({ view, models, write }: AsideProps) {
  const { t } = useTranslation();
  const { preferences } = usePreferences();
  const { metadata } = models;
  const unavailablePin = hasProblem(view, 'model_unavailable') ? view.model : null;
  const pinned = unavailablePin ? null : view.model;
  const stored = view.effort ?? '';
  // A name the catalog no longer has gets no ladder on the server either, and
  // any level saves.
  const pinMeta = unavailablePin ? metadata[unavailablePin] : undefined;

  // Every level some model the user can pick offers, to tell a pinned
  // model's short ladder from the vocabulary itself.
  const offeredAnywhere = useMemo(() => {
    const levels = new Set<string>();
    for (const data of Object.values(models.models)) {
      for (const m of data.models ?? []) {
        for (const lv of metadata[m]?.reasoning_efforts ?? []) levels.add(lv);
      }
    }
    return levels;
  }, [models.models, metadata]);

  let efforts: string[];
  let defaultLabel: string;
  let hint: string | null = null;
  if (pinned) {
    const tuning = resolveTuning(modelPrefs(preferences), modelProfile(preferences, pinned), metadata[pinned]);
    efforts = tuning.efforts;
    const named = effortLabelFor(t, tuning.effective.reasoning_effort);
    defaultLabel = named
      ? t('plugins.subagents.modelsSettingNamed', { effort: named })
      : t('plugins.subagents.modelsSetting');
    const model = modelName(pinned, metadata);
    // A set level hides what the model would pick, so that comes first; left
    // on the model's setting, the select already names it.
    if (efforts.length === 0) {
      hint = t('plugins.subagents.effortNone', { model });
    } else if (hasProblem(view, 'effort_not_offered')) {
      hint = t('plugins.subagents.effortHintInherit');
    } else if (stored && named) {
      hint = t('plugins.subagents.effortHintSetting', { effort: named });
    } else if ([...offeredAnywhere].some((lv) => !efforts.includes(lv) && EFFORT_LABELS[lv])) {
      hint = t('plugins.subagents.effortHintUnlisted', { model });
    }
  } else {
    const offered = pinMeta && (pinMeta.reasoning_efforts ?? []);
    efforts = EFFORT_ORDER.filter((lv) => EFFORT_LABELS[lv] && (!offered || offered.includes(lv)));
    defaultLabel = t('plugins.subagents.threadsSetting');
    hint =
      unavailablePin && offered && efforts.length === 0
        ? t('plugins.subagents.effortNone', { model: modelName(unavailablePin, metadata) })
        : t('plugins.subagents.effortHintInherit');
  }

  const base = view.base;
  return (
    <DetailAsideBlock
      title={t('plugins.subagents.effort')}
      side={base ? baseSide(t, view, effortOrSetting(t, base.effort, pinned)) : undefined}
    >
      {hasEffortOptions(efforts, stored) ? (
        <EffortSelect
          value={stored}
          onChange={(v) => write({ effort: v || null })}
          ariaLabel={t('plugins.subagents.effortAria', { name: view.name })}
          defaultLabel={defaultLabel}
          efforts={efforts}
          fill
        />
      ) : null}
      {hint && <p className="sa-hint">{hint}</p>}
    </DetailAsideBlock>
  );
}

function PresetMenu({ view, write }: Pick<AsideProps, 'view' | 'write'>) {
  const { t } = useTranslation();
  const locale = useLocale();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className="sa-preset">
          {t('plugins.subagents.preset.label')}
          <ChevronDown aria-hidden />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="min-w-[200px]">
        {PRESETS.map((preset) => {
          const on = sameTools(view.tools, preset.tools);
          return (
            <DropdownMenuItem key={preset.id} onSelect={() => write({ tools: [...preset.tools] })}>
              <span className="flex min-w-0 flex-col">
                <span>{t(preset.label)}</span>
                <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                  {toolsSummary(t, preset.tools, locale)}
                </span>
              </span>
              {on && <Check className="ml-auto h-4 w-4" />}
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * Its own search provider and depth. As in Settings, a level above the plan
 * is listed but not pickable, and Depth shows for a provider with more than
 * one level, and also to clear a saved level the provider lacks. A provider
 * change keeps the depth if the new provider offers it and drops it if not,
 * where Settings drops it on Default and on any provider with more than one
 * level. Untouched, both follow the user's own, which the hint names.
 */
function SearchSettings({ view, data, models, write }: AsideProps) {
  const { t } = useTranslation();
  const tierAllows = useSearchTierAllows();
  const catalog = models.searchProviders ?? {};
  const providerName = (name: string) => catalog[name]?.display_name ?? name;
  const levelName = (provider: string | null, level: string) => {
    const info = provider ? catalog[provider]?.depths.find((d) => d.name === level) : undefined;
    return t(`settings.searchDepthLevel.${level}`, info?.display_name ?? level);
  };

  const yours = data.main;
  const providers = Object.entries(catalog);
  const effective = view.search_provider ?? yours.search_provider;
  const depths = effective ? (catalog[effective]?.depths ?? []) : [];
  const depthShown = showsDepth(depths, view.search_depth);

  const providerOptions = providers.map(([value, p]) => ({
    value,
    label: p.display_name,
    disabled: !tierAllows(p.min_tier),
  }));
  if (view.search_provider && !catalog[view.search_provider]) {
    providerOptions.push({
      value: view.search_provider,
      label: t('plugins.subagents.unavailableOption', { name: view.search_provider }),
      disabled: true,
    });
  }
  const depthOptions = depths.map((d) => ({
    value: d.name,
    label: levelName(effective, d.name),
    disabled: !tierAllows(d.min_tier),
  }));
  // Counted before the saved level joins the list: the provider lacks that
  // one on every plan.
  const depthAbovePlan = depthOptions.some((o) => o.disabled);
  if (view.search_depth && depthShown && !depths.some((d) => d.name === view.search_depth)) {
    depthOptions.push({
      value: view.search_depth,
      label: t('plugins.subagents.unavailableOption', { name: view.search_depth }),
      disabled: true,
    });
  }

  // Untouched, its depth is the user's when the provider offers it, else that
  // provider's own default.
  const yourDepthHere =
    yours.search_depth && depths.some((d) => d.name === yours.search_depth) ? yours.search_depth : null;
  const depthDefault = yourDepthHere
    ? t('plugins.subagents.sameAsYoursNamed', { name: levelName(effective, yourDepthHere) })
    : effective && catalog[effective]?.default_depth
      ? t('plugins.subagents.depthDefaultNamed', { name: levelName(effective, catalog[effective].default_depth) })
      : t('plugins.subagents.sameAsYours');

  function pickProvider(next: string) {
    const nextProvider = next || yours.search_provider;
    const offered = nextProvider ? (catalog[nextProvider]?.depths ?? []) : [];
    const keep = !view.search_depth || offered.some((d) => d.name === view.search_depth);
    write({ search_provider: next || null, ...(keep ? {} : { search_depth: null }) });
  }

  const yoursLine = yours.search_provider
    ? yours.search_depth
      ? t('plugins.subagents.searchYours', {
          provider: providerName(yours.search_provider),
          depth: levelName(yours.search_provider, yours.search_depth),
        })
      : t('plugins.subagents.searchYoursProvider', { provider: providerName(yours.search_provider) })
    : t('plugins.subagents.searchCredits');

  const notOnPlan = hasProblem(view, 'search_not_on_plan');
  const providerUnavailable = hasProblem(view, 'search_unavailable');
  const depthMissing = view.problems.find((p) => p.code === 'depth_not_offered');

  return (
    <div className="sa-search">
      <span className="sa-search-label">{t('plugins.subagents.searchProvider')}</span>
      <CompactSelect
        value={view.search_provider ?? ''}
        onChange={pickProvider}
        ariaLabel={t('plugins.subagents.searchProvider')}
        defaultLabel={
          yours.search_provider
            ? t('plugins.subagents.sameAsYoursNamed', { name: providerName(yours.search_provider) })
            : t('plugins.subagents.sameAsYours')
        }
        options={providerOptions}
        disabled={!providerOptions.some((o) => !o.disabled) && !view.search_provider}
        fill
      />
      {depthShown && (
        <>
          <span className="sa-search-label">{t('plugins.subagents.depth')}</span>
          <CompactSelect
            value={view.search_depth ?? ''}
            onChange={(v) => write({ search_depth: v || null })}
            ariaLabel={t('plugins.subagents.depth')}
            defaultLabel={depthDefault}
            options={depthOptions}
            disabled={!depthOptions.some((o) => !o.disabled) && !view.search_depth}
            fill
          />
        </>
      )}
      {notOnPlan && <p className="sa-search-hint is-warn">{t('plugins.subagents.searchNotOnPlanHint')}</p>}
      {providerUnavailable && <p className="sa-search-hint is-warn">{t('plugins.subagents.searchUnavailableHint')}</p>}
      {depthMissing && (
        <p className="sa-search-hint is-warn">
          {t('plugins.subagents.depthNotOfferedHint', {
            provider: depthMissing.detail ? providerName(depthMissing.detail) : effective ? providerName(effective) : '',
          })}
        </p>
      )}
      <p className="sa-search-hint">{yoursLine}</p>
      {providerOptions.some((o) => o.disabled) && (
        <p className="sa-search-hint">{t('settings.searchProviderUpgradeHint')}</p>
      )}
      {depthShown && depthAbovePlan && (
        <p className="sa-search-hint">{t('settings.searchDepthUpgradeHint')}</p>
      )}
    </div>
  );
}

export function ToolsBlock(props: AsideProps) {
  const { view, write } = props;
  const { t } = useTranslation();
  const locale = useLocale();
  const has = (tool: SubagentTool) => view.tools.includes(tool);
  function toggle(tool: SubagentTool) {
    const next = has(tool) ? view.tools.filter((x) => x !== tool) : orderedTools([...view.tools, tool]);
    write({ tools: next });
  }
  const base = view.base;
  const hint = !base
    ? t('plugins.subagents.filesAlwaysOn')
    : sameTools(view.tools, base.tools)
      ? t('plugins.subagents.toolsSameAsBase')
      : t('plugins.subagents.toolsBaseDiffers', { base: baseSide(t, view, toolsSummary(t, base.tools, locale)) });

  return (
    <DetailAsideBlock title={t('plugins.subagents.tools')} side={<PresetMenu view={view} write={write} />}>
      <ul className="sa-switches">
        {TOOL_ORDER.map((tool) => (
          <ToolRow key={tool} tool={tool} on={has(tool)} onToggle={() => toggle(tool)}>
            {tool === 'web' && has('web') ? (
              <SearchSettings {...props} />
            ) : tool === 'code' && has('code') ? (
              <ul className="sa-nest-list">
                <SwitchRow
                  label={t('plugins.subagents.connectors')}
                  desc={t('plugins.subagents.connectorsDesc')}
                  enabled={view.connectors}
                  disabled={false}
                  onToggle={() => write({ connectors: !view.connectors })}
                />
              </ul>
            ) : null}
          </ToolRow>
        ))}
      </ul>
      <p className="sa-hint">{hint}</p>
    </DetailAsideBlock>
  );
}

function ToolRow({
  tool,
  on,
  onToggle,
  children,
}: {
  tool: SubagentTool;
  on: boolean;
  onToggle: () => void;
  children?: React.ReactNode;
}) {
  const { t } = useTranslation();
  // One node or null, never a list: a list of two `false`s is truthy and
  // would draw an empty nest under every row.
  const nested = children != null && children !== false;
  return (
    <>
      <SwitchRow
        label={t(TOOL_COPY[tool].label)}
        desc={t(TOOL_COPY[tool].desc)}
        enabled={on}
        disabled={false}
        onToggle={onToggle}
      />
      {nested && <li className="sa-nest">{children}</li>}
    </>
  );
}

/**
 * Access to the user's brokerages. It sits on the brokerage connectors, which
 * run as code, so it does nothing with Code execution off.
 */
export function BrokeragesBlock({ view, write }: Pick<AsideProps, 'view' | 'write'>) {
  const { t } = useTranslation();
  const codeOn = view.tools.includes('code');
  const access = view.brokerages !== 'off';
  const base = view.base;
  return (
    <DetailAsideBlock
      title={t('plugins.subagents.brokerages')}
      side={base ? baseSide(t, view, brokerageText(t, base.brokerages)) : undefined}
    >
      <ul className="sa-switches">
        <SwitchRow
          label={t('plugins.subagents.access')}
          desc={
            codeOn ? (
              <Trans
                i18nKey="plugins.subagents.accessDesc"
                components={{ brokerages: <Link to="/plugins?tab=brokerages" className="sa-link" /> }}
              />
            ) : (
              t('plugins.subagents.needsCode')
            )
          }
          enabled={access && codeOn}
          disabled={!codeOn}
          locked={!codeOn}
          onToggle={() => write({ brokerages: access ? 'off' : 'on' })}
        />
      </ul>
    </DetailAsideBlock>
  );
}

/**
 * Whether it reads the user's memory. Only the main agent saves to it, so a
 * subagent hands back anything worth keeping in its result.
 */
export function MemoryBlock({ view, write }: Pick<AsideProps, 'view' | 'write'>) {
  const { t } = useTranslation();
  const base = view.base;
  return (
    <DetailAsideBlock
      title={t('plugins.subagents.memory')}
      side={base ? baseSide(t, view, memoryText(t, base.memory)) : undefined}
    >
      <ul className="sa-switches">
        <SwitchRow
          label={t('plugins.subagents.memory')}
          desc={t('plugins.subagents.memoryDesc')}
          enabled={view.memory}
          disabled={false}
          onToggle={() => write({ memory: !view.memory })}
        />
      </ul>
    </DetailAsideBlock>
  );
}

export function PreferredBlock({ view, data, write }: AsideProps) {
  const { t } = useTranslation();
  const skills = usePreferredSkillOptions();
  const base = view.base;
  const connectorLabel = (name: string) => data.connectors.find((c) => c.name === name)?.label ?? name;
  const connectorOff = (name: string) => data.connectors.some((c) => c.name === name && !c.enabled);
  const skillCommand = (name: string) => skills.find((s) => s.name === name)?.command ?? name;
  const skillOff = (name: string) => skills.some((s) => s.name === name && s.off);

  const shipped = base ? [...base.preferred_connectors, ...base.preferred_skills.map((s) => `/${s}`)] : [];
  const side = base
    ? baseSide(t, view, shipped.length > 0 ? shipped.join(t('plugins.shell.summary.sep')) : t('plugins.subagents.none'))
    : undefined;

  const setConnectors = (next: string[]) => write({ preferred_connectors: next });
  const setSkills = (next: string[]) => write({ preferred_skills: next });

  return (
    <DetailAsideBlock title={t('plugins.subagents.preferred')} side={side}>
      <p className="sa-sublabel">{t('plugins.subagents.preferredConnectors')}</p>
      <div className="sa-chips">
        {view.preferred_connectors.map((name) => (
          <Chip
            key={name}
            text={name}
            title={connectorLabel(name)}
            missing={hasProblem(view, 'connector_missing', name)}
            off={connectorOff(name)}
            offNote={t('plugins.subagents.connectorOff')}
            onRemove={() => setConnectors(view.preferred_connectors.filter((n) => n !== name))}
          />
        ))}
        <AddChip
          label={t('plugins.subagents.addConnector')}
          // One switched off in Connectors is nothing to reach for.
          options={data.connectors
            .filter((c) => c.enabled && !view.preferred_connectors.includes(c.name))
            .map((c) => ({ value: c.name, label: c.label }))}
          onPick={(name) => setConnectors([...view.preferred_connectors, name])}
        />
      </div>
      <p className="sa-sublabel">{t('plugins.subagents.preferredSkills')}</p>
      <div className="sa-chips">
        {view.preferred_skills.map((name) => (
          <Chip
            key={name}
            text={`/${skillCommand(name)}`}
            title={name}
            missing={hasProblem(view, 'skill_missing', name)}
            off={skillOff(name)}
            offNote={t('plugins.subagents.skillOff')}
            onRemove={() => setSkills(view.preferred_skills.filter((n) => n !== name))}
          />
        ))}
        <AddChip
          label={t('plugins.subagents.addSkill')}
          options={addableSkills(skills, data.skills, view.preferred_skills).map((s) => ({
            value: s.name,
            label: `/${s.command ?? s.name}`,
          }))}
          onPick={(name) => setSkills([...view.preferred_skills, name])}
        />
      </div>
      <p className="sa-hint" style={{ marginTop: 0 }}>
        {t('plugins.subagents.preferredHint')}
      </p>
    </DetailAsideBlock>
  );
}

function usePreferredSkillOptions(): PreferredSkillOption[] {
  const { data } = useSkills({ includeDisabled: true, allScopes: true });
  return useMemo(() => preferredSkillOptions(data ?? []), [data]);
}

function Chip({
  text,
  title,
  missing,
  off = false,
  offNote,
  onRemove,
}: {
  text: string;
  title: string;
  missing: boolean;
  /** Still there, but switched off where it lives. */
  off?: boolean;
  /** Where that is, said on hover. */
  offNote?: string;
  onRemove: () => void;
}) {
  const { t } = useTranslation();
  const note = missing ? t('plugins.subagents.missing') : off && offNote ? offNote : title;
  return (
    <span className={cn('sa-chip', missing && 'is-missing', !missing && off && 'is-off')} title={note}>
      <span>{text}</span>
      <button type="button" onClick={onRemove} aria-label={t('plugins.subagents.removeChip', { name: text })}>
        <X aria-hidden />
      </button>
    </span>
  );
}

function AddChip({
  label,
  options,
  onPick,
}: {
  label: string;
  options: { value: string; label: string }[];
  onPick: (value: string) => void;
}) {
  const { t } = useTranslation();
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className="sa-chip-add">
          <Plus aria-hidden />
          {label}
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="sa-add-menu min-w-[200px]">
        {options.length === 0 ? (
          <DropdownMenuItem disabled>{t('plugins.subagents.nothingToAdd')}</DropdownMenuItem>
        ) : (
          options.map((o) => (
            <DropdownMenuItem key={o.value} onSelect={() => onPick(o.value)}>
              {o.label}
            </DropdownMenuItem>
          ))
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/** Which agent may hand it work. One of the two stays ticked: with neither,
 *  nothing could reach it, and switching it off says that plainly. */
export function AvailableBlock({ view, write }: Pick<AsideProps, 'view' | 'write'>) {
  const { t } = useTranslation();
  function set(audience: SubagentAudience, on: boolean) {
    const next = AUDIENCES.filter((a) => (a === audience ? on : view.available_to.includes(a)));
    write({ available_to: next });
  }
  return (
    <DetailAsideBlock title={t('plugins.subagents.availableTo')}>
      <div className="sa-checks">
        {AUDIENCES.map((audience) => {
          const checked = view.available_to.includes(audience);
          return (
            <CheckboxChoice
              key={audience}
              checked={checked}
              disabled={checked && view.available_to.length === 1}
              onCheckedChange={(on) => set(audience, on)}
              className="items-center gap-2 rounded-md"
            >
              <span>{audienceChoice(t, audience)}</span>
            </CheckboxChoice>
          );
        })}
      </div>
      <p className="sa-hint">{t('plugins.subagents.availableHint')}</p>
    </DetailAsideBlock>
  );
}
