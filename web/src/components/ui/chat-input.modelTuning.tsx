import { useId, useRef, type FocusEvent as ReactFocusEvent } from 'react';
import { Check } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import {
  Header, Menu, MenuItem, MenuSection, ToggleButton, ToggleButtonGroup, type Selection,
} from 'react-aria-components';
import { MENU_ROW, MENU_SECTION, useFocusCheckedRow } from './chat-input.composerPopover';
import { EFFORT_LABELS, isCodexModel, tuningMenuLevels } from '@/lib/modelTuning';
import type { ModelMetadataEntry, TaskCost } from '@/hooks/useFilteredModels';
import type { Payer } from '@/lib/modelPayer';
import { NameTag } from '@/components/model/PayerBadge';
import { TaskCostFigure, TaskCostHeading } from '@/components/model/TaskCost';
import { costBarFraction, costsRelativeTo, formatCostMultiple, paysPerToken } from '@/lib/taskCost';
import { formatTokenPrice } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { cn } from '@/lib/utils';

/**
 * The thread's tuning of the current model, as the composer resolves it: the
 * reasoning level a send runs and, on a Codex model, its speed. What the
 * model offers comes from its metadata. The model picker shows it beside the
 * model list (a pane) or under it (a strip).
 */
export interface ModelTuning {
  /** The level the composer shows and a send runs; null names none. */
  reasoningEffort: string | null;
  /** What Settings gives this model: its profile, else the account default,
   *  else the manifest default. Carries the "Default" tag, and picking it
   *  names null, so the thread follows Settings rather than pinning the same
   *  value. */
  defaultEffort: string | null;
  onReasoningEffortChange: (effort: string | null) => void;
  /** Null while the thread's own value is not known, which checks neither. */
  fastMode: boolean | null;
  onFastModeChange: (fast: boolean) => void;
}

/** What the pane and the strip both list: each level the model offers with
 *  its "Default" tag and costs, and, on a Codex model, the two speeds. */
function tuningRows(
  { defaultEffort, onReasoningEffortChange, fastMode }: ModelTuning,
  meta: ModelMetadataEntry | undefined,
  t: TFunction,
) {
  const levels = tuningMenuLevels(meta?.reasoning_efforts ?? []);
  const taskCost = meta?.task_cost;
  // The figure printed is a multiple of the level marked "Default", so it
  // compares this model's levels and nothing else; the bar is drawn from
  // the cross-model figure, so a short bar is cheap against every model.
  const relative = costsRelativeTo(taskCost, defaultEffort);
  const speeds = isCodexModel(meta)
    ? [
      { id: 'standard', fast: false, label: t('chat.modelSelector.speedStandard'), hint: undefined },
      { id: 'fast', fast: true, label: t('chat.modelSelector.speedFast'), hint: t('chat.modelSelector.speedFastHint') },
    ]
    : [];
  return {
    levels: levels.map((level) => ({
      level,
      label: t(EFFORT_LABELS[level]),
      isDefault: level === defaultEffort,
      relative: relative[level],
      absolute: taskCost?.[level],
    })),
    showCosts: levels.some((level) => relative[level] || taskCost?.[level]),
    speeds,
    /** The checked speed's id, none while the thread's is not known. */
    speed: speeds.find((row) => row.fast === fastMode)?.id ?? null,
    // Picking the level Settings gives clears the thread's own instead of
    // pinning today's default as a permanent choice.
    pick: (level: string) => onReasoningEffortChange(level === defaultEffort ? null : level),
  };
}

const SECTION_HEADER = 'px-2 pb-1 pt-1.5 text-xs text-(--color-text-tertiary)';

/** The chosen level's or speed's mark, beside its name. */
function ChosenCheck() {
  return <Check aria-hidden className="h-3.5 w-3.5 flex-none text-(--color-accent-primary)" />;
}

/** A level's cost against the whole catalog, as a thin bar. */
function CostBar({ cost, range }: { cost: TaskCost | undefined; range: [number, number] | null }) {
  if (!cost) return <span aria-hidden className="w-8 flex-none" />;
  return (
    <span aria-hidden className="h-1 w-8 flex-none overflow-hidden rounded-full bg-(--color-border-default)">
      <span
        className={cn(
          'block h-full rounded-full',
          cost.measured ? 'bg-(--color-text-tertiary)' : 'bg-(--color-border-strong)',
        )}
        style={{ width: `${costBarFraction(cost.value, range) * 100}%` }}
      />
    </span>
  );
}

/** Section keys, kept apart since one menu holds both lists. */
const EFFORT = 'effort:';
const SPEED = 'speed:';

/**
 * The pane beside the model list: the current model's levels with what a
 * task costs at each, its speeds on a Codex model, and its API price where
 * the user pays per token. A model with nothing to tune keeps the pane, with
 * a line saying so, so the picker holds its width as the model changes.
 *
 * A react-aria menu, as the model list beside it is a grid, so each is one
 * Tab stop with its own arrow keys. A pick never closes the picker: a level
 * is a setting, not a send.
 */
export function TuningPane({ tuning, meta, payer, range, disabled = false }: {
  tuning: ModelTuning;
  meta: ModelMetadataEntry | undefined;
  payer: Payer | undefined;
  /** The catalog's cheapest and dearest figures, which the bars span. */
  range: [number, number] | null;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const menuRef = useRef<HTMLDivElement>(null);
  const noteId = useId();
  const { levels, showCosts, speeds, speed, pick } = tuningRows(tuning, meta, t);
  const hasMenu = levels.length + speeds.length > 0;
  // Keyed on the menu being there: a pick can turn "Nothing to tune" into a
  // menu while the picker stays open, and the ref alone never changes.
  useFocusCheckedRow(menuRef, hasMenu);
  const { reasoningEffort, onFastModeChange } = tuning;
  const price = paysPerToken(payer) ? meta?.pricing : undefined;

  const onSpeed = (keys: Selection) => {
    const [key] = keys === 'all' ? [] : [...keys];
    const row = speeds.find((s) => `${SPEED}${s.id}` === key);
    if (row) onFastModeChange(row.fast);
  };

  return (
    <div
      data-slot="tuning-pane"
      className="flex w-60 flex-none flex-col border-l border-[color:var(--color-shell-divider,var(--color-border-muted))]"
    >
      {hasMenu ? (
        <Menu
          ref={menuRef}
          aria-label={t('chat.modelSelector.reasoning')}
          aria-describedby={showCosts ? noteId : undefined}
          disabledKeys={disabled ? [...levels.map((r) => `${EFFORT}${r.level}`), ...speeds.map((r) => `${SPEED}${r.id}`)] : []}
          className="min-h-0 flex-1 overflow-y-auto p-1 outline-hidden"
        >
          {levels.length > 0 && (
            <MenuSection
              selectionMode="single"
              disallowEmptySelection
              selectedKeys={reasoningEffort ? [`${EFFORT}${reasoningEffort}`] : []}
              className={MENU_SECTION}
            >
              <Header className={cn(SECTION_HEADER, 'flex items-center justify-between gap-2')}>
                {t('chat.modelSelector.reasoning')}
                {/* Over the figures, which end each row at its edge. */}
                {showCosts && <TaskCostHeading note={t('chat.modelSelector.taskCostNote')} />}
              </Header>
              {/* An action, not a selection change: a thread pinned to the level
                  that has since become the default shows it checked, and picking
                  a checked row changes no selection. */}
              {levels.map((row) => (
                <MenuItem
                  key={row.level}
                  id={`${EFFORT}${row.level}`}
                  textValue={row.label}
                  onAction={() => pick(row.level)}
                  className={MENU_ROW}
                >
                  {({ isSelected }) => (
                    <>
                      {/* The check follows the name rather than holding a column
                          at the row's end: the names stay put as the choice
                          moves, and so do the figures, which end every row. */}
                      <span className="flex min-w-0 flex-1 items-center gap-1.5">
                        <span className="truncate">{row.label}</span>
                        {isSelected && <ChosenCheck />}
                        {row.isDefault && <NameTag>{t('chat.modelSelector.defaultBadge')}</NameTag>}
                      </span>
                      {showCosts && <CostBar cost={row.absolute} range={range} />}
                      {showCosts && <TaskCostFigure cost={row.relative ?? null} className="min-w-7 text-right" />}
                    </>
                  )}
                </MenuItem>
              ))}
            </MenuSection>
          )}
          {/* Speed (Codex only: priority service tier) */}
          {speeds.length > 0 && (
            <MenuSection
              selectionMode="single"
              disallowEmptySelection
              selectedKeys={speed === null ? [] : [`${SPEED}${speed}`]}
              onSelectionChange={onSpeed}
              className={MENU_SECTION}
            >
              <Header className={SECTION_HEADER}>{t('chat.modelSelector.speed')}</Header>
              {speeds.map((row) => (
                <MenuItem
                  key={row.id}
                  id={`${SPEED}${row.id}`}
                  textValue={row.label}
                  className={MENU_ROW}
                >
                  {({ isSelected }) => (
                    <>
                      <span className="flex min-w-0 flex-1 items-center gap-1.5">
                        <span className="truncate">{row.label}</span>
                        {isSelected && <ChosenCheck />}
                      </span>
                      {row.hint && <span className="flex-none text-xs text-(--color-text-tertiary)">{row.hint}</span>}
                    </>
                  )}
                </MenuItem>
              ))}
            </MenuSection>
          )}
        </Menu>
      ) : (
        <p className="flex-1 px-3 py-3 text-xs text-(--color-text-tertiary)">{t('chat.modelSelector.nothingToTune')}</p>
      )}
      {showCosts && <span id={noteId} hidden>{t('chat.modelSelector.taskCostNote')}</span>}
      {price && (
        <div className="rule-t flex flex-none flex-col gap-0.5 px-3 py-2 text-xs">
          <span className="text-(--color-text-tertiary)">{t('chat.modelSelector.apiPrice')}</span>
          <span className="tabular-nums text-(--color-text-secondary)">
            {t('chat.modelSelector.apiPriceValue', { input: formatTokenPrice(price.input, locale), output: formatTokenPrice(price.output, locale) })}
          </span>
        </div>
      )}
    </div>
  );
}

/** Tab into a segmented control lands on its checked segment, as it does in a
 *  radio group, so arrowing starts from what is set; the group itself would
 *  land on its first. */
function toCheckedSegment(e: ReactFocusEvent<HTMLElement>) {
  const group = e.currentTarget;
  if (group.contains(e.relatedTarget as Node | null)) return;
  const checked = group.querySelector<HTMLElement>('[aria-checked="true"]');
  if (checked && checked !== e.target) checked.focus();
}

const SEGMENTS = 'flex min-w-0 gap-0.5 rounded-md bg-[var(--color-shell-fill,var(--color-bg-tag))] p-0.5';
// The label truncates rather than the button, whose overflow would clip its
// touch area; that area keeps to the segment's width and short of the row below.
const SEGMENT = cn(
  'tap-target [--tap-target-w:100%] [--tap-target-h:2rem] min-w-0 flex-auto rounded px-1.5 py-1 text-xs font-medium leading-none text-(--color-text-tertiary) outline-hidden',
  'data-hovered:text-(--color-text-secondary) data-selected:bg-[var(--color-shell-fill,var(--color-bg-elevated))] data-selected:text-(--color-text-primary)',
  'data-focus-visible:outline-current data-disabled:opacity-50',
);

/**
 * The same tuning under the model list, where the picker has no room for a
 * pane beside it: segmented levels, and speeds under them on a Codex model.
 * A level's cost goes in its tooltip and its description, as the segments
 * have no room to print it.
 */
export function TuningStrip({ tuning, meta, disabled = false }: {
  tuning: ModelTuning;
  meta: ModelMetadataEntry | undefined;
  disabled?: boolean;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const ids = useId();
  const { levels, speeds, speed, pick } = tuningRows(tuning, meta, t);
  const { reasoningEffort, onFastModeChange } = tuning;
  if (levels.length + speeds.length === 0) return null;
  const defaultLabel = levels.find((row) => row.isDefault)?.label;
  const costLine = (cost: TaskCost | undefined) => cost && t(
    cost.measured ? 'chat.modelSelector.taskCostVsDefault' : 'chat.modelSelector.taskCostVsDefaultAtMost',
    { value: formatCostMultiple(cost.value, locale) },
  );

  return (
    <div data-slot="tuning-strip" className="rule-t flex flex-none flex-col gap-1.5 px-3 py-2">
      {levels.length > 0 && (
        <>
          <div className="flex items-center justify-between gap-2 text-xs text-(--color-text-tertiary)">
            <span>{t('chat.modelSelector.reasoning')}</span>
            {defaultLabel && <span className="truncate">{t('chat.modelSelector.defaultLevel', { level: defaultLabel })}</span>}
          </div>
          {/* The group takes no focus handler of its own. */}
          <div className="contents" onFocus={toCheckedSegment}>
          <ToggleButtonGroup
            aria-label={t('chat.modelSelector.reasoning')}
            selectionMode="single"
            disallowEmptySelection
            selectedKeys={reasoningEffort ? [reasoningEffort] : []}
            isDisabled={disabled}
            data-slot="segmented"
            className={SEGMENTS}
          >
            {levels.map((row) => {
              const line = costLine(row.relative);
              return (
                <ToggleButton
                  key={row.level}
                  id={row.level}
                  // Pressed, not selected: picking the checked level that is
                  // the default still has to clear the thread's own.
                  onPress={() => pick(row.level)}
                  aria-describedby={line ? `${ids}-${row.level}` : undefined}
                  className={SEGMENT}
                >
                  <span title={line || undefined} className="block truncate">{row.label}</span>
                  {line && <span id={`${ids}-${row.level}`} hidden>{line}</span>}
                </ToggleButton>
              );
            })}
          </ToggleButtonGroup>
          </div>
        </>
      )}
      {speeds.length > 0 && (
        <div className="flex items-center justify-between gap-2">
          <span className="text-xs text-(--color-text-tertiary)">{t('chat.modelSelector.speed')}</span>
          <div className="contents" onFocus={toCheckedSegment}>
          <ToggleButtonGroup
            aria-label={t('chat.modelSelector.speed')}
            selectionMode="single"
            disallowEmptySelection
            selectedKeys={speed === null ? [] : [speed]}
            onSelectionChange={(keys) => {
              const [key] = keys;
              const row = speeds.find((s) => s.id === key);
              if (row) onFastModeChange(row.fast);
            }}
            isDisabled={disabled}
            data-slot="segmented"
            className={cn(SEGMENTS, 'w-40 flex-none')}
          >
            {speeds.map((row) => (
              <ToggleButton
                key={row.id}
                id={row.id}
                aria-describedby={row.hint ? `${ids}-${row.id}` : undefined}
                className={SEGMENT}
              >
                <span title={row.hint} className="block truncate">{row.label}</span>
                {row.hint && <span id={`${ids}-${row.id}`} hidden>{row.hint}</span>}
              </ToggleButton>
            ))}
          </ToggleButtonGroup>
          </div>
        </div>
      )}
    </div>
  );
}
