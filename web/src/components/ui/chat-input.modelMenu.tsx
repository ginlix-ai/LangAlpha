import { useId, useRef, useState, type MouseEvent as ReactMouseEvent, type ReactNode, type RefObject } from 'react';
import { Check, ChevronDown, Gauge, Info, Rocket } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import { useNavigate } from 'react-router';
import { Header, Menu, MenuItem, MenuSection, type Selection } from 'react-aria-components';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from './tooltip';
import {
  ComposerPopover, MENU_ROW, MENU_SECTION, useFocusCheckedRow,
} from './chat-input.composerPopover';
import {
  DropdownMenuLabel, DropdownMenuRadioGroup, DropdownMenuRadioItem, DropdownMenuSeparator,
  DropdownMenuSub, DropdownMenuSubContent, DropdownMenuSubTrigger,
} from './dropdown-menu';
import { getModelDisplayName } from './chat-input.helpers';
import { ModelPicker, type ModelRowStatus } from './chat-input.modelPicker';
import { EFFORT_LABELS, hasTuningMenu, tuningMenuLevels } from '@/lib/modelTuning';
import type { ModelMetadataEntry, TaskCost } from '@/hooks/useFilteredModels';
import type { ModelGroup } from '@/lib/modelGroups';
import type { Payer } from '@/lib/modelPayer';
import { MarkedModelName } from '@/components/model/ModelMark';
import { llmProviderArt } from '@/lib/brandArt';
import { NameTag } from '@/components/model/PayerBadge';
import { TaskCostFigure } from '@/components/model/TaskCost';
import { costsRelativeTo } from '@/lib/taskCost';
import { cn } from '@/lib/utils';

type ModelMetadata = Record<string, ModelMetadataEntry>;

/** Pill geometry for the model and effort triggers, shared with the measure
 *  chips so the fold budget can never drift from what actually renders. */
const TRIGGER_CLASS = 'inline-flex items-center gap-1 rounded-lg py-1.5 px-2.5 text-[0.8125rem] font-medium border border-transparent whitespace-nowrap';

/** How each pill takes a squeeze: the model's name truncates, while nothing
 *  in the effort pill can, so it keeps its width and folds into ⋯ instead. */
const PILL_FLEX = { model: 'min-w-0', effort: 'flex-none' } as const;

/** The least of the model name the pill shows: two characters and the
 *  ellipsis ("V4…"). Below it the name gives way to the logo alone, where
 *  the model has one. */
const NAME_FLOOR = 'max-w-8';

function ModelTriggerBody({ selectedModel, metadata, floor = false, nameHidden = false }: {
  selectedModel: string | null;
  metadata?: ModelMetadata;
  floor?: boolean;
  /** Logo only. The name stays for a screen reader, which reads it as the
   *  pill's name. A model with no logo keeps the name at its floor, since a
   *  bare chevron would not say which model is picked. */
  nameHidden?: boolean;
}) {
  const { t } = useTranslation();
  const provider = selectedModel ? metadata?.[selectedModel]?.provider : undefined;
  const logoOnly = nameHidden && provider !== undefined && llmProviderArt(provider) !== undefined;
  return (
    <>
      <MarkedModelName
        provider={provider}
        name={getModelDisplayName(selectedModel, metadata) || t('chat.modelSelector.fallbackName')}
        nameClassName={logoOnly ? 'sr-only' : cn('min-w-0 truncate', floor || nameHidden ? NAME_FLOOR : 'max-w-[120px]')}
        short
      />
      <ChevronDown className="h-3 w-3 flex-none" />
    </>
  );
}

function EffortTriggerBody({ effortLabel, fast, accent }: {
  effortLabel: ReactNode;
  fast: boolean;
  accent?: boolean;
}) {
  return (
    <>
      {effortLabel && <span className="flex-none">{effortLabel}</span>}
      {fast && (
        <Rocket className="h-3 w-3 flex-none" style={accent ? { color: 'var(--color-accent-light)' } : undefined} />
      )}
      <ChevronDown className="h-3 w-3 flex-none" />
    </>
  );
}

/** Measure-row twin of the model or effort pill: same geometry, no
 *  interactivity. A model twin with `floor` cuts the name to the least the
 *  toolbar keeps before it folds the effort pill away, and then the name.
 *  An effort twin is as wide as the pill can get, its longest level and the
 *  rocket a Codex model can show, so a pick never moves the fold under the
 *  menu it was made in. It holds the level shown now too, which a Codex
 *  model with no levels to pick can still name. */
export function ModelTriggerMeasure(props:
  | { pill: 'model'; selectedModel: string | null; metadata?: ModelMetadata; floor?: boolean }
  | { pill: 'effort'; effortLabel: string | null; reasoningEfforts: readonly string[]; isCodexModel: boolean }) {
  const { t } = useTranslation();
  const labels = props.pill === 'effort'
    ? [...new Set([
      ...tuningMenuLevels(props.reasoningEfforts).map((level) => t(EFFORT_LABELS[level])),
      ...(props.effortLabel ? [props.effortLabel] : []),
    ])]
    : [];
  return (
    <button type="button" tabIndex={-1} className={TRIGGER_CLASS}>
      {props.pill === 'model'
        ? <ModelTriggerBody selectedModel={props.selectedModel} metadata={props.metadata} floor={props.floor} />
        : (
          <EffortTriggerBody
            // Stacked in one cell, the labels take the widest one's room.
            effortLabel={labels.length > 0 && (
              <span className="inline-grid">
                {labels.map((label) => (
                  <span key={label} className="col-start-1 row-start-1">{label}</span>
                ))}
              </span>
            )}
            fast={props.isCodexModel}
          />
        )}
    </button>
  );
}

/** A pill in the composer's toolbar. `data-pill` names it for tests, which
 *  should not lean on its translated title. */
function Pill({ pill, buttonRef, open, popup, onPress, title, disabled, children }: {
  pill: 'model' | 'effort';
  buttonRef?: RefObject<HTMLButtonElement | null>;
  open?: boolean;
  /** What it opens; none for a pill that goes somewhere instead. */
  popup?: 'dialog' | 'menu';
  onPress: () => void;
  title?: string;
  disabled?: boolean;
  children: ReactNode;
}) {
  return (
    <button
      ref={buttonRef}
      type="button"
      data-pill={pill}
      className={cn('model-selector-trigger', TRIGGER_CLASS, PILL_FLEX[pill], 'cursor-pointer transition-colors disabled:cursor-progress')}
      onClick={(e) => { e.stopPropagation(); onPress(); }}
      disabled={disabled}
      title={title}
      aria-haspopup={popup}
      aria-expanded={popup ? open : undefined}
      data-state={popup ? (open ? 'open' : 'closed') : undefined}
    >
      {children}
    </button>
  );
}

/**
 * The model pill and the picker it opens. With nothing to pick and nothing
 * selected, no model is set up yet, and the pill becomes the way to set one
 * up rather than leaving a composer with no hint of why it cannot send.
 */
export function ChatInputModelMenu({
  selectedModel, metadata, payers, groups, favorites, recent, onToggleFavorite, onSelectModel,
  rowStatus, dropdownDirection, container, draftRef, disabled = false, nameHidden = false,
}: {
  selectedModel: string | null;
  /** Supplies authored display names; a model without one gets a derived name. */
  metadata?: ModelMetadata;
  /** Who pays for a turn on each model, tagged beside its name. */
  payers: Record<string, Payer>;
  groups: ModelGroup[];
  favorites: string[];
  /** This thread's models and the current one that are not favorites. */
  recent: string[];
  onToggleFavorite: (model: string, favorite: boolean) => void;
  onSelectModel: (model: string) => void;
  /** What each row says beside its model, and whether it is a pick. */
  rowStatus: (model: string) => ModelRowStatus;
  dropdownDirection: 'up' | 'down';
  /** Mobile portals into the composer so the picker can't escape the sheet. */
  container: HTMLElement | null;
  draftRef?: RefObject<HTMLElement | null>;
  /** Until the model list arrives every model reads as reachable, and a pick
   *  is saved on the thread, so an unreachable one would become the model
   *  every later turn of that thread asks for. */
  disabled?: boolean;
  /** The toolbar has no room for even the least of the name (`computeFold`). */
  nameHidden?: boolean;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const toSettings = () => navigate('/settings?tab=model');

  if (groups.length === 0 && !selectedModel) {
    if (disabled) return null;
    return (
      <Pill pill="model" onPress={toSettings}>
        <span style={{ color: 'var(--color-text-secondary)' }}>{t('chat.modelSelector.setUpModel')}</span>
      </Pill>
    );
  }

  return (
    <>
      <Pill
        pill="model"
        buttonRef={triggerRef}
        open={open}
        popup="dialog"
        onPress={() => setOpen((v) => !v)}
        title={t('chat.modelSelector.select')}
        disabled={disabled}
      >
        <ModelTriggerBody selectedModel={selectedModel} metadata={metadata} nameHidden={nameHidden} />
      </Pill>
      <ModelPicker
        triggerRef={triggerRef}
        isOpen={open}
        onOpenChange={setOpen}
        direction={dropdownDirection}
        container={container}
        draftRef={draftRef}
        selectedModel={selectedModel}
        metadata={metadata}
        payers={payers}
        groups={groups}
        favorites={favorites}
        recent={recent}
        onToggleFavorite={onToggleFavorite}
        onPick={onSelectModel}
        rowStatus={rowStatus}
        onManage={toSettings}
      />
    </>
  );
}

/** Section keys, kept apart since one menu holds both lists. */
const EFFORT = 'effort:';
const SPEED = 'speed:';

export interface EffortMenuProps {
  /** Already-translated effective level, or null when the model has no ladder. */
  effortLabel: string | null;
  /** The level the composer shows and a send runs; null names none. */
  reasoningEffort: string | null;
  /** What Settings gives this model: its profile, else the account default,
   *  else the manifest default. Carries the "Default" tag, and picking it
   *  names null, so the thread follows Settings rather than pinning the same
   *  value. */
  defaultEffort: string | null;
  /** Exactly the levels this model honors, weakest first. */
  reasoningEfforts: string[];
  onReasoningEffortChange: (effort: string | null) => void;
  /** Null while the thread's own value is not known, which checks neither. */
  fastMode: boolean | null;
  onFastModeChange: (fast: boolean) => void;
  isCodexModel: boolean;
  dropdownDirection: 'up' | 'down';
  /** The composer, which a phone portals the menu into. */
  container: HTMLElement | null;
  /** The model's task cost at each level, shown as a multiple of its cost
   *  at the default level. */
  taskCost?: Record<string, TaskCost>;
  disabled?: boolean;
}

/**
 * Reasoning effort and, on a Codex model, speed: the model's tuning, set
 * apart from the choice of model. Renders nothing for a model with neither.
 *
 * A pill and panel like the model picker beside it, so the two read as one
 * toolbar, holding a react-aria menu. A pick never closes it: an effort is a
 * setting, not a send.
 */
export function ChatInputEffortMenu(props: EffortMenuProps) {
  if (!hasTuningMenu(props.reasoningEfforts, props.isCodexModel)) return null;
  return <EffortMenu {...props} />;
}

/** Holds the open state, so a model with nothing to tune drops it with the
 *  pill, and the next model's menu starts shut rather than springing open. */
function EffortMenu(props: EffortMenuProps) {
  const { effortLabel, fastMode, isCodexModel, dropdownDirection, container, disabled = false } = props;
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  return (
    <>
      <Pill
        pill="effort"
        buttonRef={triggerRef}
        open={open}
        popup="menu"
        onPress={() => setOpen((v) => !v)}
        title={t('chat.modelSelector.reasoningEffort')}
        disabled={disabled}
      >
        <EffortTriggerBody effortLabel={effortLabel} fast={fastMode === true && isCodexModel} accent />
      </Pill>
      <ComposerPopover
        triggerRef={triggerRef}
        isOpen={open}
        onOpenChange={setOpen}
        direction={dropdownDirection}
        align="end"
        container={container}
        label={t('chat.modelSelector.reasoningEffort')}
        className="w-60"
      >
        <EffortList {...props} />
      </ComposerPopover>
    </>
  );
}

/**
 * What the effort menu lists, the same in the pill's panel and in ⋯'s
 * submenu: each level with its "Default" tag and task cost, and, on a Codex
 * model, the two speeds.
 */
function effortRows(
  { reasoningEfforts, defaultEffort, onReasoningEffortChange, isCodexModel, taskCost }: EffortMenuProps,
  t: TFunction,
) {
  const levels = tuningMenuLevels(reasoningEfforts);
  // The level marked "Default" is 1x, so the figures compare this model's
  // levels and nothing else.
  const costs = costsRelativeTo(taskCost, defaultEffort);
  return {
    levels: levels.map((level) => ({
      level,
      label: t(EFFORT_LABELS[level]),
      tag: level === defaultEffort ? t('chat.modelSelector.defaultBadge') : undefined,
      cost: costs[level],
    })),
    showCosts: levels.some((level) => costs[level]),
    speeds: isCodexModel
      ? [
        { fast: false, label: t('chat.modelSelector.speedStandard'), hint: undefined },
        { fast: true, label: t('chat.modelSelector.speedFast'), hint: t('chat.modelSelector.speedFastHint') },
      ]
      : [],
    // Picking the level Settings gives clears the thread's own instead of
    // pinning today's default as a permanent choice.
    pick: (level: string) => onReasoningEffortChange(level === defaultEffort ? null : level),
  };
}

/** The task-cost column's heading, whose tooltip says what the figures mean. */
function TaskCostHeading() {
  const { t } = useTranslation();
  return (
    <TooltipProvider delayDuration={300}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="mr-6 inline-flex cursor-help items-center gap-1">
            {t('chat.modelSelector.taskCostHeader')}
            <Info aria-hidden className="h-3 w-3" />
          </span>
        </TooltipTrigger>
        <TooltipContent side="top" className="max-w-56">{t('chat.modelSelector.taskCostNote')}</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** What the figures mean, for the menu to name as its description: the
 *  tooltip says it under a pointer, but a menu's arrow keys reach only its
 *  rows. Hidden, yet still the description. */
function TaskCostNote({ id }: { id: string }) {
  const { t } = useTranslation();
  return <span id={id} hidden>{t('chat.modelSelector.taskCostNote')}</span>;
}

function EffortList(props: EffortMenuProps) {
  const { reasoningEffort, fastMode, onFastModeChange } = props;
  const { t } = useTranslation();
  const menuRef = useRef<HTMLDivElement>(null);
  const noteId = useId();
  useFocusCheckedRow(menuRef);
  const { levels, showCosts, speeds, pick } = effortRows(props, t);

  const onSpeed = (keys: Selection) => {
    const [key] = keys === 'all' ? [] : [...keys];
    if (key !== undefined) onFastModeChange(key === `${SPEED}fast`);
  };

  return (
    <>
      <Menu
        ref={menuRef}
        aria-label={t('chat.modelSelector.reasoningEffort')}
        aria-describedby={showCosts ? noteId : undefined}
        autoFocus
        className="p-1 outline-hidden"
      >
        {levels.length > 0 && (
          <MenuSection
            selectionMode="single"
            disallowEmptySelection
            selectedKeys={reasoningEffort ? [`${EFFORT}${reasoningEffort}`] : []}
            className={MENU_SECTION}
          >
            <Header className={cn(SECTION_HEADER, 'flex justify-between gap-2')}>
              {t('chat.modelSelector.reasoningEffort')}
              {/* Over the figures, which end a check's width from the edge. */}
              {showCosts && <TaskCostHeading />}
            </Header>
            {/* An action, not a selection change: a thread pinned to the level
                that has since become the default shows it checked, and picking
                a checked row changes no selection. */}
            {levels.map((row) => (
              <SettingOption
                key={row.level}
                id={`${EFFORT}${row.level}`}
                label={row.label}
                tag={row.tag}
                cost={row.cost}
                onAction={() => pick(row.level)}
              />
            ))}
          </MenuSection>
        )}
        {/* Speed (Codex only: priority service tier) */}
        {speeds.length > 0 && (
          <MenuSection
            selectionMode="single"
            disallowEmptySelection
            selectedKeys={fastMode === null ? [] : [`${SPEED}${fastMode ? 'fast' : 'standard'}`]}
            onSelectionChange={onSpeed}
            className={MENU_SECTION}
          >
            <Header className={SECTION_HEADER}>{t('chat.modelSelector.speed')}</Header>
            {speeds.map((speed) => (
              <SettingOption
                key={String(speed.fast)}
                id={`${SPEED}${speed.fast ? 'fast' : 'standard'}`}
                label={speed.label}
                hint={speed.hint}
              />
            ))}
          </MenuSection>
        )}
      </Menu>
      {showCosts && <TaskCostNote id={noteId} />}
    </>
  );
}

// The model picker's tab header.
const SECTION_HEADER = 'px-2 pb-1 pt-1.5 text-xs text-(--color-text-tertiary)';

/** A pick's click still reaches the composer through the portal, and the
 *  composer answers any click by focusing its text box, which would close the
 *  menu the pick means to leave open. */
const keepMenuOpen = (e: ReactMouseEvent) => e.stopPropagation();

/**
 * The levels and speeds as ⋯ lists them, in its submenu or in ⋯ itself. A
 * pick leaves the menu open, as the pill's does.
 */
function EffortOverflowRows({ rows, title, noteId, reasoningEffort, fastMode, onFastModeChange, disabled = false }: Pick<
  EffortMenuProps, 'reasoningEffort' | 'fastMode' | 'onFastModeChange' | 'disabled'
> & {
  rows: ReturnType<typeof effortRows>;
  /** Names the levels where no submenu trigger above them does. */
  title?: string;
  noteId: string;
}) {
  const { t } = useTranslation();
  const { levels, showCosts, speeds, pick } = rows;
  const header = 'pb-1 pt-1.5 text-xs font-normal text-(--color-text-tertiary)';
  const mark = (on: boolean) => (
    <Check aria-hidden className={cn('h-4 w-4 flex-none text-(--color-accent-primary)', !on && 'invisible')} />
  );

  return (
    <>
      {levels.length > 0 && (
        <>
          {(title || showCosts) && (
            <DropdownMenuLabel className={cn(header, 'flex', title ? 'justify-between gap-2' : 'justify-end')}>
              {title}
              {showCosts && <TaskCostHeading />}
            </DropdownMenuLabel>
          )}
          <DropdownMenuRadioGroup value={reasoningEffort ?? ''}>
            {levels.map((row) => (
              <DropdownMenuRadioItem
                key={row.level}
                value={row.level}
                disabled={disabled}
                onSelect={(e) => { e.preventDefault(); pick(row.level); }}
              >
                <span className="flex min-w-0 flex-1 items-center gap-2">
                  {row.label}
                  {row.tag && <NameTag>{row.tag}</NameTag>}
                </span>
                <TaskCostFigure cost={row.cost ?? null} />
                {mark(row.level === reasoningEffort)}
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
        </>
      )}
      {speeds.length > 0 && (
        <>
          {levels.length > 0 && <DropdownMenuSeparator />}
          <DropdownMenuLabel className={header}>{t('chat.modelSelector.speed')}</DropdownMenuLabel>
          <DropdownMenuRadioGroup value={fastMode === null ? '' : fastMode ? 'fast' : 'standard'}>
            {speeds.map((speed) => (
              <DropdownMenuRadioItem
                key={String(speed.fast)}
                value={speed.fast ? 'fast' : 'standard'}
                disabled={disabled}
                onSelect={(e) => { e.preventDefault(); onFastModeChange(speed.fast); }}
              >
                <span className="flex min-w-0 flex-1 flex-col">
                  {speed.label}
                  {speed.hint && (
                    <span className="text-[0.6875rem] leading-snug text-(--color-text-tertiary)">{speed.hint}</span>
                  )}
                </span>
                {mark(fastMode === speed.fast)}
              </DropdownMenuRadioItem>
            ))}
          </DropdownMenuRadioGroup>
        </>
      )}
      {showCosts && <TaskCostNote id={noteId} />}
    </>
  );
}

/** The effort pill folded into ⋯, as a submenu there. */
export function EffortSubmenu(props: EffortMenuProps) {
  const { effortLabel, fastMode, isCodexModel, disabled = false } = props;
  const { t } = useTranslation();
  const noteId = useId();
  const rows = effortRows(props, t);

  return (
    <DropdownMenuSub>
      <DropdownMenuSubTrigger disabled={disabled}>
        <Gauge className="h-4 w-4 flex-none" />
        <span>{t('chat.modelSelector.reasoningEffort')}</span>
        <span className="ml-auto inline-flex items-center gap-1 text-(--color-text-tertiary)">
          {effortLabel}
          {fastMode === true && isCodexModel && (
            <Rocket className="h-3 w-3" style={{ color: 'var(--color-accent-light)' }} />
          )}
        </span>
      </DropdownMenuSubTrigger>
      <DropdownMenuSubContent
        className="w-60"
        aria-describedby={rows.showCosts ? noteId : undefined}
        onClick={keepMenuOpen}
      >
        <EffortOverflowRows
          rows={rows}
          noteId={noteId}
          reasoningEffort={props.reasoningEffort}
          fastMode={fastMode}
          onFastModeChange={props.onFastModeChange}
          disabled={disabled}
        />
      </DropdownMenuSubContent>
    </DropdownMenuSub>
  );
}

/**
 * The effort pill folded into ⋯ on a phone, its levels and speeds listed in
 * ⋯ itself: a phone has no room beside ⋯ for a submenu to open into.
 */
export function EffortMenuSection(props: EffortMenuProps) {
  const { t } = useTranslation();
  const noteId = useId();
  const rows = effortRows(props, t);
  const title = t('chat.modelSelector.reasoningEffort');

  return (
    <div
      role="group"
      aria-label={title}
      aria-describedby={rows.showCosts ? noteId : undefined}
      onClick={keepMenuOpen}
    >
      <EffortOverflowRows
        rows={rows}
        title={title}
        noteId={noteId}
        reasoningEffort={props.reasoningEffort}
        fastMode={props.fastMode}
        onFastModeChange={props.onFastModeChange}
        disabled={props.disabled}
      />
    </div>
  );
}

/** One word in the effort menu, in the model picker's row. */
function SettingOption({ id, label, tag, hint, cost, onAction }: {
  id: string;
  label: string;
  tag?: string;
  hint?: string;
  cost?: TaskCost;
  onAction?: () => void;
}) {
  return (
    <MenuItem id={id} textValue={label} onAction={onAction} className={MENU_ROW}>
      {({ isSelected }) => (
        <>
          <span className="flex min-w-0 flex-1 flex-col">
            <span className="flex items-center gap-2">
              {label}
              {tag && <NameTag>{tag}</NameTag>}
            </span>
            {hint && <span className="text-[0.6875rem] leading-snug text-(--color-text-tertiary)">{hint}</span>}
          </span>
          <TaskCostFigure cost={cost ?? null} />
          <Check
            aria-hidden
            className={cn('h-4 w-4 flex-none text-(--color-accent-primary)', !isSelected && 'invisible')}
          />
        </>
      )}
    </MenuItem>
  );
}
