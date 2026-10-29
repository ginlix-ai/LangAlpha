import { useId, useRef, useState, type ReactNode, type RefObject } from 'react';
import { Check, ChevronDown, Info, Rocket } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { Header, Menu, MenuItem, MenuSection, type Selection } from 'react-aria-components';
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from './tooltip';
import {
  ComposerPopover, MENU_ROW, MENU_SECTION, useFocusCheckedRow,
} from './chat-input.composerPopover';
import { getModelDisplayName } from './chat-input.helpers';
import { ModelPicker, type ModelRowStatus } from './chat-input.modelPicker';
import { EFFORT_LABELS, hasTuningMenu, tuningMenuLevels } from '@/lib/modelTuning';
import type { ModelMetadataEntry, TaskCost } from '@/hooks/useFilteredModels';
import type { ModelGroup } from '@/lib/modelGroups';
import type { Payer } from '@/lib/modelPayer';
import { MarkedModelName } from '@/components/model/ModelMark';
import { NameTag } from '@/components/model/PayerBadge';
import { TaskCostFigure } from '@/components/model/TaskCost';
import { costsRelativeTo } from '@/lib/taskCost';
import { cn } from '@/lib/utils';

type ModelMetadata = Record<string, ModelMetadataEntry>;

/** Pill geometry for the model and effort triggers, shared with the measure
 *  chips so the fold budget can never drift from what actually renders. */
const TRIGGER_CLASS = 'inline-flex min-w-0 items-center gap-1 rounded-lg py-1.5 px-2.5 text-[0.8125rem] font-medium border border-transparent whitespace-nowrap';

function ModelTriggerBody({ selectedModel, metadata }: { selectedModel: string | null; metadata?: ModelMetadata }) {
  const { t } = useTranslation();
  return (
    <>
      <MarkedModelName
        provider={selectedModel ? metadata?.[selectedModel]?.provider : undefined}
        name={getModelDisplayName(selectedModel, metadata) || t('chat.modelSelector.fallbackName')}
        nameClassName="min-w-0 max-w-[120px] truncate"
        short
      />
      <ChevronDown className="h-3 w-3 flex-none" />
    </>
  );
}

function EffortTriggerBody({ effortLabel, fast, accent }: {
  effortLabel: string | null;
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

/** Measure-row twin of the model and effort triggers: same geometry, no
 *  interactivity. The effort chip is left out where the composer hides it. */
export function ModelTriggerMeasure({ selectedModel, metadata, effortLabel, fast, showEffort }: {
  selectedModel: string | null;
  metadata?: ModelMetadata;
  effortLabel: string | null;
  fast: boolean;
  showEffort: boolean;
}) {
  return (
    <span className="inline-flex items-center gap-1">
      <button type="button" tabIndex={-1} className={TRIGGER_CLASS}>
        <ModelTriggerBody selectedModel={selectedModel} metadata={metadata} />
      </button>
      {showEffort && (
        <button type="button" tabIndex={-1} className={TRIGGER_CLASS}>
          <EffortTriggerBody effortLabel={effortLabel} fast={fast} />
        </button>
      )}
    </span>
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
      className={cn('model-selector-trigger', TRIGGER_CLASS, 'cursor-pointer transition-colors disabled:cursor-progress')}
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
  rowStatus, dropdownDirection, container, draftRef, disabled = false,
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
        <ModelTriggerBody selectedModel={selectedModel} metadata={metadata} />
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

interface EffortMenuProps {
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

function EffortList({
  reasoningEffort, defaultEffort, reasoningEfforts, onReasoningEffortChange,
  fastMode, onFastModeChange, isCodexModel, taskCost,
}: EffortMenuProps) {
  const { t } = useTranslation();
  const menuRef = useRef<HTMLDivElement>(null);
  const noteId = useId();
  useFocusCheckedRow(menuRef);

  const levels = tuningMenuLevels(reasoningEfforts);
  // The level marked "Default" is 1x, so the figures compare this model's
  // levels and nothing else.
  const costs = costsRelativeTo(taskCost, defaultEffort);
  const showCosts = levels.some((level) => costs[level]);

  // Picking the level Settings gives clears the thread's own instead of
  // pinning today's default as a permanent choice. An action, not a selection
  // change: a thread pinned to the level that has since become the default
  // shows it checked, and picking a checked row changes no selection.
  const onEffort = (level: string) => onReasoningEffortChange(level === defaultEffort ? null : level);
  const onSpeed = (keys: Selection) => {
    const [key] = keys === 'all' ? [] : [...keys];
    if (key !== undefined) onFastModeChange(key === `${SPEED}fast`);
  };

  return (
    <>
      <Menu
        ref={menuRef}
        aria-label={t('chat.modelSelector.reasoningEffort')}
        // What the figures mean. The tooltip says it under a pointer, but a
        // menu's arrow keys reach only its rows, so the menu carries it too.
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
              {showCosts && (
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
              )}
            </Header>
            {levels.map((level) => (
              <SettingOption
                key={level}
                id={`${EFFORT}${level}`}
                label={t(EFFORT_LABELS[level])}
                tag={level === defaultEffort ? t('chat.modelSelector.defaultBadge') : undefined}
                cost={costs[level]}
                onAction={() => onEffort(level)}
              />
            ))}
          </MenuSection>
        )}
        {/* Speed (Codex only: priority service tier) */}
        {isCodexModel && (
          <MenuSection
            selectionMode="single"
            disallowEmptySelection
            selectedKeys={fastMode === null ? [] : [`${SPEED}${fastMode ? 'fast' : 'standard'}`]}
            onSelectionChange={onSpeed}
            className={MENU_SECTION}
          >
            <Header className={SECTION_HEADER}>{t('chat.modelSelector.speed')}</Header>
            <SettingOption id={`${SPEED}standard`} label={t('chat.modelSelector.speedStandard')} />
            <SettingOption
              id={`${SPEED}fast`}
              label={t('chat.modelSelector.speedFast')}
              hint={t('chat.modelSelector.speedFastHint')}
            />
          </MenuSection>
        )}
      </Menu>
      {/* Hidden, yet still the menu's description. */}
      {showCosts && <span id={noteId} hidden>{t('chat.modelSelector.taskCostNote')}</span>}
    </>
  );
}

// The model picker's tab header.
const SECTION_HEADER = 'px-2 pb-1 pt-1.5 text-xs text-(--color-text-tertiary)';

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
