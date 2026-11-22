import { useEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import { onModelPickerRequest } from '@/lib/modelPickerRequest';
import { Zap } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { NoticeHeader, NoticePopover, type ComposerNotice } from './chat-input.notices';
import { getModelDisplayName } from './chat-input.helpers';
import { ModelPicker, type ModelListProps } from './chat-input.modelPicker';
import type { ModelTuning } from './chat-input.modelTuning';
import { EFFORT_LABELS, effortLabelFor, isCodexModel, tuningMenuLevels } from '@/lib/modelTuning';
import type { ModelMetadataEntry } from '@/hooks/useFilteredModels';
import { MarkedModelName } from '@/components/model/ModelMark';
import { llmProviderArt } from '@/lib/brandArt';
import { cn } from '@/lib/utils';

type ModelMetadata = Record<string, ModelMetadataEntry>;

/** Pill geometry, shared with the measure chips so the fold budget can
 *  never drift from what actually renders. */
const TRIGGER_CLASS = 'inline-flex min-w-0 items-center gap-1 rounded-lg py-1.5 px-2.5 text-[0.8125rem] font-medium border border-transparent whitespace-nowrap';

/** The least of the model name the pill shows: two characters and the
 *  ellipsis ("V4…"). Below it the name gives way to the logo alone, where
 *  the model has one. */
const NAME_FLOOR = 'max-w-8';

/** What the pill says of the model's tuning: its level, quieter than the
 *  name, and a bolt while a Codex model runs fast. */
function TuningReadout({ level, fast }: { level: ReactNode; fast: boolean }) {
  const { t } = useTranslation();
  return (
    <>
      {level && <span className="flex-none font-normal text-(--color-text-tertiary)">{level}</span>}
      {fast && <Zap aria-hidden className="h-3 w-3 flex-none" style={{ color: 'var(--color-accent-light)' }} />}
      {fast && <span className="sr-only">{t('chat.modelSelector.speedFast')}</span>}
    </>
  );
}

function ModelTriggerBody({ selectedModel, metadata, floor = false, nameHidden = false, level = null, fast = false }: {
  selectedModel: string | null;
  metadata?: ModelMetadata;
  floor?: boolean;
  /** Logo only. The name stays for a screen reader, which reads it as the
   *  pill's name. A model with no logo keeps the name at its floor, since an
   *  empty pill would not say which model is picked. */
  nameHidden?: boolean;
  level?: ReactNode;
  fast?: boolean;
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
      <TuningReadout level={level} fast={fast} />
    </>
  );
}

/**
 * Measure-row twin of the model pill: same geometry, no interactivity. The
 * whole twin holds the model's longest level and the bolt a Codex model can
 * show, so a pick in the open picker never moves the fold under it; it holds
 * the level shown now too, which may be one the ladder no longer lists. The
 * `floor` twin is the least the toolbar keeps before the name goes: no level,
 * and the name cut to its floor.
 */
export function ModelTriggerMeasure({ selectedModel, metadata, floor = false, reasoningEffort = null }: {
  selectedModel: string | null;
  metadata?: ModelMetadata;
  floor?: boolean;
  reasoningEffort?: string | null;
}) {
  const { t } = useTranslation();
  const meta = selectedModel ? metadata?.[selectedModel] : undefined;
  const ladder = tuningMenuLevels(meta?.reasoning_efforts ?? []);
  const shown = effortLabelFor(t, reasoningEffort);
  const labels = floor || ladder.length === 0 ? [] : [...new Set([
    ...ladder.map((level) => t(EFFORT_LABELS[level])),
    ...(shown ? [shown] : []),
  ])];
  return (
    <button type="button" tabIndex={-1} className={TRIGGER_CLASS}>
      <ModelTriggerBody
        selectedModel={selectedModel}
        metadata={metadata}
        floor={floor}
        // Stacked in one cell, the labels take the widest one's room.
        level={labels.length > 0 && (
          <span className="inline-grid">
            {labels.map((label) => <span key={label} className="col-start-1 row-start-1">{label}</span>)}
          </span>
        )}
        fast={!floor && isCodexModel(meta)}
      />
    </button>
  );
}

/** A pill in the composer's toolbar. `data-pill` names it for tests, which
 *  should not lean on its translated title. */
function Pill({ buttonRef, open, popup, onPress, title, disabled, warning, children }: {
  buttonRef?: RefObject<HTMLButtonElement | null>;
  open?: boolean;
  /** What it opens; none for a pill that goes somewhere instead. */
  popup?: 'dialog';
  onPress: () => void;
  title?: string;
  disabled?: boolean;
  /** A warning about the next send on this pill's model: it tints the pill
   *  and marks it, and a screen reader hears the sentence with the name. */
  warning?: string;
  children: ReactNode;
}) {
  return (
    <button
      ref={buttonRef}
      type="button"
      data-pill="model"
      data-notice={warning ? 'warning' : undefined}
      className={cn('model-selector-trigger', TRIGGER_CLASS, 'relative cursor-pointer transition-colors disabled:cursor-progress')}
      onClick={(e) => { e.stopPropagation(); onPress(); }}
      disabled={disabled}
      title={title}
      aria-haspopup={popup}
      aria-expanded={popup ? open : undefined}
      data-state={popup ? (open ? 'open' : 'closed') : undefined}
    >
      {children}
      {warning && (
        <>
          {/* Ringed by the composer's own surface, so it reads over the tint's edge. */}
          <span
            aria-hidden
            className="absolute -right-0.5 -top-0.5 h-2 w-2 rounded-full bg-(--color-warning) ring-2 ring-(--color-bg-card)"
          />
          <span className="sr-only">{warning}</span>
        </>
      )}
    </button>
  );
}

/**
 * The model pill and the picker it opens. The pill names the model and its
 * reasoning level, and the picker chooses both: the models in a list, the
 * current one's levels and speed beside or under it. With nothing to pick and
 * nothing selected, no model is set up yet, and the pill becomes the way to
 * set one up rather than leaving a composer with no hint of why it cannot
 * send.
 */
export function ChatInputModelMenu({
  onSelectModel, dropdownDirection, container, draftRef, disabled = false, nameHidden = false,
  levelHidden = false, notice = null, tuning, ...list
}: ModelListProps & {
  onSelectModel: (model: string) => void;
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
  /** The toolbar has no room for the level beside the name (`computeFold`). */
  levelHidden?: boolean;
  /** The current model's level and speed, which the pill names and the
   *  picker sets. Without it the picker chooses a model alone. */
  tuning?: ModelTuning;
  /** What the host says about the next send on this model. A warning stays on
   *  the pill and heads the picker until its condition clears; either kind
   *  opens beside the pill once on its own. */
  notice?: ComposerNotice | null;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const { selectedModel, metadata, groups } = list;
  const meta = selectedModel ? metadata?.[selectedModel] : undefined;
  const [open, setOpen] = useState(false);
  const warning = notice?.tone === 'warning' ? notice : null;
  const triggerRef = useRef<HTMLButtonElement>(null);
  const toSettings = () => navigate('/settings?tab=model');
  // A refusal's "Choose a model" opens the picker of the composer on screen.
  useEffect(() => onModelPickerRequest(() => {
    if (!disabled && triggerRef.current?.offsetParent) setOpen(true);
  }), [disabled]);

  if (groups.length === 0 && !selectedModel) {
    if (disabled) return null;
    return (
      <Pill onPress={toSettings}>
        <span style={{ color: 'var(--color-text-secondary)' }}>{t('chat.modelSelector.setUpModel')}</span>
      </Pill>
    );
  }

  return (
    <>
      <Pill
        buttonRef={triggerRef}
        open={open}
        popup="dialog"
        onPress={() => setOpen((v) => !v)}
        title={t('chat.modelSelector.select')}
        disabled={disabled}
        warning={warning?.text}
      >
        <ModelTriggerBody
          selectedModel={selectedModel}
          metadata={metadata}
          nameHidden={nameHidden}
          // The level shows wherever the model has a ladder, the default
          // included, so the pill always says what a send runs.
          level={!levelHidden && tuning && tuningMenuLevels(meta?.reasoning_efforts ?? []).length > 0
            ? effortLabelFor(t, tuning.reasoningEffort)
            : null}
          fast={!levelHidden && isCodexModel(meta) && tuning?.fastMode === true}
        />
      </Pill>
      <NoticePopover notice={notice} anchorRef={triggerRef} blocked={open} side={dropdownDirection === 'down' ? 'bottom' : 'top'} />
      <ModelPicker
        notice={warning && <NoticeHeader notice={warning} onClose={() => setOpen(false)} />}
        triggerRef={triggerRef}
        isOpen={open}
        onOpenChange={setOpen}
        direction={dropdownDirection}
        container={container}
        draftRef={draftRef}
        {...list}
        onPick={onSelectModel}
        onManage={toSettings}
        tuning={tuning}
        disabled={disabled}
      />
    </>
  );
}
