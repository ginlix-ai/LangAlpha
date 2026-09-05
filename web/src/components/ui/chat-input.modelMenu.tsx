import { useState, type ReactNode, type RefObject } from 'react';
import { Check, ChevronDown, ChevronRight, Rocket } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import {
  DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuSeparator, DropdownMenuSub, DropdownMenuSubTrigger, DropdownMenuSubContent,
} from './dropdown-menu';
import { useIsMobile } from '@/hooks/useIsMobile';
import { getModelDisplayName } from './chat-input.helpers';
import { EFFORT_LABELS, effortLabelFor } from '@/lib/modelTuning';
import { derivePrimaryModels } from './chat-input.models';

/** Pill geometry for the model trigger — shared with the measure chip so the
 *  fold budget can never drift from what actually renders. */
const TRIGGER_CLASS = 'inline-flex min-w-0 items-center gap-1 rounded-full py-1.5 px-2.5 text-[0.8125rem] font-medium border border-transparent whitespace-nowrap';

function TriggerBody({
  selectedModel, effortLabel, fastMode, isCodexModel, accent,
}: {
  selectedModel: string | null;
  /** Already-translated effective level, or null when the model has no ladder. */
  effortLabel: string | null;
  fastMode: boolean;
  isCodexModel: boolean;
  accent?: boolean;
}) {
  return (
    <>
      <span className="min-w-0 max-w-[120px] truncate">{getModelDisplayName(selectedModel) || 'Model'}</span>
      {effortLabel && (
        <span className="flex-none" style={{ color: 'var(--color-text-tertiary)' }}>{effortLabel}</span>
      )}
      {fastMode && isCodexModel && (
        <Rocket className="h-3 w-3 flex-none" style={accent ? { color: 'var(--color-accent-light)' } : undefined} />
      )}
      <ChevronDown className="h-3 w-3 flex-none" />
    </>
  );
}

/** Measure-row twin of the model trigger: same geometry, no interactivity. */
export function ModelTriggerMeasure(props: {
  selectedModel: string | null;
  effortLabel: string | null;
  fastMode: boolean;
  isCodexModel: boolean;
}) {
  return <button type="button" tabIndex={-1} className={TRIGGER_CLASS}><TriggerBody {...props} /></button>;
}

/** One "Label … value ›" row and the list it opens. Radix submenus lose a tap
 *  on mobile, so there the row expands its options in place instead of flying
 *  them out; both shapes are real menu items, which is what keeps roving focus
 *  and Enter working on either path. */
/** The menu speaks one size. A link that leaves the menu is quieter than a
 *  setting row by tone, not by scale, so it matches rather than shrinks. */
const NAV_ITEM = 'text-[0.8125rem]';

function SettingRow({
  label, value, expanded, onToggle, children, isMobile, quiet = false, contentClass = 'w-56',
}: {
  label: string;
  /** Omitted by a row that opens a list rather than holding a setting. */
  value?: string;
  expanded: boolean;
  onToggle: () => void;
  children: ReactNode;
  isMobile: boolean;
  /** Navigation rows sit below the separator and read quieter than the
   *  settings above it. */
  quiet?: boolean;
  /** Flyout width, since model names need more room than effort words. */
  contentClass?: string;
}) {
  const tone = { color: quiet ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)' };
  const body = (
    <>
      <span>{label}</span>
      {value !== undefined && <span className="model-setting-value">{value}</span>}
    </>
  );

  if (isMobile) {
    return (
      <>
        <DropdownMenuItem
          variant="setting"
          style={tone}
          aria-expanded={expanded}
          onSelect={(e) => { e.preventDefault(); onToggle(); }}
        >
          {body}
          <ChevronRight className={`h-3.5 w-3.5 flex-none transition-transform ${expanded ? 'rotate-90' : ''}`} />
        </DropdownMenuItem>
        {expanded && <div className="model-setting-options">{children}</div>}
      </>
    );
  }
  return (
    <DropdownMenuSub>
      <DropdownMenuSubTrigger variant="setting" style={tone}>
        {body}
        <ChevronRight className="h-3.5 w-3.5 flex-none" />
      </DropdownMenuSubTrigger>
      <DropdownMenuSubContent
        className={contentClass}
        data-click-outside-ignore
        collisionPadding={{ top: 8, right: 8, bottom: 60, left: 8 }}
        style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
      >
        {children}
      </DropdownMenuSubContent>
    </DropdownMenuSub>
  );
}

/** One selectable word in a setting's option list. Never closes the menu:
 *  picking an effort is a setting, not a send. */
function SettingOption({
  label, badge, hint, selected, onPick,
}: {
  label: string;
  badge?: string;
  hint?: string;
  selected: boolean;
  onPick: () => void;
}) {
  return (
    <DropdownMenuItem
      variant="setting"
      style={{ color: 'var(--color-text-primary)' }}
      role="menuitemradio"
      aria-checked={selected}
      onSelect={(e) => { e.preventDefault(); onPick(); }}
    >
      <div className="min-w-0">
        <span className="model-setting-option-label">
          {label}
          {badge && <span className="model-setting-badge">{badge}</span>}
        </span>
        {hint && <span className="model-setting-hint">{hint}</span>}
      </div>
      {selected && <Check className="h-4 w-4 flex-none" style={{ color: 'var(--color-accent-primary)' }} />}
    </DropdownMenuItem>
  );
}

/** One model in a picker list. Unlike a setting option, choosing a model is a
 *  commitment rather than an adjustment, so the menu closes behind it. */
function ModelOption({ model, selected, onPick }: {
  model: string;
  selected: boolean;
  onPick: () => void;
}) {
  return (
    <DropdownMenuItem variant="setting" onSelect={onPick} style={{ color: 'var(--color-text-primary)' }}>
      <span>{getModelDisplayName(model)}</span>
      {selected && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
    </DropdownMenuItem>
  );
}

/**
 * Model selector: current + thread models, per-model reasoning effort and
 * Codex fast mode, then quick-access models (desktop flyout / mobile inline)
 * and the settings escape hatch. Renders nothing when there is nothing to pick
 * and no stars to manage.
 */
export function ChatInputModelMenu({
  selectedModel,
  onSelectModel,
  threadModels,
  validModelNames,
  moreModelsItems,
  hasStarredModels,
  reasoningEffort,
  inheritedEffort,
  onReasoningEffortChange,
  fastMode,
  onFastModeChange,
  isCodexModel,
  reasoningEfforts,
  dropdownDirection,
  containerRef,
}: {
  selectedModel: string | null;
  onSelectModel: (model: string) => void;
  threadModels: string[];
  /** Gates thread history — a model can be revoked after a turn used it. */
  validModelNames: Set<string>;
  moreModelsItems: string[];
  hasStarredModels: boolean;
  reasoningEffort: string | null;
  /** What this model resolves to with no override — account-wide value, else
   *  the manifest default. Carries the "Default" badge, and picking it clears
   *  the override rather than pinning the same value. */
  inheritedEffort: string | null;
  onReasoningEffortChange: (effort: string | null) => void;
  fastMode: boolean;
  onFastModeChange: (fast: boolean) => void;
  isCodexModel: boolean;
  /** Exactly the levels this model honors, weakest first. Empty renders no
   *  selector at all — a model with no reasoning control used to show the same
   *  row of buttons as every other, all emitting an identical request. */
  reasoningEfforts: string[];
  dropdownDirection: 'up' | 'down';
  /** Mobile portals into the composer so the menu can't escape the sheet. */
  containerRef: RefObject<HTMLElement | null>;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const [menuOpen, setMenuOpen] = useState(false);
  const [showMoreModels, setShowMoreModels] = useState(false);
  const [expanded, setExpanded] = useState<'effort' | 'speed' | null>(null);

  const effectiveEffort = reasoningEffort ?? inheritedEffort;
  const effortLabel = effortLabelFor(t, effectiveEffort);

  // Show if there's anything to pick (quick-access list or current selection),
  // or if the user has stars to manage even when they all filter out (keeps
  // the settings link reachable).
  if (moreModelsItems.length === 0 && !hasStarredModels && !selectedModel) return null;

  const primaryModels = derivePrimaryModels({ selectedModel, threadModels, validModelNames });

  return (
    <DropdownMenu modal={false} open={menuOpen} onOpenChange={(open) => { setMenuOpen(open); if (!open) { setShowMoreModels(false); setExpanded(null); } }}>
      <DropdownMenuTrigger asChild>
        <button
          className={`model-selector-trigger ${TRIGGER_CLASS} cursor-pointer transition-colors`}
          onClick={(e) => e.stopPropagation()}
          type="button"
          title="Select model"
        >
          <TriggerBody selectedModel={selectedModel} effortLabel={effortLabel} fastMode={fastMode} isCodexModel={isCodexModel} accent />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="end"
        side={dropdownDirection === 'down' ? 'bottom' : 'top'}
        className="w-60"
        container={isMobile ? containerRef.current : undefined}
        data-click-outside-ignore
        onClick={(e) => e.stopPropagation()}
        style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
      >
        {/* Thread models */}
        {primaryModels.map((m) => (
          <ModelOption key={m} model={m} selected={m === selectedModel} onPick={() => onSelectModel(m)} />
        ))}
        {(reasoningEfforts.length > 0 || isCodexModel) && (
          <DropdownMenuSeparator style={{ backgroundColor: 'var(--color-border-muted)' }} />
        )}
        {reasoningEfforts.length > 0 && (
          <SettingRow
            label={t('chat.modelSelector.reasoningEffort')}
            value={effortLabel ?? ''}
            expanded={expanded === 'effort'}
            onToggle={() => setExpanded((v) => (v === 'effort' ? null : 'effort'))}
            isMobile={isMobile}
          >
            <div className="model-setting-caption">{t('chat.modelSelector.effortHint')}</div>
            {reasoningEfforts.map((level) => (
              EFFORT_LABELS[level] ? (
                <SettingOption
                  key={level}
                  label={t(EFFORT_LABELS[level])}
                  badge={level === inheritedEffort ? t('chat.modelSelector.defaultBadge') : undefined}
                  selected={level === effectiveEffort}
                  // Picking the inherited level clears the override instead of
                  // pinning today's default as a permanent choice.
                  onPick={() => onReasoningEffortChange(level === inheritedEffort ? null : level)}
                />
              ) : null
            ))}
          </SettingRow>
        )}
        {/* Speed (Codex only — priority service tier) */}
        {isCodexModel && (
          <SettingRow
            label={t('chat.modelSelector.speed')}
            value={t(fastMode ? 'chat.modelSelector.speedFast' : 'chat.modelSelector.speedStandard')}
            expanded={expanded === 'speed'}
            onToggle={() => setExpanded((v) => (v === 'speed' ? null : 'speed'))}
            isMobile={isMobile}
          >
            <SettingOption
              label={t('chat.modelSelector.speedStandard')}
              selected={!fastMode}
              onPick={() => onFastModeChange(false)}
            />
            <SettingOption
              label={t('chat.modelSelector.speedFast')}
              hint={t('chat.modelSelector.speedFastHint')}
              selected={fastMode}
              onPick={() => onFastModeChange(true)}
            />
          </SettingRow>
        )}
        <DropdownMenuSeparator style={{ backgroundColor: 'var(--color-border-muted)' }} />
        {/* More models: the same row shape as the settings above, so the
            desktop flyout and the mobile inline list come from one place. */}
        <SettingRow
          label={t('chat.modelSelector.moreModels')}
          expanded={showMoreModels}
          onToggle={() => setShowMoreModels((v) => !v)}
          isMobile={isMobile}
          quiet
          contentClass="w-60"
        >
          {moreModelsItems.length > 0 ? (
            moreModelsItems.map((m) => (
              <ModelOption key={m} model={m} selected={m === selectedModel} onPick={() => onSelectModel(m)} />
            ))
          ) : (
            <DropdownMenuItem
              onSelect={() => navigate('/settings?tab=model')}
              className={NAV_ITEM}
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {t('chat.modelSelector.configureModels')}
            </DropdownMenuItem>
          )}
        </SettingRow>
        <DropdownMenuItem
          onSelect={() => navigate('/settings?tab=model')}
          className={NAV_ITEM}
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('chat.modelSelector.manageModels')}
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
