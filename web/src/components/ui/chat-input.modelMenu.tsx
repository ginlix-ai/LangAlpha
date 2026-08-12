import { useState, type RefObject } from 'react';
import {
  Brain, Check, ChevronDown, ChevronRight, CircleHelp, Flame, Rocket, Sparkles, Zap,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import {
  DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem,
  DropdownMenuSeparator, DropdownMenuSub, DropdownMenuSubTrigger, DropdownMenuSubContent,
} from './dropdown-menu';
import { useIsMobile } from '@/hooks/useIsMobile';
import { getModelDisplayName } from './chat-input.helpers';
import { derivePrimaryModels } from './chat-input.models';

/** Pill geometry for the model trigger — shared with the measure chip so the
 *  fold budget can never drift from what actually renders. */
const TRIGGER_CLASS = 'inline-flex min-w-0 items-center gap-1 rounded-full py-1.5 px-2.5 text-[0.8125rem] font-medium border border-transparent whitespace-nowrap';

function TriggerBody({
  selectedModel, fastMode, isCodexModel, accent,
}: {
  selectedModel: string | null;
  fastMode: boolean;
  isCodexModel: boolean;
  accent?: boolean;
}) {
  return (
    <>
      <span className="min-w-0 max-w-[120px] truncate">{getModelDisplayName(selectedModel) || 'Model'}</span>
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
  fastMode: boolean;
  isCodexModel: boolean;
}) {
  return <button type="button" tabIndex={-1} className={TRIGGER_CLASS}><TriggerBody {...props} /></button>;
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
  onReasoningEffortChange,
  fastMode,
  onFastModeChange,
  isCodexModel,
  supportsXhigh,
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
  onReasoningEffortChange: (effort: string | null) => void;
  fastMode: boolean;
  onFastModeChange: (fast: boolean) => void;
  isCodexModel: boolean;
  supportsXhigh: boolean;
  dropdownDirection: 'up' | 'down';
  /** Mobile portals into the composer so the menu can't escape the sheet. */
  containerRef: RefObject<HTMLElement | null>;
}) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const [menuOpen, setMenuOpen] = useState(false);
  const [showMoreModels, setShowMoreModels] = useState(false);

  // Show if there's anything to pick (quick-access list or current selection),
  // or if the user has stars to manage even when they all filter out (keeps
  // the settings link reachable).
  if (moreModelsItems.length === 0 && !hasStarredModels && !selectedModel) return null;

  const primaryModels = derivePrimaryModels({ selectedModel, threadModels, validModelNames });

  return (
    <DropdownMenu modal={false} open={menuOpen} onOpenChange={(open) => { setMenuOpen(open); if (!open) setShowMoreModels(false); }}>
      <DropdownMenuTrigger asChild>
        <button
          className={`model-selector-trigger ${TRIGGER_CLASS} cursor-pointer transition-colors outline-none`}
          onClick={(e) => e.stopPropagation()}
          type="button"
          title="Select model"
        >
          <TriggerBody selectedModel={selectedModel} fastMode={fastMode} isCodexModel={isCodexModel} accent />
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
          <DropdownMenuItem
            key={m}
            onSelect={() => onSelectModel(m)}
            className="text-[0.8125rem] justify-between"
            style={{ color: 'var(--color-text-primary)' }}
          >
            <span>{getModelDisplayName(m)}</span>
            {m === selectedModel && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator style={{ backgroundColor: 'var(--color-border-muted)' }} />
        {/* Reasoning effort (custom control — not a menu item, won't close on click) */}
        <div className="model-effort-section">
          <span className="model-effort-label">{t('chat.modelSelector.reasoningEffort')}</span>
          <div className="model-effort-toggle">
            {([
              ['low', Zap, t('chat.modelSelector.effortLow')],
              ['medium', Brain, t('chat.modelSelector.effortMedium')],
              ['high', Flame, t('chat.modelSelector.effortHigh')],
              ...(supportsXhigh ? [['xhigh', Sparkles, t('chat.modelSelector.effortXhigh')]] as const : []),
            ] as const).map(([level, Icon, label]) => (
              <button
                key={level}
                className={`model-effort-btn ${level === reasoningEffort ? 'active' : ''}`}
                onMouseDown={(e) => {
                  e.preventDefault();
                  onReasoningEffortChange(level === reasoningEffort ? null : level);
                }}
                title={label}
              >
                <Icon className="h-3.5 w-3.5" />
              </button>
            ))}
          </div>
        </div>
        {/* Fast mode (Codex only — custom control, won't close on click) */}
        {isCodexModel && (
          <div className="model-effort-section">
            <span className="model-effort-label">
              <Rocket className="h-3.5 w-3.5" style={{ marginRight: 4, verticalAlign: '-2px', display: 'inline' }} />
              {t('chat.modelSelector.fastMode')}
              <span className="fast-mode-help-wrap">
                <CircleHelp className="fast-mode-help-icon" />
                <span className="fast-mode-help-tooltip">{t('chat.modelSelector.fastModeHelpLine1')}<br />{t('chat.modelSelector.fastModeHelpLine2')}</span>
              </span>
            </span>
            <button
              className={`fast-mode-switch ${fastMode ? 'active' : ''}`}
              onMouseDown={(e) => { e.preventDefault(); onFastModeChange(!fastMode); }}
              title={t('chat.modelSelector.fastModeDesc')}
              role="switch"
              aria-checked={fastMode}
            >
              <span className="fast-mode-switch-thumb" />
            </button>
          </div>
        )}
        <DropdownMenuSeparator style={{ backgroundColor: 'var(--color-border-muted)' }} />
        {/* More models — desktop: sub-menu flyout, mobile: inline toggle with onMouseDown (same pattern as effort buttons) */}
        {isMobile ? (
          <>
            {/* Toggle trigger — onMouseDown fires after pointerup on mobile, avoiding Radix's pointer sequence race */}
            <div
              className="flex items-center justify-between px-3 py-2 text-xs cursor-pointer rounded-sm transition-colors hover:bg-accent/15"
              style={{ color: 'var(--color-text-tertiary)' }}
              role="menuitem"
              onMouseDown={(e) => {
                e.preventDefault();
                setShowMoreModels((v) => !v);
              }}
            >
              <span>{t('chat.modelSelector.moreModels')}</span>
              <ChevronRight className={`h-3.5 w-3.5 transition-transform ${showMoreModels ? 'rotate-90' : ''}`} />
            </div>
            {/* Model items — plain divs (not DropdownMenuItems) to avoid Collection registration changes */}
            <div className={showMoreModels ? '' : 'hidden'}>
              {moreModelsItems.length > 0 ? (
                moreModelsItems.map((m) => (
                  <div
                    key={m}
                    className="relative flex w-full cursor-default select-none items-center justify-between rounded-sm px-3 py-2 pl-6 text-[0.8125rem] outline-none transition-colors hover:bg-accent/15"
                    style={{ color: 'var(--color-text-primary)' }}
                    role="menuitem"
                    onMouseDown={(e) => {
                      e.preventDefault();
                      onSelectModel(m);
                      setMenuOpen(false);
                    }}
                  >
                    <span>{getModelDisplayName(m)}</span>
                    {m === selectedModel && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                  </div>
                ))
              ) : (
                <div
                  className="relative flex w-full cursor-default select-none items-center rounded-sm px-3 py-2 pl-6 text-xs outline-none transition-colors hover:bg-accent/15"
                  style={{ color: 'var(--color-text-tertiary)' }}
                  role="menuitem"
                  onMouseDown={(e) => {
                    e.preventDefault();
                    navigate('/settings?tab=model');
                    setMenuOpen(false);
                  }}
                >
                  {t('chat.modelSelector.configureModels')}
                </div>
              )}
            </div>
          </>
        ) : (
          <DropdownMenuSub>
            <DropdownMenuSubTrigger
              className="text-xs justify-between"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <span className="flex-1">{t('chat.modelSelector.moreModels')}</span>
              <ChevronRight className="h-3.5 w-3.5" />
            </DropdownMenuSubTrigger>
            <DropdownMenuSubContent
              className="w-60"
              data-click-outside-ignore
              collisionPadding={{ top: 8, right: 8, bottom: 60, left: 8 }}
              style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-muted)' }}
            >
              {moreModelsItems.length > 0 ? (
                moreModelsItems.map((m) => (
                  <DropdownMenuItem
                    key={m}
                    onSelect={() => onSelectModel(m)}
                    className="text-[0.8125rem] justify-between"
                    style={{ color: 'var(--color-text-primary)' }}
                  >
                    <span>{getModelDisplayName(m)}</span>
                    {m === selectedModel && <Check className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />}
                  </DropdownMenuItem>
                ))
              ) : (
                <DropdownMenuItem
                  onSelect={() => navigate('/settings?tab=model')}
                  className="text-xs"
                  style={{ color: 'var(--color-text-tertiary)' }}
                >
                  {t('chat.modelSelector.configureModels')}
                </DropdownMenuItem>
              )}
            </DropdownMenuSubContent>
          </DropdownMenuSub>
        )}
        {isMobile ? (
          <div
            className="relative flex w-full cursor-default select-none items-center rounded-sm px-3 py-2 text-xs outline-none transition-colors hover:bg-accent/15"
            style={{ color: 'var(--color-text-tertiary)' }}
            role="menuitem"
            onMouseDown={(e) => {
              e.preventDefault();
              navigate('/settings?tab=model');
              setMenuOpen(false);
            }}
          >
            {t('chat.modelSelector.manageModels')}
          </div>
        ) : (
          <DropdownMenuItem
            onSelect={() => navigate('/settings?tab=model')}
            className="text-xs"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('chat.modelSelector.manageModels')}
          </DropdownMenuItem>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
