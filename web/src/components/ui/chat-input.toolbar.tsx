import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Check, ChevronDown, FileStack, FolderOpen, Radar, ScrollText, Zap,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  DropdownMenuItem, DropdownMenuSub, DropdownMenuSubContent, DropdownMenuSubTrigger,
} from './dropdown-menu';
import { PillToggle } from './chat-input.parts';
import type { ToolbarItem } from './chat-input.useToolbarFold';
import type { Workspace } from './chat-input.types';

/**
 * The composer's foldable toolbar, declared once in PRIORITY order: the first
 * item is the last to fold, and that same order is the inline render order, so
 * folding only ever removes controls from the tail — nothing shifts position.
 *
 * Each item carries its inline render, its ⋯-menu render and a group tag;
 * separators between groups are emitted by the menu loop, never by hand.
 */
export function useToolbarItems({
  mode,
  onModeChange,
  ptcDisabledReason,
  planMode,
  setPlanMode,
  watchMode,
  setWatchMode,
  marketWatchEnabled,
  workspaces,
  selectedWorkspaceId,
  onWorkspaceChange,
}: {
  mode?: 'fast' | 'ptc';
  onModeChange?: (mode: 'fast' | 'ptc') => void;
  ptcDisabledReason?: string | null;
  planMode: boolean;
  setPlanMode: (v: boolean) => void;
  watchMode: boolean;
  setWatchMode: (v: boolean) => void;
  marketWatchEnabled: boolean;
  workspaces?: Workspace[] | null;
  selectedWorkspaceId?: string | null;
  onWorkspaceChange?: ((wsId: string) => void) | null;
}): ToolbarItem[] {
  const { t } = useTranslation();

  const [showWorkspaceMenu, setShowWorkspaceMenu] = useState(false);
  const workspaceMenuRef = useRef<HTMLDivElement>(null);
  const workspaceBtnRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!showWorkspaceMenu) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (workspaceBtnRef.current?.contains(e.target as Node)) return;
      if (workspaceMenuRef.current && !workspaceMenuRef.current.contains(e.target as Node)) {
        setShowWorkspaceMenu(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showWorkspaceMenu]);

  const hasModeToggle = mode !== undefined && onModeChange !== undefined;
  const showPlanWatchSlot = !hasModeToggle || mode === 'ptc';
  const showWorkspaceSelector = !!(hasModeToggle && mode === 'ptc' && workspaces && workspaces.length > 0);
  const ptcBlocked = mode === 'fast' && !!ptcDisabledReason;
  const selectedWorkspaceName = useMemo(() => {
    if (!workspaces || !selectedWorkspaceId) return 'Workspace';
    return workspaces.find((w) => w.workspace_id === selectedWorkspaceId)?.name || 'Workspace';
  }, [workspaces, selectedWorkspaceId]);

  return useMemo<ToolbarItem[]>(() => [
    {
      id: 'mode',
      group: 'agent',
      visible: hasModeToggle,
      inline: ({ measureOnly }) => (
        <PillToggle
          icon={mode === 'fast' ? Zap : FileStack}
          label={mode === 'fast' ? 'Flash' : 'PTC'}
          title={mode === 'fast'
            ? 'Flash — quick answer using flash model'
            : 'PTC — full agent with workspace and tools'}
          disabledReason={ptcBlocked ? ptcDisabledReason : null}
          aria="none"
          onToggle={() => onModeChange?.(mode === 'fast' ? 'ptc' : 'fast')}
          measureOnly={measureOnly}
        />
      ),
      menu: () => (
        <>
          <DropdownMenuItem onSelect={() => { if (mode !== 'fast') onModeChange?.('fast'); }}>
            <Zap className="h-4 w-4" />
            <span>Flash</span>
            {mode === 'fast' && <Check className="ml-auto h-4 w-4" />}
          </DropdownMenuItem>
          <DropdownMenuItem
            disabled={ptcBlocked}
            title={ptcBlocked ? ptcDisabledReason! : undefined}
            onSelect={() => { if (mode !== 'ptc') onModeChange?.('ptc'); }}
          >
            <FileStack className="h-4 w-4" />
            <span>PTC</span>
            {mode === 'ptc' && <Check className="ml-auto h-4 w-4" />}
          </DropdownMenuItem>
        </>
      ),
    },
    {
      id: 'plan',
      group: 'toggle',
      // Shown when the host enforces PTC (no mode toggle) or PTC is selected.
      visible: showPlanWatchSlot,
      active: planMode,
      inline: ({ measureOnly }) => (
        <PillToggle
          active={planMode}
          onToggle={() => setPlanMode(!planMode)}
          icon={ScrollText}
          label={t('chat.pills.plan')}
          title={t('chat.pills.planTitle')}
          measureOnly={measureOnly}
        />
      ),
      menu: () => (
        <DropdownMenuItem
          title={t('chat.pills.planTitle')}
          onSelect={(e) => { e.preventDefault(); setPlanMode(!planMode); }}
        >
          <ScrollText className="h-4 w-4" style={planMode ? { color: 'var(--color-accent-light)' } : undefined} />
          <span>{t('chat.pills.plan')}</span>
          {planMode && <Check className="ml-auto h-4 w-4" style={{ color: 'var(--color-accent-light)' }} />}
        </DropdownMenuItem>
      ),
    },
    {
      id: 'watch',
      group: 'toggle',
      visible: showPlanWatchSlot && marketWatchEnabled,
      active: watchMode,
      inline: ({ measureOnly }) => (
        <PillToggle
          active={watchMode}
          onToggle={() => setWatchMode(!watchMode)}
          icon={Radar}
          label={t('chat.pills.watch')}
          title={t('chat.pills.watchTitle')}
          measureOnly={measureOnly}
        />
      ),
      menu: () => (
        <DropdownMenuItem
          title={t('chat.pills.watchTitle')}
          onSelect={(e) => { e.preventDefault(); setWatchMode(!watchMode); }}
        >
          <Radar className="h-4 w-4" style={watchMode ? { color: 'var(--color-accent-light)' } : undefined} />
          <span>{t('chat.pills.watch')}</span>
          {watchMode && <Check className="ml-auto h-4 w-4" style={{ color: 'var(--color-accent-light)' }} />}
        </DropdownMenuItem>
      ),
    },
    {
      id: 'workspace',
      group: 'workspace',
      // Widest and least-toggled, so it folds first.
      visible: showWorkspaceSelector,
      inline: ({ measureOnly }) => {
        const pill = (
          <PillToggle
            active={showWorkspaceMenu}
            onToggle={() => setShowWorkspaceMenu(!showWorkspaceMenu)}
            icon={FolderOpen}
            label={selectedWorkspaceName}
            title="Select workspace"
            trailing={<ChevronDown className="h-3 w-3 flex-none" />}
            aria="expanded"
            gap={4}
            className="min-w-0"
            labelClassName="min-w-0 max-w-[100px] truncate"
            buttonRef={measureOnly ? undefined : workspaceBtnRef}
            measureOnly={measureOnly}
          />
        );
        if (measureOnly) return pill;
        return (
          <div className="relative flex min-w-0" ref={workspaceMenuRef}>
            {pill}
            {showWorkspaceMenu && (
              <div className="workspace-dropdown workspace-dropdown-up">
                {workspaces?.map((ws) => (
                  <div
                    key={ws.workspace_id}
                    className={`workspace-dropdown-item ${ws.workspace_id === selectedWorkspaceId ? 'active' : ''}`}
                    onMouseDown={(e) => {
                      e.preventDefault();
                      onWorkspaceChange?.(ws.workspace_id);
                      setShowWorkspaceMenu(false);
                    }}
                  >
                    <FolderOpen className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                    <span>{ws.name}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        );
      },
      menu: () => (
        <DropdownMenuSub>
          <DropdownMenuSubTrigger>
            <FolderOpen className="h-4 w-4 flex-none" />
            <span className="min-w-0 max-w-[120px] truncate">{selectedWorkspaceName}</span>
          </DropdownMenuSubTrigger>
          <DropdownMenuSubContent className="max-h-64 overflow-y-auto">
            {workspaces?.map((ws) => (
              <DropdownMenuItem
                key={ws.workspace_id}
                onSelect={() => onWorkspaceChange?.(ws.workspace_id)}
              >
                <span className="truncate">{ws.name}</span>
                {ws.workspace_id === selectedWorkspaceId && <Check className="ml-auto h-4 w-4 flex-shrink-0" />}
              </DropdownMenuItem>
            ))}
          </DropdownMenuSubContent>
        </DropdownMenuSub>
      ),
    },
  ], [
    hasModeToggle, mode, onModeChange, ptcBlocked, ptcDisabledReason,
    showPlanWatchSlot, planMode, setPlanMode, watchMode, setWatchMode, marketWatchEnabled,
    showWorkspaceSelector, showWorkspaceMenu, selectedWorkspaceName,
    workspaces, selectedWorkspaceId, onWorkspaceChange, t,
  ]);
}
