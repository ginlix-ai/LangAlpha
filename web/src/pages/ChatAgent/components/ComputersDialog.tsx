/**
 * The machines behind the workspace gallery, and the one place to manage them.
 *
 * Deliberately a dialog rather than a page: a computer is infrastructure a
 * user visits to manage, not a place they work. Tier, always-on, name and
 * storage all belong to the machine, so they live here rather than on any one
 * workspace's menu, where changing them would silently change its siblings.
 */
import { useEffect, useId, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { ChevronDown, Cpu, Pencil, Play, Square, Star } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Disclosure } from '@/components/ui/Disclosure';
import { ModalShell } from '@/components/ui/ModalShell';
import { Loader } from '@/components/ui/loader';
import { ToggleSwitch } from '@/components/ui/switch';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { Computer } from '@/types/api';

import { useComputerStorage, useComputers } from '../hooks/useComputers';
import { openComputerSpec } from '../hooks/computerPanelStore';
import { useComputerActions, type PowerAction } from '../hooks/useComputerActions';
import { ComputerStatusIndicator, isComputerStatusTransitional } from './computerStatusUi';
import { tierLabel } from './tierUi';
import { DiskBar, diskLevelColor, diskUsageLabel } from './computerDiskUi';
import { activeSpecChange, effectiveTier, specChangeLabel, SpecChangeProgress } from './specChangeUi';
import AlwaysOnConfirmDialog from './AlwaysOnConfirmDialog';
import { denialMessage } from '../utils/denialMessage';

/** The server's column width for a computer's name. */
const COMPUTER_NAME_MAX = 255;

interface ComputersDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Scroll to and highlight this machine's row. */
  focusComputerId?: string | null;
  /** Open the focused row's storage breakdown. */
  expandStorage?: boolean;
}

function ComputersDialog({ open, onOpenChange, focusComputerId = null, expandStorage = false }: ComputersDialogProps) {
  const { t } = useTranslation();
  const titleId = useId();
  const [alwaysOnTarget, setAlwaysOnTarget] = useState<Computer | null>(null);

  const { data, isLoading } = useComputers({ enabled: open });
  const computers = data?.computers ?? [];
  const { power, renameComputerTo, alwaysOn } = useComputerActions();
  // A refused start relays the quota service's own sentence (denialMessage).
  const error = power.error ? denialMessage(power.error, t) : null;

  const closeAndReset = () => {
    power.reset();
    onOpenChange(false);
  };

  const toggleAlwaysOn = (computer: Computer) => {
    if (alwaysOn.isPending) return;
    if (computer.is_always_on) {
      alwaysOn.mutate({ computerId: computer.computer_id, enabled: false });
    } else {
      setAlwaysOnTarget(computer);
    }
  };

  // Additional-computer creation stays hidden until projects can select a machine.
  return (
    <>
      <AnimatePresence>
        {open && (
          <ModalShell
            labelId={titleId}
            title={t('computer.computers', 'Computers')}
            subtitle={t('computer.computersDesc', 'Your workspaces are folders on these machines. Starting or stopping one affects every workspace it holds.')}
            onClose={closeAndReset}
            width="standard"
            density="form"
          >
            {isLoading ? (
              <div className="flex items-center gap-2 py-6 justify-center" style={{ color: 'var(--color-text-tertiary)' }}>
                <Loader size={14} style={{ color: 'var(--color-accent-primary)' }} />
                <span className="text-sm">{t('computer.loading', 'Loading computers')}</span>
              </div>
            ) : (
              <div className="flex flex-col gap-2">
                {computers.length === 0 && (
                  <p className="text-sm py-4" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('computer.empty', 'No computers yet. Your first workspace creates one.')}
                  </p>
                )}
                {computers.map((computer) => (
                  <ComputerRow
                    // Keyed on the request too, so opening onto a row's storage
                    // remounts it expanded instead of re-syncing local state.
                    key={`${computer.computer_id}:${expandStorage && computer.computer_id === focusComputerId}`}
                    computer={computer}
                    focused={computer.computer_id === focusComputerId}
                    initiallyExpanded={expandStorage && computer.computer_id === focusComputerId}
                    busy={power.isPending && power.variables?.computerId === computer.computer_id}
                    alwaysOnBusy={alwaysOn.isPending && alwaysOn.variables?.computerId === computer.computer_id}
                    onAction={(action) => power.mutate({ computerId: computer.computer_id, action })}
                    onRename={(name) => renameComputerTo(computer.computer_id, name)}
                    onChangeSpec={() => openComputerSpec(computer.computer_id)}
                    onToggleAlwaysOn={() => toggleAlwaysOn(computer)}
                  />
                ))}
              </div>
            )}

            {error && (
              <p className="text-sm" role="alert" style={{ color: 'var(--color-icon-danger)' }}>
                {error}
              </p>
            )}
          </ModalShell>
        )}
      </AnimatePresence>
      <AlwaysOnConfirmDialog
        target={alwaysOnTarget}
        onClose={() => setAlwaysOnTarget(null)}
        onConfirm={() => {
          if (!alwaysOnTarget) return;
          alwaysOn.mutate(
            { computerId: alwaysOnTarget.computer_id, enabled: true },
            { onSuccess: () => setAlwaysOnTarget(null) },
          );
        }}
        busy={!!alwaysOnTarget && alwaysOn.isPending}
      />
    </>
  );
}

interface ComputerRowProps {
  computer: Computer;
  focused: boolean;
  initiallyExpanded: boolean;
  busy: boolean;
  alwaysOnBusy: boolean;
  onAction: (action: PowerAction) => void;
  onRename: (name: string) => void;
  onChangeSpec: () => void;
  onToggleAlwaysOn: () => void;
}

function ComputerRow({
  computer,
  focused,
  initiallyExpanded,
  busy,
  alwaysOnBusy,
  onAction,
  onRename,
  onChangeSpec,
  onToggleAlwaysOn,
}: ComputerRowProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const rowRef = useRef<HTMLDivElement>(null);
  const breakdownId = useId();
  const [expanded, setExpanded] = useState(initiallyExpanded);
  const isRunning = computer.status === 'running';
  const isTransitional = isComputerStatusTransitional(computer.status);
  const tier = effectiveTier(computer);
  const disk = computer.disk ?? null;
  const specChange = activeSpecChange(computer);

  useEffect(() => {
    if (focused) rowRef.current?.scrollIntoView?.({ block: 'nearest' });
  }, [focused]);

  return (
    <div
      ref={rowRef}
      className="flex flex-col gap-3 rounded-lg border p-3"
      style={{ borderColor: focused ? 'var(--color-border-elevated)' : 'var(--color-border-muted)' }}
      data-testid="computer-row"
    >
      <div className="flex items-center gap-3">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 min-w-0">
            <EditableName name={computer.name} onCommit={onRename} />
            {computer.is_primary && (
              <span
                className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[0.625rem] font-medium shrink-0"
                style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-secondary)' }}
                title={t('computer.primaryBadgeTitle', 'New workspaces are created on this computer')}
              >
                <Star className="h-3 w-3" />
                {t('computer.primaryBadge', 'Primary')}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2 text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
            <ComputerStatusIndicator status={computer.status} />
            <span aria-hidden="true">·</span>
            <span>{tierLabel(t, tier)}</span>
            {typeof computer.workspace_count === 'number' && (
              <>
                <span aria-hidden="true">·</span>
                <span>{t('computer.workspaceCount', { count: computer.workspace_count })}</span>
              </>
            )}
          </div>
        </div>
        {isRunning ? (
          <Button variant="ghost" size="sm" className="gap-1.5" disabled={busy || !!specChange} onClick={() => onAction('stop')} data-computer-power>
            <Square className="h-3.5 w-3.5" />
            {t('computer.stop', 'Stop')}
          </Button>
        ) : (
          <Button
            variant="ghost"
            size="sm"
            className="gap-1.5"
            disabled={busy || isTransitional || !!specChange || computer.status === 'deleted'}
            onClick={() => onAction('start')}
            data-computer-power
          >
            <Play className="h-3.5 w-3.5" />
            {t('computer.start', 'Start')}
          </Button>
        )}
      </div>

      <div className="flex flex-col gap-1.5">
        <div className="flex items-center justify-between gap-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          <span style={disk && disk.level !== 'healthy' ? { color: diskLevelColor(disk.level) } : undefined}>
            {disk ? diskUsageLabel(t, disk, locale) : t('computer.disk.unknown', 'Storage not measured yet')}
          </span>
          <button
            type="button"
            className="-my-1.5 inline-flex min-h-7 items-center gap-1 rounded px-1.5 hover:opacity-80"
            aria-expanded={expanded}
            aria-controls={breakdownId}
            onClick={() => setExpanded((v) => !v)}
          >
            {t('computer.disk.breakdown', 'By workspace')}
            <ChevronDown className={cn('h-3 w-3 transition-transform', expanded && 'rotate-180')} />
          </button>
        </div>
        {disk && <DiskBar disk={disk} />}
        <Disclosure open={expanded} id={breakdownId}>
          <StorageBreakdown computer={computer} enabled={expanded} />
        </Disclosure>
      </div>

      <div className="flex items-center justify-between gap-3">
        <label className="flex items-center gap-2 text-sm" style={{ color: 'var(--color-text-secondary)' }}>
          <ToggleSwitch
            checked={computer.is_always_on}
            onChange={onToggleAlwaysOn}
            // Mid start or stop the switch would promise a state the machine
            // is about to leave; it opens again once the machine settles.
            disabled={alwaysOnBusy || isTransitional || computer.status === 'deleted'}
            ariaLabel={t('computer.alwaysOn', 'Always-on')}
          />
          <span>{t('computer.alwaysOn', 'Always-on')}</span>
        </label>
        {specChange ? (
          // Opens the same dialog, which shows the change's progress, not a picker.
          <button
            type="button"
            className="rounded px-1 text-xs hover:opacity-80"
            aria-label={specChangeLabel(t, specChange)}
            onClick={onChangeSpec}
          >
            <SpecChangeProgress change={specChange} asLabel />
          </button>
        ) : (
          <Button variant="ghost" size="sm" className="gap-1.5" onClick={onChangeSpec} disabled={isTransitional || computer.status === 'deleted'}>
            <Cpu className="h-3.5 w-3.5" />
            {t('workspace.changeSpec', 'Change spec')}
          </Button>
        )}
      </div>
    </div>
  );
}

/** The name, editable in place. Enter or blur saves; Escape backs out. */
function EditableName({ name, onCommit }: { name: string; onCommit: (name: string) => void }) {
  const { t } = useTranslation();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (editing) inputRef.current?.select();
  }, [editing]);

  const commit = () => {
    const next = draft.trim();
    setEditing(false);
    if (next && next !== name) onCommit(next);
  };

  if (editing) {
    return (
      <input
        ref={inputRef}
        value={draft}
        maxLength={COMPUTER_NAME_MAX}
        aria-label={t('computer.renameLabel', 'Computer name')}
        className="min-w-0 flex-1 rounded border bg-transparent px-1.5 py-0.5 text-sm font-medium"
        style={{ borderColor: 'var(--color-border-default)', color: 'var(--color-text-primary)', backgroundColor: 'var(--color-bg-input)' }}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            commit();
          } else if (e.key === 'Escape') {
            // Claimed here so the dialog around it stays open.
            e.preventDefault();
            setDraft(name);
            setEditing(false);
          }
        }}
      />
    );
  }
  return (
    <span className="group/name flex items-center gap-1 min-w-0">
      <span className="font-medium truncate" style={{ color: 'var(--color-text-primary)' }}>
        {name}
      </span>
      <button
        type="button"
        className="-m-1 shrink-0 rounded p-2 opacity-60 hover:opacity-100 focus-visible:opacity-100"
        style={{ color: 'var(--color-text-tertiary)' }}
        aria-label={t('computer.rename', 'Rename computer')}
        onClick={() => {
          setDraft(name);
          setEditing(true);
        }}
      >
        <Pencil className="h-3 w-3" />
      </button>
    </span>
  );
}

function StorageBreakdown({ computer, enabled }: { computer: Computer; enabled: boolean }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { data, isLoading, isError } = useComputerStorage(computer.computer_id, { enabled });

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        <Loader size={12} className="text-(--color-accent-primary)" />
        {t('computer.disk.measuring', 'Measuring')}
      </div>
    );
  }
  if (isError || !data) {
    return (
      <p className="py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('computer.disk.breakdownFailed', 'Could not read storage right now.')}
      </p>
    );
  }
  if (!data.live) {
    // A running machine answers stored readings only when it cannot measure:
    // a failed read if it ever reported a disk, a machine that never measures
    // (no storage quota) if not. Neither is fixed by starting it.
    return (
      <p className="py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
        {computer.status !== 'running'
          ? t('computer.disk.startForBreakdown', 'Start the computer to see what each workspace uses.')
          : data.disk
            ? t('computer.disk.breakdownFailed', 'Could not read storage right now.')
            : t('computer.disk.breakdownUnavailable', "This computer doesn't report storage by workspace.")}
      </p>
    );
  }
  const rows = [...data.workspaces].sort((a, b) => b.bytes - a.bytes);
  return (
    <ul className="flex flex-col gap-1 pt-1 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
      {rows.map((w) => (
        <li key={w.workspace_id} className="flex items-center justify-between gap-3">
          <span className="truncate">{w.name}</span>
          <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>{formatBytes(w.bytes, locale)}</span>
        </li>
      ))}
      {data.other_bytes > 0 && (
        <li className="flex items-center justify-between gap-3">
          <span className="truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('computer.disk.other', 'System, caches and shared files')}
          </span>
          <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>{formatBytes(data.other_bytes, locale)}</span>
        </li>
      )}
    </ul>
  );
}

export default ComputersDialog;
