/**
 * The near-full disk warning for one computer.
 *
 * Every workspace now lives on one shared machine, so files from all of them
 * fill the same disk, and the moment it fills, saving a file or running code
 * fails in whichever workspace happens to be active. The warning therefore
 * shows wherever the user works (the gallery and every chat on that machine).
 * Its call to action is the spec change, offered only where a size with more
 * disk is one the plan allows: a dialog whose every bigger size is disabled is
 * a dead end, so a plan that lacks them gets the way to a bigger plan instead.
 * At `critical` the storage breakdown also lets the user delete by hand.
 */
import { useEffect, useSyncExternalStore } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertTriangle, HardDrive, X } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import { cn } from '@/lib/utils';
import { createValueStore } from '@/lib/valueStore';
import type { Computer, ComputerDiskLevel } from '@/types/api';

import { providerManagesSpec } from './providerUi';
import { openComputerSpec, openComputersPanel } from '../hooks/computerPanelStore';
import { useComputerProviders } from '../hooks/useComputers';
import { useTierQuota } from '../hooks/useTierQuota';
import { diskLevelRank, diskUpgradeTier, isDiskAlertLevel, largerDiskTiers } from './computerDiskUi';
import { PlanUpgradeHint } from './planUpgradeHint';
import { activeSpecChange, effectiveTier, SpecChangeProgress } from './specChangeUi';
import { needsBiggerPlan, normalizeTier, offeredTiers, quotaForSpecChange } from './tierUi';

// --- Dismissal, per computer for the browser session --------------------------

type Dismissals = Record<string, ComputerDiskLevel>;

const DISMISS_KEY = 'la:disk-warning-dismissed';

function readDismissed(): Dismissals {
  try {
    const raw = sessionStorage.getItem(DISMISS_KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    return parsed && typeof parsed === 'object' ? (parsed as Dismissals) : {};
  } catch {
    return {};
  }
}

const dismissedStore = createValueStore<Dismissals>(readDismissed());

function writeDismissed(next: Dismissals) {
  dismissedStore.set(next);
  try {
    sessionStorage.setItem(DISMISS_KEY, JSON.stringify(next));
  } catch {
    // Storage blocked: the dismissal still holds for this page's life.
  }
}

function dismiss(computerId: string, level: ComputerDiskLevel) {
  writeDismissed({ ...dismissedStore.get(), [computerId]: level });
}

/**
 * Forget a dismissal once its computer's disk has recovered, so the next
 * fill-up in the same tab warns again instead of staying silent until critical.
 * Mounted once, by ComputersDialogHost, which always holds the list.
 */
export function useForgetRecoveredDismissals(computers: readonly Computer[] | undefined) {
  useEffect(() => {
    const dismissed = dismissedStore.get();
    const recovered = (computers ?? []).filter(
      (c) => dismissed[c.computer_id] && c.disk?.level && !isDiskAlertLevel(c.disk.level),
    );
    if (recovered.length === 0) return;
    const next = { ...dismissed };
    for (const c of recovered) delete next[c.computer_id];
    writeDismissed(next);
  }, [computers]);
}

function useDismissed(): Dismissals {
  return useSyncExternalStore(dismissedStore.subscribe, dismissedStore.get);
}

/**
 * Whether a computer's warning should show. Critical is never dismissible; a
 * dismissed warning returns once the level gets worse than what was dismissed.
 */
export function useDiskWarningVisible(computer: Computer | null | undefined): boolean {
  return isWarningVisible(computer, useDismissed());
}

function isWarningVisible(computer: Computer | null | undefined, dismissed: Dismissals): boolean {
  const level = computer?.disk?.level;
  if (!computer || !isDiskAlertLevel(level)) return false;
  if (level === 'critical') return true;
  const seen = dismissed[computer.computer_id];
  return !seen || diskLevelRank(level) > diskLevelRank(seen);
}

/**
 * The one machine a single banner speaks for: the worst disk among those whose
 * warning still shows, so dismissing one machine hands the banner to the next
 * rather than silencing a machine that is just as full.
 */
export function useDiskAlertComputer(computers: readonly Computer[] | undefined): Computer | null {
  const dismissed = useDismissed();
  let worst: Computer | null = null;
  for (const c of computers ?? []) {
    if (isWarningVisible(c, dismissed) && diskLevelRank(c.disk?.level) > diskLevelRank(worst?.disk?.level)) worst = c;
  }
  return worst;
}

// --- The warning -----------------------------------------------------------

const ACTION_CLASS = 'h-7 px-2.5 text-xs';

interface DiskWarningProps {
  computer: Computer;
  className?: string;
}

export function DiskWarning({ computer, className }: DiskWarningProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const visible = useDiskWarningVisible(computer);
  const disk = computer.disk;
  const level = disk?.level;
  // Whether there is a size with more disk depends on what the provider runs
  // (Medium and Large share one, and only E2B runs Extra Large), and whether
  // it can be picked on what the plan allows.
  const { data: providerData } = useComputerProviders({ enabled: visible && isDiskAlertLevel(level) });
  const { data: planQuota } = useTierQuota({ enabled: visible && isDiskAlertLevel(level) });

  if (!visible || !disk || !isDiskAlertLevel(level)) return null;

  const critical = level === 'critical';
  const tint = critical ? 'var(--color-loss-soft)' : 'var(--color-warning-soft)';
  const specChange = activeSpecChange(computer);
  const tier = effectiveTier(computer);
  const provider = providerData?.providers.find((p) => p.kind === computer.kind);
  const offer = offeredTiers(provider, tier);
  // The slot is the size the row holds, which the server counts.
  const quota = quotaForSpecChange(planQuota, normalizeTier(computer.resource_tier));
  const managesSpec = providerManagesSpec(computer.kind);
  const canGrow = managesSpec && diskUpgradeTier(tier, offer, quota) !== null;
  const planLimited =
    managesSpec && !canGrow && largerDiskTiers(tier, offer).some((id) => needsBiggerPlan(quota, id));

  const title = critical
    ? t('computer.disk.criticalTitle', '{{name}} is almost out of disk space', { name: computer.name })
    : t('computer.disk.warningTitle', '{{name}} is running low on disk space', { name: computer.name });
  const body = critical
    ? t(
        'computer.disk.criticalBody',
        'Only {{free}} left. Saving files and running code will start to fail in every workspace on this computer.',
        { free: formatBytes(disk.free_bytes, locale) },
      )
    : t(
        'computer.disk.warningBody',
        '{{free}} left, shared by every workspace on this computer. Free up space before it fills up.',
        { free: formatBytes(disk.free_bytes, locale) },
      );

  // A change already running replaces the call to action with its progress:
  // offering it again would only earn a 409. With no size of more disk that
  // the provider runs and the plan allows, there is no spec change to offer.
  const changeSpec = specChange ? (
    <SpecChangeProgress change={specChange} className="text-xs" />
  ) : canGrow ? (
    <Button size="sm" className={ACTION_CLASS} onClick={() => openComputerSpec(computer.computer_id)}>
      {t('workspace.changeSpec', 'Change spec')}
    </Button>
  ) : null;
  const breakdown = critical ? (
    <Button
      size="sm"
      variant="ghost"
      className={cn(ACTION_CLASS, 'gap-1.5')}
      onClick={() => openComputersPanel({ view: 'details', computerId: computer.computer_id })}
    >
      <HardDrive className="h-3.5 w-3.5" />
      {t('computer.disk.openBreakdown', 'Open storage breakdown')}
    </Button>
  ) : null;

  return (
    <div
      role={critical ? 'alert' : 'status'}
      data-testid="disk-warning"
      data-level={level}
      className={cn('flex items-start gap-2 rounded-lg border px-3 py-2.5 text-sm', className)}
      // Opaque: the tint is layered over the page surface, since in a chat the
      // warning sits above the composer and the transcript scrolls under it.
      style={{
        backgroundColor: 'var(--color-bg-page)',
        backgroundImage: `linear-gradient(${tint}, ${tint})`,
        borderColor: critical ? 'var(--color-loss-muted)' : 'var(--color-warning)',
        color: 'var(--color-text-primary)',
      }}
    >
      <AlertTriangle
        className="mt-0.5 h-4 w-4 shrink-0"
        style={{ color: critical ? 'var(--color-loss)' : 'var(--color-warning)' }}
        aria-hidden="true"
      />
      {/* Actions sit beside the text, and drop below it where the row is too narrow. */}
      <div className="flex flex-1 min-w-0 flex-col gap-2 sm:flex-row sm:items-center">
        <div className="flex-1 min-w-0">
          <p className="font-medium">{title}</p>
          <p className="text-xs mt-0.5" style={{ color: 'var(--color-text-secondary)' }}>
            {body}
          </p>
          {!specChange && planLimited && (
            <PlanUpgradeHint
              lead={t('computer.spec.planLimited', 'Need a bigger spec than your plan includes?')}
              testId="disk-plan-cta"
              className="mt-1"
            />
          )}
        </div>
        {(changeSpec || breakdown) && (
          <div className="flex shrink-0 flex-wrap items-center gap-1.5">
            {breakdown}
            {changeSpec}
          </div>
        )}
      </div>
      {!critical && (
        <button
          type="button"
          className="-m-1 shrink-0 rounded p-1.5 hover:opacity-80"
          style={{ color: 'var(--color-text-tertiary)' }}
          aria-label={t('computer.disk.dismiss', 'Dismiss')}
          onClick={() => dismiss(computer.computer_id, level)}
        >
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );
}
