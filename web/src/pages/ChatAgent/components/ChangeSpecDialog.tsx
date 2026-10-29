import { useId, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence } from '@/lib/framer';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { ModalShell } from '@/components/ui/ModalShell';
import { useLocale } from '@/hooks/useLocale';
import type { Computer, ComputerProviderOption, ResourceTier, WorkspaceQuota } from '@/types/api';
import {
  TierRadioGroup,
  isTierBlocked,
  needsBiggerPlan,
  normalizeTier,
  offeredTiers,
  quotaForSpecChange,
  tierCapacity,
  tierLabel,
  type TierOffer,
} from './tierUi';
import { DiskBar, diskUpgradeTier, diskUsageLabel, isDiskAlertLevel } from './computerDiskUi';
import { specErrorFiles, type SpecError } from './specErrors';
import { activeSpecChange, effectiveTier, specChangeLabel } from './specChangeUi';
import { PlanUpgradeHint } from './planUpgradeHint';
import { MachinePreview } from './machineArt/MachinePreview';
import { hostedPreview } from './machineArt/machine';

type SpecTarget = Pick<Computer, 'name' | 'kind' | 'resource_tier' | 'status' | 'disk' | 'spec_change'>;

interface ChangeSpecDialogProps {
  target: SpecTarget | null;
  onClose: () => void;
  onSubmit: (tier: ResourceTier) => void;
  /** The request is in flight. It answers once the change is accepted, so this is brief. */
  busy: boolean;
  /** Per-tier count quotas (platform mode only); null/undefined hides the capacity hint. */
  quota?: WorkspaceQuota | null;
  /**
   * The computer's provider as offered now, which decides the sizes shown.
   * Unknown, or a provider no longer offered, shows the base ladder.
   */
  provider?: ComputerProviderOption | null;
  /** The last refusal, shown inline so a list of files can be read and acted on. */
  error?: SpecError | null;
  /** Opened over the Computers dialog, which steps back and keeps the page dimmed. */
  stacked?: boolean;
}

/**
 * Change a computer's resource tier. The machine is backed up and recreated,
 * which is slow and touches every workspace on it, so the dialog says so
 * before the click and keeps saying so while it runs. The change runs on the
 * server, so its progress comes from the row: closing the dialog stops
 * nothing, and reopening it shows the same change still going.
 *
 * The panel is mounted per opening, inside `AnimatePresence`, so it leaves
 * with the last target it showed rather than an empty one.
 */
function ChangeSpecDialog({ target, ...rest }: ChangeSpecDialogProps) {
  return <AnimatePresence>{target && <ChangeSpecPanel target={target} {...rest} />}</AnimatePresence>;
}

function ChangeSpecPanel({
  target,
  onClose,
  onSubmit,
  busy,
  quota: planQuota,
  provider,
  error,
  stacked = false,
}: Omit<ChangeSpecDialogProps, 'target'> & { target: SpecTarget }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const titleId = useId();
  const currentTier = effectiveTier(target);
  const offer = offeredTiers(provider, currentTier);
  // A computer bigger than Small moves in the slot it holds, so the other
  // larger sizes the plan allows stay open to it. The slot is the size the row
  // holds, which the server counts, even when an interrupted change shows the
  // size it came from.
  const quota = quotaForSpecChange(planQuota, normalizeTier(target.resource_tier));
  // Derived rather than seeded: the quota and the provider's sizes land after
  // the dialog opens and can refetch while it is open, and the row behind
  // `target` is replaced on every list refetch. A pick holds while the
  // provider offers it and the plan allows it (the current tier always does);
  // otherwise the seed stands.
  const [picked, setPicked] = useState<ResourceTier | null>(null);
  const tier =
    picked && offer.tiers.includes(picked) && (picked === currentTier || !isTierBlocked(tierCapacity(quota, picked)))
      ? picked
      : seedTier(currentTier, target, offer, quota);
  const isRunning = target.status === 'running';
  const errorFiles = specErrorFiles(error?.files, t, locale);
  const change = activeSpecChange(target);
  // The box it becomes: the size picked, or the one a running change moves to.
  const shownTier = change ? normalizeTier(change.target_tier) : tier;
  // A bigger tier the plan keeps closed can't be picked here, so the dialog
  // points to where that is decided instead of leaving a dead end. Only where
  // a bigger plan opens it: a plan at the top size is not told to upgrade.
  const planLimited =
    !change &&
    offer.tiers.slice(offer.tiers.indexOf(currentTier) + 1).some((id) => needsBiggerPlan(quota, id));
  // The row is the only witness to where the change is. It reads 'starting'
  // from before the backup until the new sandbox is up, so that one phase
  // covers both; 'running' is the moment before the machine is claimed.
  const phase = !change
    ? null
    : isRunning
      ? t('computer.spec.phaseBackup', 'Backing up every workspace on this computer.')
      : target.status === 'starting' || target.status === 'creating'
        ? t('computer.spec.phaseRebuild', 'Backing up your files and applying the {{tier}} spec.', {
            tier: tierLabel(t, normalizeTier(change.target_tier)),
          })
        : t('computer.spec.phaseApply', 'Applying the new spec.');

  const footer = (
    <div className="flex items-center justify-end gap-2">
      <Button type="button" variant="ghost" onClick={onClose} disabled={busy}>
        {change ? t('common.close', 'Close') : t('common.cancel')}
      </Button>
      {!change && (
        <Button
          type="button"
          onClick={() => onSubmit(tier)}
          disabled={busy || tier === currentTier}
          aria-busy={busy}
        >
          {busy ? t('computer.spec.applying', 'Applying...') : t('computer.spec.apply', 'Back up and change spec')}
        </Button>
      )}
    </div>
  );

  return (
    <ModalShell
      labelId={titleId}
      title={t('computer.spec.title', 'Change spec for {{name}}', { name: target.name })}
      subtitle={
        <>
          {isRunning
            ? t(
                'computer.spec.descRunning',
                'Every workspace on this computer is backed up first. The computer is then recreated at the new spec and your files are restored. Workspaces on it are unavailable for a few minutes.',
              )
            : t(
                'computer.spec.descStopped',
                'Every workspace on this computer is backed up first. The new spec applies the next time the computer starts, and your files are restored then.',
              )}{' '}
          {t(
            'computer.spec.notKept',
            "Installed packages, caches and running apps aren't kept. The Agent reinstalls what it needs.",
          )}
        </>
      }
      onClose={onClose}
      closeDisabled={busy}
      width="standard"
      density="form"
      stacked={stacked}
      footer={footer}
    >
      <MachinePreview machine={hostedPreview(t, target.kind, offer, shownTier)} />
      {target.disk && (
        <div className="flex flex-col gap-1.5">
          <DiskBar disk={target.disk} />
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            {diskUsageLabel(t, target.disk, locale)}
          </span>
        </div>
      )}
      {change ? (
        <div
          className="flex items-start gap-2 rounded-md px-3 py-2.5 text-sm"
          role="status"
          aria-live="polite"
          data-testid="spec-change-progress"
          style={{ backgroundColor: 'var(--color-bg-card)', color: 'var(--color-text-primary)' }}
        >
          <Loader size={14} className="mt-0.5 text-(--color-accent-primary)" />
          <div className="flex flex-col gap-0.5">
            <span className="font-medium">{specChangeLabel(t, change)}</span>
            <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              {phase}{' '}
              {t('computer.spec.progressHint', 'This can take a few minutes. You can close this dialog; the change keeps running.')}
            </span>
          </div>
        </div>
      ) : (
        <TierRadioGroup
          offer={offer}
          value={tier}
          onChange={setPicked}
          label={t('computer.tierLabel', 'Computer spec')}
          quota={quota}
          currentTier={currentTier}
        />
      )}
      {planLimited && (
        <PlanUpgradeHint lead={t('computer.spec.planLimited', 'Need a bigger spec than your plan includes?')} />
      )}
      {error && !busy && !change && (
        <div
          role="alert"
          className="rounded-md px-3 py-2 text-sm"
          style={{ backgroundColor: 'var(--color-loss-soft)', color: 'var(--color-text-primary)' }}
        >
          <p>{error.message}</p>
          {errorFiles.length > 0 && (
            <ul className="mt-1.5 list-disc pl-5 text-xs font-mono break-all" style={{ color: 'var(--color-text-secondary)' }}>
              {errorFiles.map((line) => <li key={line}>{line}</li>)}
            </ul>
          )}
        </div>
      )}
    </ModalShell>
  );
}

function seedTier(
  current: ResourceTier,
  target: SpecTarget,
  offer: TierOffer,
  quota: WorkspaceQuota | null | undefined,
): ResourceTier {
  return (isDiskAlertLevel(target.disk?.level) && diskUpgradeTier(current, offer, quota)) || current;
}

export default ChangeSpecDialog;
