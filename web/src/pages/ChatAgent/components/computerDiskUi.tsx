/**
 * Disk vocabulary for a computer: the level ladder, its colors, and the bar.
 *
 * The level is decided server-side from free bytes, so every surface here only
 * maps it to presentation; none of them re-derives it from the numbers.
 */
import { useTranslation } from 'react-i18next';

import { useLocale } from '@/hooks/useLocale';
import { formatBytes } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { ComputerDisk, ComputerDiskLevel, ResourceTier, WorkspaceQuota } from '@/types/api';

import { isTierBlocked, tierCapacity, type TierOffer } from './tierUi';

type Translate = ReturnType<typeof useTranslation>['t'];

const LEVEL_RANK: Record<ComputerDiskLevel, number> = {
  healthy: 0,
  notice: 1,
  warning: 2,
  critical: 3,
};

export function diskLevelRank(level: ComputerDiskLevel | null | undefined): number {
  return level ? LEVEL_RANK[level] ?? 0 : 0;
}

/** Levels that earn a banner. Notice stays in the storage bar and the card dot. */
export function isDiskAlertLevel(level: ComputerDiskLevel | null | undefined): level is 'warning' | 'critical' {
  return level === 'warning' || level === 'critical';
}

/** Fill color of the bar and the card dot, by level. */
export function diskLevelColor(level: ComputerDiskLevel | null | undefined): string {
  switch (level) {
    case 'critical':
      return 'var(--color-loss)';
    case 'warning':
      return 'var(--color-warning)';
    // The accent annotates; it is not a state color, so notice stays neutral.
    case 'notice':
      return 'var(--color-text-secondary)';
    default:
      return 'var(--color-text-tertiary)';
  }
}

/**
 * The offered sizes with more disk than `tier`, smallest first. Medium and
 * Large share a disk, so the next size up is not always one. Without the
 * provider's specs (an older server) the next size up stands in.
 */
export function largerDiskTiers(tier: ResourceTier, offer: TierOffer): ResourceTier[] {
  const diskOf = (id: ResourceTier) => offer.specs.find((s) => s.tier === id)?.disk_gib;
  const current = diskOf(tier);
  if (current === undefined) {
    const index = offer.tiers.indexOf(tier);
    return index >= 0 && index < offer.tiers.length - 1 ? [offer.tiers[index + 1]] : [];
  }
  return offer.tiers.filter((id) => (diskOf(id) ?? 0) > current);
}

/**
 * The size a low disk points to: the smallest with more disk that the plan
 * allows. Null when the provider runs none or the plan allows none. For a
 * computer already bigger than Small, pass the quota through
 * `quotaForSpecChange`, or the slot it holds reads as taken.
 */
export function diskUpgradeTier(
  tier: ResourceTier,
  offer: TierOffer,
  quota: WorkspaceQuota | null | undefined,
): ResourceTier | null {
  return largerDiskTiers(tier, offer).find((id) => !isTierBlocked(tierCapacity(quota, id))) ?? null;
}

export function diskUsageLabel(t: Translate, disk: ComputerDisk, locale: string): string {
  return t('computer.disk.usage', '{{used}} of {{total}} used, {{free}} free', {
    used: formatBytes(disk.used_bytes, locale),
    total: formatBytes(disk.total_bytes, locale),
    free: formatBytes(disk.free_bytes, locale),
  });
}

interface DiskBarProps {
  disk: ComputerDisk;
  className?: string;
}

/** Thin usage bar. The label beside it carries the numbers; the bar is the glance. */
export function DiskBar({ disk, className }: DiskBarProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const ratio = disk.total_bytes > 0 ? Math.min(1, Math.max(0, disk.used_bytes / disk.total_bytes)) : 0;
  return (
    <div
      className={cn('h-1.5 w-full overflow-hidden rounded-full', className)}
      style={{ backgroundColor: 'var(--color-border-muted)' }}
      role="meter"
      aria-valuemin={0}
      aria-valuemax={disk.total_bytes}
      aria-valuenow={disk.used_bytes}
      aria-label={t('computer.disk.label', 'Storage')}
      aria-valuetext={diskUsageLabel(t, disk, locale)}
    >
      <div
        className="h-full rounded-full transition-[width] duration-fold"
        style={{ width: `${Math.round(ratio * 1000) / 10}%`, backgroundColor: diskLevelColor(disk.level) }}
      />
    </div>
  );
}

/** The usage in words, in its level's colour once it is low; says so while unmeasured. */
export function DiskUsageText({ disk }: { disk: ComputerDisk | null }) {
  const { t } = useTranslation();
  const locale = useLocale();
  return (
    <span style={disk && disk.level !== 'healthy' ? { color: diskLevelColor(disk.level) } : undefined}>
      {disk ? diskUsageLabel(t, disk, locale) : t('computer.disk.unknown', 'Storage not measured yet')}
    </span>
  );
}
