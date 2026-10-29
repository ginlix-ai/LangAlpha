/**
 * "Disk 92%" in the disk's colour, once a computer's disk is past healthy, and
 * nothing before that. The disk is the machine's, not a workspace's, so it is
 * said where a machine is named once: the toolbar's computer chip and the rows
 * of the computer filter, beside the banner.
 */
import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { diskLevelColor } from '../computerDiskUi';

type Translate = ReturnType<typeof useTranslation>['t'];

export function DiskPercent({ computer, className }: { computer: Computer; className?: string }) {
  const { t } = useTranslation();
  const disk = computer.disk;
  const level = disk?.level;
  if (!disk || !level || level === 'healthy' || disk.total_bytes <= 0) return null;
  return (
    <span
      className={`shrink-0 whitespace-nowrap font-medium${className ? ` ${className}` : ''}`}
      style={{ color: diskLevelColor(level) }}
      title={diskTitle(t, level, computer.name)}
      data-testid="disk-percent"
    >
      {t('computer.disk.percent', 'Disk {{percent}}%', {
        percent: Math.min(100, Math.round((disk.used_bytes / disk.total_bytes) * 100)),
      })}
    </span>
  );
}

function diskTitle(t: Translate, level: string, name: string): string {
  if (level === 'critical') return t('computer.disk.criticalTitle', '{{name}} is almost out of disk space', { name });
  if (level === 'warning') return t('computer.disk.warningTitle', '{{name}} is running low on disk space', { name });
  return t('computer.disk.lowTitle', 'Disk is getting full');
}

export default DiskPercent;
