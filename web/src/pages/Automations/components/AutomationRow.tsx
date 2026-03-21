import React from 'react';
import { motion } from 'framer-motion';
import { Clock, Timer, TrendingUp } from 'lucide-react';
import StatusBadge from './StatusBadge';
import { cronToHuman } from '../utils/cron';
import { formatPriceTrigger } from '../utils/price';
import { formatRelativeTime, formatDateTime } from '../utils/time';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useTranslation } from 'react-i18next';
import type { Automation } from '@/types/automation';

const STATUS_GLOW: Record<string, string> = {
  active: 'var(--color-success-soft)',
  paused: 'var(--color-warning-soft)',
  disabled: 'var(--color-loss-soft)',
  completed: 'var(--color-info-soft)',
};

interface AutomationRowProps {
  automation: Automation;
  index: number;
  onClick: (automation: Automation) => void;
}

export default function AutomationRow({ automation, index, onClick }: AutomationRowProps) {
  const { t } = useTranslation();
  const isCron = automation.trigger_type === 'cron';
  const isPrice = automation.trigger_type === 'price';
  const schedule = isPrice
    ? formatPriceTrigger(automation.trigger_config)
    : isCron
      ? cronToHuman(automation.cron_expression as string)
      : formatDateTime(automation.next_run_at);

  const glowColor = STATUS_GLOW[automation.status] || 'transparent';
  const isMobile = useIsMobile();

  if (isMobile) {
    return (
      <motion.div
        initial={{ opacity: 0, x: -20 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.3, delay: index * 0.05 }}
        onClick={() => onClick(automation)}
        className="flex flex-col gap-2 px-4 py-3 cursor-pointer transition-colors relative overflow-hidden rounded-lg border"
        style={{
          backgroundColor: 'var(--color-bg-card)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        {/* Status gradient glow on right edge */}
        <div
          className="absolute inset-y-0 right-0 w-32 pointer-events-none"
          style={{
            background: `linear-gradient(to left, ${glowColor}, transparent)`,
          }}
        />

        {/* Top row: icon + name + status */}
        <div className="flex items-center gap-2 min-w-0">
          {isPrice ? (
            <TrendingUp className="w-4 h-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
          ) : isCron ? (
            <Clock className="w-4 h-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
          ) : (
            <Timer className="w-4 h-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
          )}
          <span className="text-sm truncate font-medium flex-1" style={{ color: 'var(--color-text-primary)' }}>{automation.name}</span>
          <div className="relative z-10 shrink-0">
            <StatusBadge status={automation.status} />
          </div>
        </div>

        {/* Bottom row: schedule + next run */}
        <div className="flex items-center gap-3 text-xs pl-6" style={{ color: 'var(--color-text-secondary)' }}>
          <span className="truncate">{schedule}</span>
          {!isPrice && automation.next_run_at && (
            <>
              <span style={{ color: 'var(--color-border-default)' }}>·</span>
              <span className="shrink-0">{formatRelativeTime(automation.next_run_at)}</span>
            </>
          )}
        </div>
      </motion.div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.3, delay: index * 0.05 }}
      whileHover={{ y: -1 }}
      onClick={() => onClick(automation)}
      className="grid grid-cols-[1fr_1fr_0.6fr_0.8fr_0.5fr] gap-4 items-center px-4 py-3 cursor-pointer transition-colors relative overflow-hidden rounded-lg border"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        borderColor: 'var(--color-border-default)',
      }}
    >
      {/* Status gradient glow on right edge */}
      <div
        className="absolute inset-y-0 right-0 w-32 pointer-events-none"
        style={{
          background: `linear-gradient(to left, ${glowColor}, transparent)`,
        }}
      />

      {/* Name */}
      <div className="flex items-center gap-2 min-w-0">
        {isPrice ? (
          <TrendingUp className="w-4 h-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
        ) : isCron ? (
          <Clock className="w-4 h-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
        ) : (
          <Timer className="w-4 h-4 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />
        )}
        <span className="text-sm truncate font-medium" style={{ color: 'var(--color-text-primary)' }}>{automation.name}</span>
      </div>

      {/* Schedule */}
      <span className="text-sm truncate" style={{ color: 'var(--color-text-secondary)' }}>
        {schedule}
      </span>

      {/* Agent Mode */}
      <span
        className="text-xs uppercase font-mono tracking-wide"
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {automation.agent_mode as string}
      </span>

      {/* Next Run */}
      <span className="text-sm" style={{ color: 'var(--color-text-secondary)' }}>
        {isPrice ? t('automation.onTrigger') : automation.next_run_at ? formatRelativeTime(automation.next_run_at) : '\u2014'}
      </span>

      {/* Status */}
      <div className="flex justify-end relative z-10">
        <StatusBadge status={automation.status} />
      </div>
    </motion.div>
  );
}
