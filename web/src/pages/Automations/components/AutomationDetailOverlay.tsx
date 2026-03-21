import React, { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import { useIsMobile } from '@/hooks/useIsMobile';
import {
  X,
  Pause,
  Play,
  Zap,
  Pencil,
  Trash2,
  Clock,
  Timer,
  TrendingUp,
  ExternalLink,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { ScrollArea } from '@/components/ui/scroll-area';
import { MobileBottomSheet } from '@/components/ui/mobile-bottom-sheet';
import StatusBadge from './StatusBadge';
import ExecutionHistoryTable from './ExecutionHistoryTable';
import { useExecutions } from '../hooks/useExecutions';
import { cronToHuman } from '../utils/cron';
import { formatPriceTrigger, formatRetriggerMode } from '../utils/price';
import { formatRelativeTime, formatDateTime } from '../utils/time';
import type { Automation } from '@/types/automation';

interface StatCardProps {
  label: string;
  value: string;
  sub?: string | null;
}

function StatCard({ label, value, sub }: StatCardProps) {
  return (
    <div
      className="rounded-lg px-3 py-2 border"
      style={{
        backgroundColor: 'var(--color-bg-card)',
        borderColor: 'var(--color-border-default)',
      }}
    >
      <p className="text-[10px] uppercase tracking-wider mb-1" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </p>
      <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{value}</p>
      {sub && (
        <p className="text-[11px] mt-0.5" style={{ color: 'var(--color-text-secondary)' }}>
          {sub}
        </p>
      )}
    </div>
  );
}

interface AutomationDetailOverlayProps {
  automation: Automation;
  onClose: () => void;
  onEdit: (automation: Automation) => void;
  onDelete: (automation: Automation) => void;
  onPause: (id: string) => void;
  onResume: (id: string) => void;
  onTrigger: (id: string) => void;
  mutationsLoading: boolean;
}

export default function AutomationDetailOverlay({
  automation,
  onClose,
  onEdit,
  onDelete,
  onPause,
  onResume,
  onTrigger,
  mutationsLoading,
}: AutomationDetailOverlayProps) {
  const navigate = useNavigate();
  const { executions, loading: execLoading } = useExecutions(automation.automation_id as string);
  const isCron = automation.trigger_type === 'cron';
  const isPrice = automation.trigger_type === 'price';
  const isMobile = useIsMobile();

  const latestThreadExecution = useMemo(
    () => executions.find((e) => e.conversation_thread_id && (e.status === 'completed' || e.status === 'running')),
    [executions]
  );

  const detailContent = (
    <div className={isMobile ? "flex flex-col gap-4 pb-4" : "p-5 flex flex-col gap-5"}>
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2 min-w-0">
          {isPrice ? <TrendingUp className="w-5 h-5 shrink-0" style={{ color: 'var(--color-text-secondary)' }} /> : isCron ? <Clock className="w-5 h-5 shrink-0" style={{ color: 'var(--color-text-secondary)' }} /> : <Timer className="w-5 h-5 shrink-0" style={{ color: 'var(--color-text-secondary)' }} />}
          <h2 className="text-lg font-semibold truncate" style={{ color: 'var(--color-text-primary)' }}>{automation.name}</h2>
          <StatusBadge status={automation.status} />
        </div>
        {!isMobile && (
          <button
            onClick={onClose}
            className="p-1 rounded hover:bg-foreground/10 transition-colors shrink-0"
          >
            <X className="w-4 h-4" style={{ color: 'var(--color-text-secondary)' }} />
          </button>
        )}
      </div>

      {/* Action Buttons */}
      <div className="flex flex-wrap gap-2">
        {automation.status === 'active' ? (
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onPause(automation.automation_id as string)}
            disabled={mutationsLoading}
            className="hover:bg-[var(--color-warning-soft)] text-xs h-7 px-2.5"
            style={{ color: 'var(--color-warning)' }}
          >
            <Pause className="w-3 h-3 mr-1" /> Pause
          </Button>
        ) : automation.status === 'paused' ? (
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onResume(automation.automation_id as string)}
            disabled={mutationsLoading}
            className="hover:bg-[var(--color-profit-soft)] text-xs h-7 px-2.5"
            style={{ color: 'var(--color-profit)' }}
          >
            <Play className="w-3 h-3 mr-1" /> Resume
          </Button>
        ) : null}
        <Button
          size="sm"
          variant="ghost"
          onClick={() => onTrigger(automation.automation_id as string)}
          disabled={mutationsLoading}
          className="hover:bg-[var(--color-info-soft)] text-xs h-7 px-2.5"
          style={{ color: 'var(--color-info)' }}
        >
          <Zap className="w-3 h-3 mr-1" /> Trigger Now
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={() => onEdit(automation)}
          className="hover:bg-foreground/10 text-xs h-7 px-2.5"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          <Pencil className="w-3 h-3 mr-1" /> Edit
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={() => onDelete(automation)}
          className="hover:bg-[var(--color-loss-soft)] text-xs h-7 px-2.5"
          style={{ color: 'var(--color-loss)' }}
        >
          <Trash2 className="w-3 h-3 mr-1" /> Delete
        </Button>
      </div>

      {/* Latest Result Link */}
      {latestThreadExecution && (
        <button
          onClick={() => {
            const wsId = automation.workspace_id;
            const threadId = latestThreadExecution.conversation_thread_id as string;
            navigate(`/chat/t/${threadId}`, {
              state: wsId ? { workspaceId: wsId } : {},
            });
          }}
          className="flex items-center gap-2 px-3 py-2 rounded-lg border text-sm transition-colors hover:bg-foreground/5"
          style={{ borderColor: 'var(--color-border-default)', color: 'var(--color-accent-primary)' }}
        >
          <ExternalLink className="w-3.5 h-3.5" />
          View Latest Result
          <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            ({latestThreadExecution.status})
          </span>
        </button>
      )}

      {/* Stats Grid */}
      {isPrice ? (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <StatCard
            label="Trigger"
            value={formatPriceTrigger(automation.trigger_config)}
            sub={automation.trigger_config?.symbol || null}
          />
          <StatCard
            label="Retrigger"
            value={formatRetriggerMode(automation.trigger_config)}
          />
          <StatCard
            label="Last Run"
            value={automation.last_run_at ? formatRelativeTime(automation.last_run_at) : '\u2014'}
            sub={automation.last_run_at ? formatDateTime(automation.last_run_at) : null}
          />
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <StatCard
            label="Schedule"
            value={isCron ? cronToHuman(automation.cron_expression as string) : 'One-time'}
            sub={isCron ? `${automation.cron_expression} (${automation.timezone as string})` : `${automation.timezone as string}`}
          />
          <StatCard
            label="Next Run"
            value={automation.next_run_at ? formatRelativeTime(automation.next_run_at as string) : '\u2014'}
            sub={automation.next_run_at ? formatDateTime(automation.next_run_at as string) : null}
          />
          <StatCard
            label="Last Run"
            value={automation.last_run_at ? formatRelativeTime(automation.last_run_at) : '\u2014'}
            sub={automation.last_run_at ? formatDateTime(automation.last_run_at) : null}
          />
        </div>
      )}

      {/* Instruction */}
      <div>
        <p className="text-[10px] uppercase tracking-wider mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
          Instruction
        </p>
        <div
          className="rounded-lg px-3 py-2.5 text-sm whitespace-pre-wrap"
          style={{
            backgroundColor: 'var(--color-bg-input)',
            color: 'var(--color-text-secondary)',
          }}
        >
          {automation.instruction as string}
        </div>
      </div>

      {/* Configuration */}
      <div>
        <p className="text-[10px] uppercase tracking-wider mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
          Configuration
        </p>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-1.5 text-sm">
          <div className="flex justify-between">
            <span style={{ color: 'var(--color-text-secondary)' }}>Agent Mode</span>
            <span className="uppercase font-mono text-xs" style={{ color: 'var(--color-text-primary)' }}>{automation.agent_mode as string}</span>
          </div>
          <div className="flex justify-between">
            <span style={{ color: 'var(--color-text-secondary)' }}>Thread Strategy</span>
            <span className="text-xs" style={{ color: 'var(--color-text-primary)' }}>{automation.thread_strategy as string}</span>
          </div>
          {automation.workspace_id && (
            <div className="flex justify-between">
              <span style={{ color: 'var(--color-text-secondary)' }}>Workspace</span>
              <span className="text-xs truncate max-w-[160px]" style={{ color: 'var(--color-text-primary)' }}>{(automation.workspace_id as string).slice(0, 8)}...</span>
            </div>
          )}
          <div className="flex justify-between">
            <span style={{ color: 'var(--color-text-secondary)' }}>Failures</span>
            <span className="text-xs" style={{ color: 'var(--color-text-primary)' }}>{automation.failure_count as number} / {automation.max_failures as number}</span>
          </div>
          {(automation.delivery_config as any)?.methods?.length > 0 && (
            <div className="flex justify-between">
              <span style={{ color: 'var(--color-text-secondary)' }}>Delivery</span>
              <span className="text-xs capitalize" style={{ color: 'var(--color-text-primary)' }}>
                {((automation.delivery_config as any).methods as string[]).join(', ')}
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Execution History */}
      <div>
        <p className="text-[10px] uppercase tracking-wider mb-1.5" style={{ color: 'var(--color-text-secondary)' }}>
          Execution History
        </p>
        <ExecutionHistoryTable executions={executions} loading={execLoading} workspaceId={automation.workspace_id} />
      </div>
    </div>
  );

  if (isMobile) {
    return (
      <MobileBottomSheet open onClose={onClose} height="85vh">
        {detailContent}
      </MobileBottomSheet>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: 10 }}
      transition={{ duration: 0.2 }}
      className="absolute inset-0 z-20 rounded-xl border overflow-hidden flex flex-col"
      style={{
        backgroundColor: 'var(--color-bg-elevated)',
        borderColor: 'var(--color-border-elevated)',
      }}
    >
      <ScrollArea className="flex-1">{detailContent}</ScrollArea>
    </motion.div>
  );
}
