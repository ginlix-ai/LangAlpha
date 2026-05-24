/**
 * EntryStatusBadge — small visual indicator for an entry's analysis status.
 */
import { AlertTriangle, CheckCircle2, Clock, Loader2, XCircle } from 'lucide-react';
import type { TemplateEntryStatus } from '@/types/template';

const CONFIG: Record<
  TemplateEntryStatus,
  {
    label: string;
    icon: React.ComponentType<{ className?: string }>;
    color: string;
  }
> = {
  pending: { label: '等待中', icon: Clock, color: 'text-muted-foreground' },
  analyzing: { label: '分析中', icon: Loader2, color: 'text-blue-500' },
  completed: { label: '已完成', icon: CheckCircle2, color: 'text-green-600' },
  partial: { label: '部分完成', icon: AlertTriangle, color: 'text-amber-600' },
  failed: { label: '失败', icon: XCircle, color: 'text-red-500' },
};

export function EntryStatusBadge({ status }: { status: TemplateEntryStatus }) {
  const cfg = CONFIG[status] ?? CONFIG.pending;
  const Icon = cfg.icon;
  const animate = status === 'analyzing' || status === 'pending';
  return (
    <span className={`inline-flex items-center gap-1 text-xs ${cfg.color}`}>
      <Icon className={`h-3.5 w-3.5 ${animate ? 'animate-spin' : ''}`} />
      {cfg.label}
    </span>
  );
}

