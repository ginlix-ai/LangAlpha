/**
 * EviDashboard — Custom dashboard for the EVI Strategy template.
 *
 * Each row is a company. EVI provides multi-segment valuation, so the
 * key columns expose the *group level* fair value plus number of
 * segments and open monitor tasks (to make "this one needs attention"
 * obvious at a glance).
 *
 * The `summary` JSONB written by skills/evi-toolkit/scripts/evi_persist_entry.py
 * is the source of truth for what's shown here.
 */
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  MessageSquare,
  RefreshCw,
  Trash2,
  Layers,
  Bell,
} from 'lucide-react';
import { EntryStatusBadge } from '../components/EntryStatusBadge';
import { useRerunEntry, useDeleteEntry } from '../hooks/useTemplates';
import type { TemplateEntry } from '@/types/template';

interface Props {
  templateId: string;
  entries: TemplateEntry[];
  isFetching?: boolean;
}

interface EviSummary {
  company_name?: string;
  fair_value_base?: number;
  fair_value_bear?: number;
  fair_value_bull?: number;
  current_price?: number;
  upside_pct?: number;
  judgment?: string; // 低估 / 合理 / 高估
  currency_unit?: string;
  n_segments?: number;
  monitor_open_tasks?: number;
  schema_version?: string;
  // v2 fields
  checklist_overall?: 'ok' | 'partial' | 'blocked';
  checklist_missing?: number;
}

const JUDGMENT_COLOR: Record<string, string> = {
  低估: 'text-green-600',
  合理: 'text-yellow-600',
  高估: 'text-red-500',
};

const CHECKLIST_BADGE: Record<string, { text: string; cls: string }> = {
  ok:      { text: '✅ 齐备', cls: 'text-emerald-600' },
  partial: { text: '⚠️ 部分', cls: 'text-amber-600' },
  blocked: { text: '❌ 阻塞', cls: 'text-red-500' },
};

export default function EviDashboard({ templateId, entries }: Props) {
  return (
    <div className="p-6">
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr className="text-left">
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">公司</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">代码</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-center">数据</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">公允 (Base)</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">区间</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">当前价</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">空间</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-center">判断</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-center">分部</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-center">监控</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">状态</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">更新</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => (
                <EviRow key={e.entry_id} templateId={templateId} entry={e} />
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function EviRow({
  templateId,
  entry,
}: {
  templateId: string;
  entry: TemplateEntry;
}) {
  const navigate = useNavigate();
  const rerun = useRerunEntry(templateId);
  const deleteEntry = useDeleteEntry(templateId);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const s = (entry.summary ?? {}) as EviSummary;

  const isCompleted = entry.status === 'completed' || entry.status === 'partial';
  const canRerun = ['completed', 'partial', 'failed'].includes(entry.status);
  const canDelete = entry.status !== 'analyzing';

  const judgmentColor = s.judgment ? JUDGMENT_COLOR[s.judgment] : '';
  const upside = s.upside_pct;
  const upsideClass =
    upside !== undefined && upside !== null
      ? upside > 0
        ? 'text-green-600'
        : upside < 0
        ? 'text-red-500'
        : ''
      : '';

  const range =
    s.fair_value_bear !== undefined && s.fair_value_bull !== undefined
      ? `${formatNum(s.fair_value_bear)} – ${formatNum(s.fair_value_bull)}`
      : '—';

  const handleDelete = () => {
    deleteEntry.mutate(entry.entry_id, {
      onSuccess: () => setConfirmDelete(false),
    });
  };

  return (
    <>
      <tr
        className="border-t hover:bg-muted/30 cursor-pointer"
        onClick={() => navigate(`/chat/${entry.workspace_id}`)}
      >
        <td className="px-3 py-2.5 font-medium">
          {s.company_name ?? entry.display_name ?? entry.entry_key}
        </td>
        <td className="px-3 py-2.5 font-mono text-xs text-muted-foreground">
          {entry.entry_key}
        </td>
        <td className="px-3 py-2.5 text-center">
          {(() => {
            const k = s.checklist_overall;
            const meta = k ? CHECKLIST_BADGE[k] : undefined;
            if (!meta) return <span className="text-muted-foreground">—</span>;
            return (
              <span className={`text-xs whitespace-nowrap ${meta.cls}`}>
                {meta.text}
                {s.checklist_missing ? ` (${s.checklist_missing})` : ''}
              </span>
            );
          })()}
        </td>
        <td className="px-3 py-2.5 text-right font-medium">
          {isCompleted ? formatNum(s.fair_value_base) : '—'}
        </td>
        <td className="px-3 py-2.5 text-right text-xs text-muted-foreground whitespace-nowrap">
          {isCompleted ? range : '—'}
        </td>
        <td className="px-3 py-2.5 text-right">
          {isCompleted ? formatNum(s.current_price) : '—'}
        </td>
        <td className={`px-3 py-2.5 text-right font-medium ${upsideClass}`}>
          {isCompleted && upside !== undefined && upside !== null
            ? `${upside > 0 ? '+' : ''}${upside.toFixed(1)}%`
            : '—'}
        </td>
        <td className={`px-3 py-2.5 text-center font-medium ${judgmentColor}`}>
          {isCompleted ? s.judgment ?? '—' : '—'}
        </td>
        <td className="px-3 py-2.5 text-center">
          {s.n_segments !== undefined && s.n_segments !== null ? (
            <span className="inline-flex items-center gap-1 text-muted-foreground">
              <Layers className="h-3.5 w-3.5" />
              {s.n_segments}
            </span>
          ) : (
            '—'
          )}
        </td>
        <td className="px-3 py-2.5 text-center">
          {s.monitor_open_tasks ? (
            <span className="inline-flex items-center gap-1 text-amber-600">
              <Bell className="h-3.5 w-3.5" />
              {s.monitor_open_tasks}
            </span>
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </td>
        <td className="px-3 py-2.5">
          <EntryStatusBadge status={entry.status} />
        </td>
        <td className="px-3 py-2.5 text-muted-foreground whitespace-nowrap">
          {formatTime(entry.updated_at)}
        </td>
        <td className="px-3 py-2.5">
          <div
            className="flex items-center justify-end gap-1"
            onClick={(ev) => ev.stopPropagation()}
          >
            <Button
              size="sm"
              variant="ghost"
              onClick={() => navigate(`/chat/${entry.workspace_id}`)}
              title="打开聊天"
            >
              <MessageSquare className="h-4 w-4" />
            </Button>
            {canRerun && (
              <Button
                size="sm"
                variant="ghost"
                onClick={() => rerun.mutate(entry.entry_id)}
                disabled={rerun.isPending}
                title="重新分析"
              >
                <RefreshCw
                  className={`h-4 w-4 ${rerun.isPending ? 'animate-spin' : ''}`}
                />
              </Button>
            )}
            {canDelete && (
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setConfirmDelete(true)}
                className="text-muted-foreground hover:text-red-500"
                title="删除"
              >
                <Trash2 className="h-4 w-4" />
              </Button>
            )}
          </div>
        </td>
      </tr>

      {confirmDelete && (
        <tr>
          <td colSpan={13} className="px-3 py-2 bg-red-500/5 border-t">
            <div className="flex items-center justify-between">
              <span className="text-sm text-red-600">
                确认删除「{s.company_name ?? entry.display_name ?? entry.entry_key}」？工作区和分析数据将被永久删除。
              </span>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => setConfirmDelete(false)}
                >
                  取消
                </Button>
                <Button
                  size="sm"
                  variant="destructive"
                  onClick={handleDelete}
                  disabled={deleteEntry.isPending}
                >
                  {deleteEntry.isPending ? '删除中...' : '确认删除'}
                </Button>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function formatNum(n: number | undefined | null): string {
  if (n === undefined || n === null) return '—';
  if (typeof n !== 'number') return String(n);
  if (Math.abs(n) >= 1000) return n.toFixed(0);
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    const now = new Date();
    const sameDay =
      d.getFullYear() === now.getFullYear() &&
      d.getMonth() === now.getMonth() &&
      d.getDate() === now.getDate();
    if (sameDay) {
      return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }
    return d.toLocaleDateString();
  } catch {
    return iso;
  }
}
