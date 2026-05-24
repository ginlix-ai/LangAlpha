/**
 * SiriusDashboard — Custom dashboard for the Sirius valuation template.
 *
 * Renders an Excel-like table: every row is a company, key quantitative
 * numbers are exposed as dedicated columns for easy comparison.
 *
 * The `summary` JSONB written by skills/sirius-valuation/scripts/persist_entry.py
 * is the source of truth for what's shown here.
 */
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { MessageSquare, RefreshCw, Trash2 } from 'lucide-react';
import { EntryStatusBadge } from '../components/EntryStatusBadge';
import { useRerunEntry, useDeleteEntry } from '../hooks/useTemplates';
import type { TemplateEntry } from '@/types/template';

interface Props {
  templateId: string;
  entries: TemplateEntry[];
  isFetching?: boolean;
}

interface SiriusSummary {
  fair_value?: number;
  current_price?: number;
  upside_pct?: number;
  judgment?: string; // 低估 / 合理 / 高估
  company_type?: string;
  recommendation?: string;
  fair_value_adjusted?: number;
}

const JUDGMENT_COLOR: Record<string, string> = {
  低估: 'text-green-600',
  合理: 'text-yellow-600',
  高估: 'text-red-500',
};

export default function SiriusDashboard({ templateId, entries }: Props) {
  return (
    <div className="p-6">
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr className="text-left">
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">公司</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">代码</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">公允价值</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">当前价</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">安全边际</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-center">判断</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">类型</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">状态</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap">更新时间</th>
                <th className="px-3 py-2.5 font-medium whitespace-nowrap text-right">操作</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e) => (
                <SiriusRow key={e.entry_id} templateId={templateId} entry={e} />
              ))}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function SiriusRow({
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
  const s = (entry.summary ?? {}) as SiriusSummary;

  const fairValue = s.fair_value_adjusted ?? s.fair_value;
  const upside = s.upside_pct;
  const judgmentColor = s.judgment ? JUDGMENT_COLOR[s.judgment] : '';
  const isCompleted = entry.status === 'completed';
  const canRerun = entry.status === 'completed' || entry.status === 'failed';
  const canDelete = entry.status !== 'analyzing';

  const upsideClass =
    upside !== undefined && upside !== null
      ? upside > 0
        ? 'text-green-600'
        : upside < 0
        ? 'text-red-500'
        : ''
      : '';

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
          {entry.display_name ?? entry.entry_key}
        </td>
        <td className="px-3 py-2.5 font-mono text-xs text-muted-foreground">
          {entry.entry_key}
        </td>
        <td className="px-3 py-2.5 text-right font-medium">
          {isCompleted ? formatNum(fairValue) : '—'}
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
        <td className="px-3 py-2.5 text-muted-foreground">
          {s.company_type ?? '—'}
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

      {/* Delete confirmation dialog */}
      {confirmDelete && (
        <tr>
          <td colSpan={10} className="px-3 py-2 bg-red-500/5 border-t">
            <div className="flex items-center justify-between">
              <span className="text-sm text-red-600">
                确认删除「{entry.display_name ?? entry.entry_key}」？工作区和分析数据将被永久删除。
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
