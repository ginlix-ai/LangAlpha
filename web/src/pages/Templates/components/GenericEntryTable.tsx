/**
 * GenericEntryTable — Fallback table view for templates without a
 * custom dashboard. Lists entries with status badge + view-in-chat link.
 */
import { useNavigate } from 'react-router-dom';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { MessageSquare, RefreshCw } from 'lucide-react';
import { EntryStatusBadge } from './EntryStatusBadge';
import { useRerunEntry } from '../hooks/useTemplates';
import type { TemplateEntry } from '@/types/template';

interface Props {
  templateId: string;
  entries: TemplateEntry[];
}

export function GenericEntryTable({ templateId, entries }: Props) {
  const navigate = useNavigate();
  const rerun = useRerunEntry(templateId);

  return (
    <div className="p-6">
      <Card>
        <table className="w-full text-sm">
          <thead className="bg-muted/50">
            <tr className="text-left">
              <th className="px-4 py-2 font-medium">名称</th>
              <th className="px-4 py-2 font-medium">代码</th>
              <th className="px-4 py-2 font-medium">状态</th>
              <th className="px-4 py-2 font-medium">更新时间</th>
              <th className="px-4 py-2 font-medium text-right">操作</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((e) => (
              <tr key={e.entry_id} className="border-t hover:bg-muted/30">
                <td className="px-4 py-2">{e.display_name ?? e.entry_key}</td>
                <td className="px-4 py-2 font-mono text-xs">{e.entry_key}</td>
                <td className="px-4 py-2">
                  <EntryStatusBadge status={e.status} />
                </td>
                <td className="px-4 py-2 text-muted-foreground">
                  {new Date(e.updated_at).toLocaleString()}
                </td>
                <td className="px-4 py-2">
                  <div className="flex items-center justify-end gap-2">
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => navigate(`/chat/${e.workspace_id}`)}
                      title="打开聊天"
                    >
                      <MessageSquare className="h-4 w-4" />
                    </Button>
                    {(e.status === 'completed' || e.status === 'failed') && (
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => rerun.mutate(e.entry_id)}
                        disabled={rerun.isPending}
                        title="重跑分析"
                      >
                        <RefreshCw className="h-4 w-4" />
                      </Button>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
