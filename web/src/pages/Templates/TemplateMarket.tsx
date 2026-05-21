/**
 * TemplateMarket — Landing page at /templates.
 *
 * Shows a card for each registered template. Clicking a card navigates to
 * /templates/:id (the template home page).
 */
import { useNavigate } from 'react-router-dom';
import { TrendingUp, LayoutTemplate, Loader2, ArrowLeft } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { useTemplateManifests } from './hooks/useTemplates';
import type { TemplateManifest } from '@/types/template';

const ICON_MAP: Record<string, React.ComponentType<{ className?: string }>> = {
  'trending-up': TrendingUp,
};

export default function TemplateMarket() {
  const navigate = useNavigate();
  const { data, isLoading, error } = useTemplateManifests();

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin mr-2" /> 加载模板…
      </div>
    );
  }
  if (error) {
    return (
      <div className="p-6 text-red-500">
        加载模板失败：{(error as any)?.message || String(error)}
      </div>
    );
  }

  const templates = data?.templates ?? [];

  return (
    <div className="p-6 max-w-6xl mx-auto h-full overflow-auto">
      <div className="mb-6">
        <button
          onClick={() => navigate('/chat')}
          className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground mb-3 transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          返回工作区
        </button>
        <h1 className="text-2xl font-semibold">模板市场</h1>
        <p className="text-sm text-muted-foreground mt-1">
          选择一个模板开始自动化分析。每个模板会为每家公司创建独立工作区并按预设脚本运行 Agent。
        </p>
      </div>

      {templates.length === 0 ? (
        <div className="text-center text-muted-foreground py-12">
          <LayoutTemplate className="h-12 w-12 mx-auto mb-3 opacity-30" />
          暂无可用模板
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {templates.map((m) => (
            <TemplateCard
              key={m.id}
              manifest={m}
              onClick={() => navigate(`/chat/templates/${m.id}`)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function TemplateCard({
  manifest,
  onClick,
}: {
  manifest: TemplateManifest;
  onClick: () => void;
}) {
  const Icon = (manifest.icon && ICON_MAP[manifest.icon]) || LayoutTemplate;
  return (
    <Card
      className="cursor-pointer hover:shadow-md transition-shadow"
      onClick={onClick}
    >
      <CardHeader className="space-y-2">
        <div className="flex items-center gap-2">
          <Icon className="h-5 w-5 text-primary" />
          <CardTitle className="text-base">{manifest.name}</CardTitle>
        </div>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground line-clamp-3">
          {manifest.description}
        </p>
        {manifest.estimated_minutes ? (
          <p className="text-xs text-muted-foreground mt-2">
            预计耗时 ~{manifest.estimated_minutes} 分钟
          </p>
        ) : null}
      </CardContent>
    </Card>
  );
}
