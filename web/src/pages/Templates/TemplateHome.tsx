/**
 * TemplateHome — Per-template dashboard at /templates/:templateId.
 *
 * Renders:
 *   - Banner with template name + "+ 新增" button
 *   - The template-specific dashboard component (looked up by id from
 *     the local registry). Falls back to a generic table if the template
 *     hasn't registered a custom view.
 */
import { useMemo, useState, lazy, Suspense } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { ArrowLeft, Plus, Loader2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import {
  useTemplateEntries,
  useTemplateManifest,
} from './hooks/useTemplates';
import { InstantiateDialog } from './components/InstantiateDialog';
import { GenericEntryTable } from './components/GenericEntryTable';
import type { TemplateManifest } from '@/types/template';

// ---------------------------------------------------------------------------
// Custom dashboard registry — add new templates here.
// Each entry maps a template id to a lazy-loaded dashboard component that
// receives the entry list. If a template isn't listed, we fall back to
// GenericEntryTable.
// ---------------------------------------------------------------------------
const SiriusDashboard = lazy(
  () => import('./sirius/SiriusDashboard'),
);

const CUSTOM_DASHBOARDS: Record<
  string,
  React.LazyExoticComponent<React.ComponentType<any>>
> = {
  'sirius-valuation': SiriusDashboard,
};

export default function TemplateHome() {
  const { templateId } = useParams<{ templateId: string }>();
  const navigate = useNavigate();
  const [dialogOpen, setDialogOpen] = useState(false);

  const manifestQ = useTemplateManifest(templateId);
  const entriesQ = useTemplateEntries(templateId, { limit: 200 });

  const CustomDashboard = useMemo(
    () => (templateId ? CUSTOM_DASHBOARDS[templateId] : undefined),
    [templateId],
  );

  if (!templateId) {
    return <div className="p-6">缺少 template id</div>;
  }

  if (manifestQ.isLoading) {
    return (
      <div className="flex items-center justify-center h-full text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin mr-2" /> 加载模板…
      </div>
    );
  }
  if (manifestQ.error || !manifestQ.data) {
    return (
      <div className="p-6 text-red-500">
        模板未找到或加载失败：{(manifestQ.error as any)?.message}
      </div>
    );
  }

  const manifest = manifestQ.data;
  const entries = entriesQ.data?.entries ?? [];

  return (
    <div className="h-full flex flex-col">
      {/* Header */}
      <div className="border-b px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => navigate('/chat/templates')}
            className="gap-1"
          >
            <ArrowLeft className="h-4 w-4" />
            模板市场
          </Button>
          <div className="h-5 w-px bg-border" />
          <div>
            <div className="text-base font-semibold">{manifest.name}</div>
            <div className="text-xs text-muted-foreground">
              {entriesQ.data
                ? `${entriesQ.data.total} 个分析`
                : '加载中…'}
            </div>
          </div>
        </div>

        <Button onClick={() => setDialogOpen(true)} className="gap-1">
          <Plus className="h-4 w-4" />
          新增分析
        </Button>
      </div>

      {/* Template description banner (shown when there are entries, collapses to save space) */}
      <TemplateBanner templateId={templateId} manifest={manifest} />

      {/* Body */}
      <div className="flex-1 overflow-auto">
        {entriesQ.isLoading ? (
          <div className="flex items-center justify-center h-full text-muted-foreground">
            <Loader2 className="h-5 w-5 animate-spin mr-2" /> 加载中…
          </div>
        ) : entries.length === 0 ? (
          <EmptyState onAdd={() => setDialogOpen(true)} />
        ) : CustomDashboard ? (
          <Suspense
            fallback={
              <div className="flex items-center justify-center h-32 text-muted-foreground">
                <Loader2 className="h-5 w-5 animate-spin" />
              </div>
            }
          >
            <CustomDashboard
              templateId={templateId}
              entries={entries}
              isFetching={entriesQ.isFetching}
            />
          </Suspense>
        ) : (
          <GenericEntryTable templateId={templateId} entries={entries} />
        )}
      </div>

      <InstantiateDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        manifest={manifest}
      />
    </div>
  );
}

function EmptyState({ onAdd }: { onAdd: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center px-6 py-12">
      <Plus className="h-12 w-12 opacity-20 mb-3" />
      <div className="text-base font-medium mb-1">还没有分析</div>
      <div className="text-sm text-muted-foreground mb-4">
        点击下方按钮添加第一个分析公司
      </div>
      <Button onClick={onAdd} className="gap-1">
        <Plus className="h-4 w-4" />
        新增分析
      </Button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Template-specific description banners (keyed by template id)
// ---------------------------------------------------------------------------

const TEMPLATE_BANNERS: Record<string, React.ReactNode> = {
  'sirius-valuation': (
    <div className="px-6 pt-3 pb-1 max-w-4xl">
      <p className="text-xs text-muted-foreground leading-relaxed">
        <strong className="text-foreground/70">Sirius 七维度估值：</strong>
        D1 商业模式 → D2 护城河 → D3 外部环境 → D4 管理层 → D5 MD&A → D6 综合评估 → D7 定性调整（修正 DCF/PEG/PS 假设，选敏感性矩阵坐标，输出公允价值与买入建议）。
        数据来源 FMP API，WACC 自动计算，三场景 DCF + PEG + PS 交叉验证。
      </p>
    </div>
  ),
};

function TemplateBanner({
  templateId,
  manifest,
}: {
  templateId: string;
  manifest: TemplateManifest;
}) {
  const banner = TEMPLATE_BANNERS[templateId];
  if (!banner) return null;
  return <>{banner}</>;
}
