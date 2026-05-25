/**
 * EviReportPanel v4 — 总分结构动态展示
 *
 * 顶层 Tab（固定）：
 *   1. 估值结论 — facets + 整体估值 + SOTP 汇总 + final.md
 *   2. {分部1} — 该分部的产业调研 + 估值（如 multi_segment）
 *   3. {分部2}
 *   ...
 *   N+1. 自动化任务 — automation 状态 + 监控记录
 *   N+2. 数据收集 — CHECKLIST + 数据索引（紧凑）
 *
 * single_segment 模式：
 *   1. 估值结论
 *   2. 公司产业调研（company_overview.md）
 *   3. 自动化任务
 *   4. 数据收集
 *
 * 报告页面带左侧 TOC（自动从 markdown h2/h3 提取）
 */
import React, { useState, useMemo, useRef, useCallback } from 'react';
import {
  TrendingUp, TrendingDown, Minus,
  Bell, Database, FileText,
  CheckCircle2, XCircle, AlertTriangle, BarChart3,
  Building2, List, Activity, History,
} from 'lucide-react';
import type { TemplateEntry } from '@/types/template';
import Markdown from '@/pages/ChatAgent/components/Markdown';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Scenario { bear?: number; base?: number; bull?: number; }

interface ChecklistItem {
  key: string;
  label: string;
  status: 'ok' | 'partial' | 'missing';
  detail?: string;
  last_updated?: string | null;
  severity?: 'blocking' | 'important' | 'nice_to_have';
}

interface Checklist {
  generated_at?: string;
  required_periods?: number;
  summary?: {
    total?: number;
    ok?: number;
    partial?: number;
    missing?: number;
    blocking_missing?: string[];
    overall?: 'ok' | 'partial' | 'blocked';
  };
  items?: ChecklistItem[];
}

interface ReportEntry {
  key: string;
  title: string;
  path: string;
  markdown: string;
  size_chars?: number;
  updated_at?: string;
  // v3 新增：报告归属（公司层 / 分部层）
  scope?: 'company' | 'segment' | 'overview';
  segment_id?: string;   // scope=segment 时有
  doc_type?: 'research' | 'valuation' | 'monitor' | 'final';
}

interface SegmentSummary {
  segment_id: string;
  name?: string;
  fair_value_share?: Scenario;        // 折成每股的份额
  fair_value_segment?: Scenario;      // 该 segment 的 EV
  contribution_pct_base?: number;
  primary_method?: string;
  confidence?: number;
  revenue_share_pct?: number;
}

interface Facets {
  company_name?: string;
  structure_type?: 'single_segment' | 'multi_segment';
  currency_unit?: string;
  fair_value?: Scenario;
  current_price?: number;
  upside_pct?: number | Scenario;
  judgment?: string;
  n_segments?: number;
  segments?: SegmentSummary[];        // multi_segment 才有
  key_drivers?: string[];
  key_risks?: string[];
  rerate_triggers?: Array<{
    metric: string;
    threshold_down?: number;
    threshold_up?: number;
    current_value?: number;
    status?: string;
  }>;
  [k: string]: any;
}

interface MonitorBlock {
  last_run_id?: string | null;
  last_checked_at?: string | null;
  open_tasks?: any[];
  automation_id?: string;
  automation_schedule?: string;
  /** 已注册的监控项（多个 automation 任务） */
  monitors?: Array<{
    id?: string;
    name: string;
    type: 'metric' | 'event' | 'industry' | 'competitor' | 'custom';
    description?: string;
    schedule?: string;          // cron / 自然语言
    automation_id?: string;
    last_triggered_at?: string;
    trigger_count?: number;
    last_impact?: string;       // 最近一次发现了什么
    status?: 'active' | 'paused' | 'pending';
  }>;
}

interface FactsSummary {
  total?: number;
  by_segment?: Record<string, number>;
  high_reliability_pct?: number;
}

interface EviPayload {
  schema_version?: string;
  company?: { symbol?: string; display_name?: string; market?: string };
  facets?: Facets;
  checklist?: Checklist | null;
  reports?: ReportEntry[];
  monitor?: MonitorBlock;
  indexed_facts_summary?: FactsSummary;
  // v1 兼容字段
  segments?: Record<string, any>;
  group?: { final?: any; reverse_valuation?: any; assumption_ledger?: any };
}

interface Props {
  entry: TemplateEntry;
  onOpenFile?: (filePath: string) => void;
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

const JUDGMENT_MAP: Record<
  string,
  { color: string; bg: string; Icon: React.ComponentType<{ className?: string }> }
> = {
  低估: { color: 'text-emerald-600 dark:text-emerald-400', bg: 'bg-emerald-500/10', Icon: TrendingUp },
  合理: { color: 'text-amber-600 dark:text-amber-400', bg: 'bg-amber-500/10', Icon: Minus },
  高估: { color: 'text-red-500 dark:text-red-400', bg: 'bg-red-500/10', Icon: TrendingDown },
};

const STATUS_STYLE = {
  ok:      { Icon: CheckCircle2,  color: 'text-emerald-600' },
  partial: { Icon: AlertTriangle, color: 'text-amber-600' },
  missing: { Icon: XCircle,       color: 'text-red-500' },
} as const;

function fmt(n: unknown): string {
  if (n === null || n === undefined) return '—';
  if (typeof n !== 'number' || !isFinite(n)) return '—';
  if (Math.abs(n) >= 1000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

function fmtPct(n: unknown): string {
  if (n === null || n === undefined) return '—';
  if (typeof n !== 'number' || !isFinite(n)) return '—';
  return `${n > 0 ? '+' : ''}${n.toFixed(1)}%`;
}

function asScenario(v: unknown): Scenario {
  if (v === null || v === undefined) return {};
  if (typeof v === 'number') return { base: v };
  if (typeof v === 'object') {
    const o = v as Record<string, unknown>;
    const out: Scenario = {};
    if (typeof o.bear === 'number') out.bear = o.bear;
    if (typeof o.base === 'number') out.base = o.base;
    if (typeof o.bull === 'number') out.bull = o.bull;
    return out;
  }
  return {};
}

function pickBase(v: unknown): number | undefined {
  if (typeof v === 'number') return v;
  if (v && typeof v === 'object') {
    const o = v as Record<string, unknown>;
    if (typeof o.base === 'number') return o.base;
    if (typeof o.bear === 'number' && typeof o.bull === 'number') return (o.bear + o.bull) / 2;
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// TOC extraction
// ---------------------------------------------------------------------------

interface TocItem { id: string; text: string; level: number; }

function extractToc(md: string): TocItem[] {
  const lines = md.split('\n');
  const items: TocItem[] = [];
  for (const line of lines) {
    const m = line.match(/^(#{2,4})\s+(.+)$/);
    if (m) {
      const level = m[1].length;
      const text = m[2].replace(/[*_`#]/g, '').trim();
      const id = text
        .toLowerCase()
        .replace(/[^\w\u4e00-\u9fff]+/g, '-')
        .replace(/^-|-$/g, '');
      items.push({ id, text, level });
    }
  }
  return items;
}

// ---------------------------------------------------------------------------
// Report classification (for backward compat with v2 reports)
// ---------------------------------------------------------------------------

/**
 * 把 reports 按 scope/segment 分类：
 * - 公司层估值类报告（company-valuation）
 * - 公司层调研类报告（company-research）
 * - 分部报告（按 segment_id 归集，含 research + valuation）
 * - 监控类（monitor）
 * - 数据类（data）
 */
interface ClassifiedReports {
  companyValuation: ReportEntry[];   // valuation_summary, valuation, final, reverse_valuation
  companyResearch: ReportEntry[];    // company_overview
  bySegment: Record<string, ReportEntry[]>;  // 每个分部的所有报告
  monitor: ReportEntry[];
  data: ReportEntry[];
  others: ReportEntry[];
}

function classifyReports(reports: ReportEntry[]): ClassifiedReports {
  const out: ClassifiedReports = {
    companyValuation: [],
    companyResearch: [],
    bySegment: {},
    monitor: [],
    data: [],
    others: [],
  };

  for (const r of reports) {
    const key = (r.key || '').toLowerCase();
    const path = (r.path || '').toLowerCase();

    // 1) 显式 segment_id 字段最优先
    if (r.segment_id) {
      const list = out.bySegment[r.segment_id] || (out.bySegment[r.segment_id] = []);
      list.push(r);
      continue;
    }
    // 2) 路径在 segments/ 下
    const segMatch = path.match(/segments\/([^/]+?)(?:_valuation)?\.md$/);
    if (segMatch) {
      const segId = segMatch[1];
      const list = out.bySegment[segId] || (out.bySegment[segId] = []);
      list.push({ ...r, segment_id: segId });
      continue;
    }

    // 3) 公司层 — 估值
    if (
      key === 'final' ||
      key === 'valuation' ||
      key === 'valuation_summary' ||
      key === 'reverse_valuation'
    ) {
      out.companyValuation.push(r);
      continue;
    }
    // 4) 公司层 — 产业调研
    if (key === 'company_overview' || key === 'industry_research') {
      out.companyResearch.push(r);
      continue;
    }
    // 5) 监控
    if (key === 'monitor') {
      out.monitor.push(r);
      continue;
    }
    // 6) 数据
    if (key === 'data' || key === 'data_index') {
      out.data.push(r);
      continue;
    }
    // 7) v2 旧 key（向后兼容）
    if (
      key === 'segments' || key === 'facts' ||
      key === 'assumptions' || key === 'valuation_router'
    ) {
      out.others.push(r);
      continue;
    }
    out.others.push(r);
  }

  return out;
}

// ---------------------------------------------------------------------------
// Tab definitions (dynamic)
// ---------------------------------------------------------------------------

interface TabDef {
  key: string;
  label: string;
  Icon: React.ComponentType<{ className?: string }>;
  type: 'valuation' | 'segment' | 'changelog' | 'automation' | 'data';
  segment_id?: string;
}

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

export function EviReportPanel(props: Props) {
  return (
    <EviReportErrorBoundary entry={props.entry}>
      <EviReportPanelInner {...props} />
    </EviReportErrorBoundary>
  );
}

function EviReportPanelInner({ entry, onOpenFile }: Props) {
  const payload = (entry.payload ?? {}) as EviPayload;
  const facets = payload.facets ?? {};
  const checklist = payload.checklist ?? null;
  const reports = payload.reports ?? [];
  const monitor = payload.monitor ?? {};
  const factsSummary = payload.indexed_facts_summary ?? {};
  const company = payload.company ?? {};

  const judgment = facets.judgment;
  const judgmentInfo = judgment ? JUDGMENT_MAP[judgment] : undefined;
  const partial = entry.status === 'partial' || (!facets.fair_value);

  // Classify reports
  const classified = useMemo(() => classifyReports(reports), [reports]);

  // Build dynamic tabs
  const tabs = useMemo<TabDef[]>(() => {
    const out: TabDef[] = [];

    // 1) 估值结论（永远第一个）
    out.push({
      key: 'valuation',
      label: '估值结论',
      Icon: BarChart3,
      type: 'valuation',
    });

    // 2) 各分部 Tab（按 facets.segments 顺序，回退到 classified.bySegment）
    const segOrder = facets.segments?.map((s) => s.segment_id) || Object.keys(classified.bySegment);
    const seenSegs = new Set<string>();
    for (const segId of segOrder) {
      if (!segId || seenSegs.has(segId)) continue;
      seenSegs.add(segId);
      const segMeta = facets.segments?.find((s) => s.segment_id === segId);
      out.push({
        key: `segment:${segId}`,
        label: segMeta?.name || segId,
        Icon: Building2,
        type: 'segment',
        segment_id: segId,
      });
    }
    // 把 classified 中存在但 facets 中缺失的分部也加入
    for (const segId of Object.keys(classified.bySegment)) {
      if (seenSegs.has(segId)) continue;
      seenSegs.add(segId);
      out.push({
        key: `segment:${segId}`,
        label: segId,
        Icon: Building2,
        type: 'segment',
        segment_id: segId,
      });
    }

    // 3) 公司产业调研（如果是 single_segment 或没有分部 tab）
    if (out.filter((t) => t.type === 'segment').length === 0 && classified.companyResearch.length > 0) {
      out.push({
        key: 'company-research',
        label: '产业调研',
        Icon: FileText,
        type: 'segment',  // 用 segment 渲染逻辑
      });
    }

    // 4) 更新记录
    out.push({
      key: 'changelog',
      label: '更新记录',
      Icon: History,
      type: 'changelog',
    });

    // 5) 自动化任务
    out.push({
      key: 'automation',
      label: '自动化任务',
      Icon: Bell,
      type: 'automation',
    });

    // 6) 数据收集
    out.push({
      key: 'data',
      label: '数据收集',
      Icon: Database,
      type: 'data',
    });

    return out;
  }, [facets.segments, classified]);

  const [activeTabKey, setActiveTabKey] = useState<string>(tabs[0]?.key || 'valuation');
  const activeTab = tabs.find((t) => t.key === activeTabKey) || tabs[0];

  return (
    <div className="px-4 sm:px-6 lg:px-8 py-6 space-y-5 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-xl font-semibold leading-tight">
            {facets.company_name || company.display_name || entry.display_name || entry.entry_key}
          </div>
          <div className="text-xs text-muted-foreground mt-1 flex items-center gap-2">
            <span className="font-mono">{company.symbol ?? entry.entry_key}</span>
            {company.market && <span>· {company.market.toUpperCase()}</span>}
            {facets.structure_type && (
              <span>· {facets.structure_type === 'multi_segment' ? `SOTP (${facets.n_segments} 分部)` : '整体估值'}</span>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {entry.upgradable && (
            <UpgradeButton entry={entry} />
          )}
          {judgmentInfo && (
            <div className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 ${judgmentInfo.bg}`}>
              <judgmentInfo.Icon className={`h-3.5 w-3.5 ${judgmentInfo.color}`} />
              <span className={`text-xs font-medium ${judgmentInfo.color}`}>{judgment}</span>
            </div>
          )}
        </div>
      </div>

      {partial && (
        <div className="flex items-start gap-2 rounded-md border border-amber-300/50 bg-amber-500/5 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
          <AlertTriangle className="h-4 w-4 flex-shrink-0 mt-0.5" />
          <span>分析处于<b>部分完成</b>状态——可以让 Agent 继续完成剩余 Phase。</span>
        </div>
      )}

      {/* Top-level Tabs */}
      <div className="border-b overflow-x-auto">
        <div className="flex gap-0.5">
          {tabs.map((t) => {
            const active = t.key === activeTabKey;
            return (
              <button
                key={t.key}
                onClick={() => setActiveTabKey(t.key)}
                className={`flex items-center gap-1.5 px-4 py-2.5 text-sm whitespace-nowrap border-b-2 transition-colors ${
                  active
                    ? 'border-primary text-primary font-medium'
                    : 'border-transparent text-muted-foreground hover:text-foreground hover:border-muted-foreground/30'
                }`}
              >
                <t.Icon className="h-4 w-4" />
                {t.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Tab Content */}
      {activeTab.type === 'valuation' && (
        <ValuationTab
          facets={facets}
          companyValuation={classified.companyValuation}
          others={classified.others}
          onOpenFile={onOpenFile}
        />
      )}

      {activeTab.type === 'segment' && activeTab.segment_id && (
        <SegmentTab
          segmentId={activeTab.segment_id}
          segmentMeta={facets.segments?.find((s) => s.segment_id === activeTab.segment_id)}
          reports={classified.bySegment[activeTab.segment_id] || []}
          currencyUnit={facets.currency_unit}
          onOpenFile={onOpenFile}
        />
      )}

      {activeTab.type === 'segment' && !activeTab.segment_id && (
        <CompanyResearchTab
          reports={classified.companyResearch}
          onOpenFile={onOpenFile}
        />
      )}

      {activeTab.type === 'changelog' && (
        <ChangelogTab reports={reports} onOpenFile={onOpenFile} />
      )}

      {activeTab.type === 'automation' && (
        <AutomationTab
          monitor={monitor}
          reports={classified.monitor}
          rerateTriggers={facets.rerate_triggers}
          onOpenFile={onOpenFile}
        />
      )}

      {activeTab.type === 'data' && (
        <DataTab
          checklist={checklist}
          reports={classified.data}
          factsSummary={factsSummary}
          onOpenFile={onOpenFile}
        />
      )}

      {/* Raw payload */}
      <RawPayloadCard payload={payload} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab: 估值结论
// ---------------------------------------------------------------------------

function ValuationTab({
  facets, companyValuation, others, onOpenFile,
}: {
  facets: Facets;
  companyValuation: ReportEntry[];
  others: ReportEntry[];
  onOpenFile?: (f: string) => void;
}) {
  const allReports = [...companyValuation, ...others];
  const finalReport = allReports.find((r) => r.key === 'final');
  const orderedReports = [
    ...(finalReport ? [finalReport] : []),
    ...allReports.filter((r) => r.key !== 'final'),
  ];

  const [activeIdx, setActiveIdx] = useState(0);

  return (
    <div className="space-y-4">
      {/* Summary card */}
      <SummaryCard facets={facets} />

      {/* Segment contribution chart (if multi_segment) */}
      {facets.segments && facets.segments.length > 0 && (
        <SegmentContributionCard segments={facets.segments} currencyUnit={facets.currency_unit} />
      )}

      {/* Reports tabs */}
      {orderedReports.length > 0 && (
        <>
          {orderedReports.length > 1 && (
            <div className="flex gap-1 border-b pb-0 overflow-x-auto">
              {orderedReports.map((r, i) => (
                <button
                  key={r.key}
                  onClick={() => setActiveIdx(i)}
                  className={`px-3 py-1.5 text-xs whitespace-nowrap rounded-t-md transition-colors ${
                    i === activeIdx
                      ? 'bg-primary/10 text-primary font-medium border-b-2 border-primary -mb-px'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  {r.title}
                </button>
              ))}
            </div>
          )}
          <ReportWithToc report={orderedReports[activeIdx] || orderedReports[0]} onOpenFile={onOpenFile} />
        </>
      )}

      {orderedReports.length === 0 && (
        <div className="text-center py-12 text-muted-foreground text-sm">
          估值报告尚未生成。请完成 Phase 2 估值分析。
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab: 分部
// ---------------------------------------------------------------------------

function SegmentTab({
  segmentId, segmentMeta, reports, currencyUnit, onOpenFile,
}: {
  segmentId: string;
  segmentMeta?: SegmentSummary;
  reports: ReportEntry[];
  currencyUnit?: string;
  onOpenFile?: (f: string) => void;
}) {
  // 估值类报告优先
  const valuationReport = reports.find(
    (r) => r.key.includes('valuation') || (r.path || '').includes('_valuation')
  );
  const researchReport = reports.find(
    (r) => !r.key.includes('valuation') && !(r.path || '').includes('_valuation')
  );

  const orderedReports = [
    ...(valuationReport ? [valuationReport] : []),
    ...(researchReport ? [researchReport] : []),
    ...reports.filter((r) => r !== valuationReport && r !== researchReport),
  ];

  const [activeIdx, setActiveIdx] = useState(0);

  return (
    <div className="space-y-4">
      {/* 分部估值快照（若有） */}
      {segmentMeta && segmentMeta.fair_value_share && (
        <div className="rounded-lg border bg-card p-4">
          <div className="text-sm font-medium mb-3">{segmentMeta.name || segmentId} 估值快照</div>
          <div className="grid grid-cols-3 gap-2 sm:gap-4">
            <Stat label="Bear" value={fmt(segmentMeta.fair_value_share.bear)} unit={currencyUnit} tone="bear" />
            <Stat label="Base" value={fmt(segmentMeta.fair_value_share.base)} unit={currencyUnit} tone="base" highlight />
            <Stat label="Bull" value={fmt(segmentMeta.fair_value_share.bull)} unit={currencyUnit} tone="bull" />
          </div>
          <div className="mt-3 grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            {segmentMeta.contribution_pct_base !== undefined && (
              <KV label="基准贡献" value={`${segmentMeta.contribution_pct_base.toFixed(1)}%`} />
            )}
            {segmentMeta.revenue_share_pct !== undefined && (
              <KV label="收入占比" value={`${segmentMeta.revenue_share_pct.toFixed(1)}%`} />
            )}
            {segmentMeta.primary_method && (
              <KV label="主估值方法" value={segmentMeta.primary_method} />
            )}
            {segmentMeta.confidence !== undefined && (
              <KV label="置信" value={`${(segmentMeta.confidence * 100).toFixed(0)}%`} />
            )}
          </div>
        </div>
      )}

      {/* Reports tabs */}
      {orderedReports.length > 0 ? (
        <>
          {orderedReports.length > 1 && (
            <div className="flex gap-1 border-b pb-0 overflow-x-auto">
              {orderedReports.map((r, i) => (
                <button
                  key={r.key}
                  onClick={() => setActiveIdx(i)}
                  className={`px-3 py-1.5 text-xs whitespace-nowrap rounded-t-md transition-colors ${
                    i === activeIdx
                      ? 'bg-primary/10 text-primary font-medium border-b-2 border-primary -mb-px'
                      : 'text-muted-foreground hover:text-foreground'
                  }`}
                >
                  {r.title}
                </button>
              ))}
            </div>
          )}
          <ReportWithToc report={orderedReports[activeIdx] || orderedReports[0]} onOpenFile={onOpenFile} />
        </>
      ) : (
        <div className="text-center py-12 text-muted-foreground text-sm">
          该分部的报告尚未生成。
        </div>
      )}
    </div>
  );
}

function CompanyResearchTab({
  reports, onOpenFile,
}: { reports: ReportEntry[]; onOpenFile?: (f: string) => void }) {
  const [activeIdx, setActiveIdx] = useState(0);

  if (reports.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground text-sm">
        产业调研报告尚未生成。
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {reports.length > 1 && (
        <div className="flex gap-1 border-b pb-0 overflow-x-auto">
          {reports.map((r, i) => (
            <button
              key={r.key}
              onClick={() => setActiveIdx(i)}
              className={`px-3 py-1.5 text-xs whitespace-nowrap rounded-t-md transition-colors ${
                i === activeIdx
                  ? 'bg-primary/10 text-primary font-medium border-b-2 border-primary -mb-px'
                  : 'text-muted-foreground hover:text-foreground'
              }`}
            >
              {r.title}
            </button>
          ))}
        </div>
      )}
      <ReportWithToc report={reports[activeIdx] || reports[0]} onOpenFile={onOpenFile} />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Upgrade Button — 右上角显示，当 entry.upgradable = true
// ---------------------------------------------------------------------------

function UpgradeButton({ entry }: { entry: { entry_id: string; template_id: string; workspace_id: string; current_version?: string; latest_version?: string } }) {
  const [loading, setLoading] = useState(false);
  const [upgradeResult, setUpgradeResult] = useState<{
    from_version: string;
    to_version: string;
    release_notes: Array<{
      version: string;
      summary: string;
      changes: string[];
      suggested_actions?: Array<{ label: string; prompt: string }>;
    }>;
  } | null>(null);
  const [error, setError] = useState('');

  const handleUpgrade = async () => {
    setLoading(true);
    setError('');
    try {
      const resp = await fetch(
        `/api/v1/templates/${entry.template_id}/entries/${entry.entry_id}/upgrade`,
        { method: 'POST' }
      );
      if (!resp.ok) {
        const data = await resp.json().catch(() => ({}));
        throw new Error(data.detail || `HTTP ${resp.status}`);
      }
      const data = await resp.json();
      setUpgradeResult(data);
    } catch (e: any) {
      setError(e.message || '升级失败');
    } finally {
      setLoading(false);
    }
  };

  // 发送建议操作给 Agent（通过对话框）
  const sendToAgent = (prompt: string) => {
    // 跳转到对话页面并发送消息
    const chatUrl = `/chat/${entry.workspace_id}?auto_send=${encodeURIComponent(prompt)}`;
    window.location.href = chatUrl;
  };

  // 升级完成后：显示更新纪要 + 建议操作
  if (upgradeResult) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={() => setUpgradeResult(null)}>
        <div className="bg-card border rounded-xl shadow-xl max-w-lg w-full mx-4 max-h-[80vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
          <div className="px-5 py-4 border-b">
            <div className="flex items-center gap-2 text-sm font-medium text-emerald-600">
              <CheckCircle2 className="h-4 w-4" />
              模板已更新到 v{upgradeResult.to_version}
            </div>
            <div className="text-xs text-muted-foreground mt-1">
              从 v{upgradeResult.from_version} 升级
            </div>
          </div>

          {upgradeResult.release_notes.map((note) => (
            <div key={note.version} className="px-5 py-4 border-b last:border-b-0">
              <div className="text-sm font-medium mb-2">{note.summary}</div>
              <ul className="text-xs text-muted-foreground space-y-1.5 mb-4">
                {note.changes.map((c, i) => (
                  <li key={i} className="flex gap-2">
                    <span className="text-emerald-500 shrink-0">✓</span>
                    <span>{c}</span>
                  </li>
                ))}
              </ul>

              {note.suggested_actions && note.suggested_actions.length > 0 && (
                <div>
                  <div className="text-xs font-medium text-foreground mb-2">建议操作（点击立即执行）：</div>
                  <div className="flex flex-wrap gap-2">
                    {note.suggested_actions.map((action, i) => (
                      <button
                        key={i}
                        onClick={() => sendToAgent(action.prompt)}
                        className="text-xs px-3 py-1.5 rounded-full border border-blue-500/50 bg-blue-500/10 text-blue-500 hover:bg-blue-500/20 transition-colors"
                      >
                        {action.label}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}

          <div className="px-5 py-3 border-t bg-muted/20 flex justify-end">
            <button
              onClick={() => setUpgradeResult(null)}
              className="text-xs px-3 py-1.5 rounded bg-muted hover:bg-muted/80 transition-colors"
            >
              关闭
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="relative">
      <button
        onClick={handleUpgrade}
        disabled={loading}
        className="inline-flex items-center gap-1.5 rounded-full px-3 py-1.5 border border-blue-500/50 bg-blue-500/10 text-blue-500 text-xs font-medium hover:bg-blue-500/20 transition-colors disabled:opacity-50"
        title={`当前 v${entry.current_version || '0.0.0'} → 最新 v${entry.latest_version}`}
      >
        {loading ? (
          <span className="animate-spin h-3 w-3 border-2 border-blue-500 border-t-transparent rounded-full" />
        ) : (
          <TrendingUp className="h-3.5 w-3.5" />
        )}
        {loading ? '更新中...' : `更新到 v${entry.latest_version}`}
      </button>
      {error && (
        <div className="absolute top-full right-0 mt-1 px-2 py-1 rounded bg-red-500/10 text-red-500 text-[10px] whitespace-nowrap z-10">
          {error}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab: 更新记录（changelog.md）
// ---------------------------------------------------------------------------

function ChangelogTab({
  reports, onOpenFile,
}: { reports: ReportEntry[]; onOpenFile?: (f: string) => void }) {
  // 找 changelog.md
  const changelog = reports.find((r) => r.key === 'changelog');

  return (
    <div className="space-y-4">
      <div className="rounded-lg border bg-card p-4">
        <div className="text-sm font-medium mb-2 flex items-center gap-2">
          <History className="h-4 w-4" />
          更新记录
        </div>
        <div className="text-xs text-muted-foreground">
          AI 每次完成分析/修改/重估后自动追加。用户指正、监控触发的变更都会记录在此。
        </div>
      </div>

      {changelog ? (
        <ReportWithToc report={changelog} onOpenFile={onOpenFile} />
      ) : (
        <div className="rounded-lg border bg-card p-6 text-center">
          <div className="text-muted-foreground text-sm mb-2">暂无更新记录</div>
          <div className="text-xs text-muted-foreground">
            首次分析完成后，AI 会在 <code className="text-foreground">reports/changelog.md</code> 追加变更纪要。<br />
            后续每次修改（用户指正 / 监控触发 / 数据刷新）都会自动记录。
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab: 自动化任务
// ---------------------------------------------------------------------------

function AutomationTab({
  monitor, reports, rerateTriggers, onOpenFile,
}: {
  monitor: MonitorBlock;
  reports: ReportEntry[];
  rerateTriggers?: Facets['rerate_triggers'];
  onOpenFile?: (f: string) => void;
}) {
  const open = monitor.open_tasks ?? [];
  const monitors = monitor.monitors ?? [];
  const totalTriggers = monitors.reduce((acc, m) => acc + (m.trigger_count ?? 0), 0);

  return (
    <div className="space-y-4">
      {/* Monitor status */}
      <div className="rounded-lg border bg-card p-4">
        <div className="text-sm font-medium mb-3 flex items-center gap-2">
          <Bell className="h-4 w-4" />
          监控状态
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div className="rounded-md border p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">已注册监控</div>
            <div className="text-sm font-medium mt-1">
              {monitors.length > 0 ? `${monitors.length} 项` : '未配置'}
            </div>
          </div>
          <div className="rounded-md border p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">累计触发</div>
            <div className={`text-sm font-medium mt-1 ${totalTriggers > 0 ? 'text-blue-500' : 'text-muted-foreground'}`}>
              {totalTriggers} 次
            </div>
          </div>
          <div className="rounded-md border p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">未处理任务</div>
            <div className={`text-sm font-medium mt-1 ${open.length ? 'text-amber-500' : 'text-emerald-500'}`}>
              {open.length} 个
            </div>
          </div>
          <div className="rounded-md border p-3">
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">最近扫描</div>
            <div className="text-xs font-medium mt-1 truncate">
              {monitor.last_checked_at ?? '尚未启动'}
            </div>
          </div>
        </div>
      </div>

      {/* 已注册的监控（多类型） */}
      {monitors.length > 0 ? (
        <div className="rounded-lg border bg-card">
          <div className="px-4 py-3 border-b text-sm font-medium flex items-center gap-2">
            <Activity className="h-4 w-4" />
            已注册监控（{monitors.length}）
          </div>
          <div className="divide-y">
            {monitors.map((m, i) => (
              <div key={m.id ?? i} className="px-4 py-3">
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <MonitorTypeBadge type={m.type} />
                      <span className="text-sm font-medium truncate">{m.name}</span>
                      {m.status === 'paused' && (
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground">已暂停</span>
                      )}
                    </div>
                    {m.description && (
                      <div className="text-xs text-muted-foreground mt-1">{m.description}</div>
                    )}
                    {m.last_impact && (
                      <div className="text-xs mt-1">
                        <span className="text-muted-foreground">最近影响：</span>
                        <span className="text-foreground">{m.last_impact}</span>
                      </div>
                    )}
                  </div>
                  <div className="text-right text-xs text-muted-foreground shrink-0">
                    {m.schedule && <div className="font-mono">{m.schedule}</div>}
                    <div>触发 {m.trigger_count ?? 0} 次</div>
                    {m.last_triggered_at && (
                      <div className="text-[10px]">{m.last_triggered_at.slice(0, 10)}</div>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <MonitorSetupGuide />
      )}

      {/* Rerate triggers — 与监控共生：阈值告警 */}
      {rerateTriggers && rerateTriggers.length > 0 && (
        <div className="rounded-lg border bg-card">
          <div className="px-4 py-3 border-b text-sm font-medium">
            重估触发指标（rerate_triggers）
            <span className="text-xs text-muted-foreground ml-2">
              · 由 evi-reverse-valuation 生成，定时监控会检查这些阈值
            </span>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-muted/30">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">指标</th>
                  <th className="text-right px-3 py-2 font-medium">当前值</th>
                  <th className="text-right px-3 py-2 font-medium">向下阈值</th>
                  <th className="text-right px-3 py-2 font-medium">向上阈值</th>
                  <th className="text-center px-3 py-2 font-medium">状态</th>
                </tr>
              </thead>
              <tbody>
                {rerateTriggers.map((t, i) => {
                  const breached = t.status?.includes('breached');
                  return (
                    <tr key={i} className="border-t">
                      <td className="px-3 py-2 font-medium">{t.metric}</td>
                      <td className="px-3 py-2 text-right">{fmt(t.current_value)}</td>
                      <td className="px-3 py-2 text-right text-muted-foreground">{fmt(t.threshold_down)}</td>
                      <td className="px-3 py-2 text-right text-muted-foreground">{fmt(t.threshold_up)}</td>
                      <td className="px-3 py-2 text-center">
                        {breached ? (
                          <span className="text-red-500">🔴 {t.status}</span>
                        ) : (
                          <span className="text-emerald-500">✓ 正常</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Open tasks */}
      {open.length > 0 && (
        <div className="rounded-lg border bg-card">
          <div className="px-4 py-3 border-b text-sm font-medium">待处理任务（{open.length}）</div>
          <div className="divide-y">
            {open.map((t: any, i: number) => (
              <div key={t.id ?? i} className="px-4 py-3">
                <div className="flex items-center justify-between">
                  <div className="text-sm font-medium">
                    {t.method ? `${t.method}` : t.id ?? `task_${i}`}
                    {t.segment_id && <span className="text-muted-foreground ml-2">· {t.segment_id}</span>}
                  </div>
                  {t.triggered_at && <span className="text-xs text-muted-foreground">{t.triggered_at}</span>}
                </div>
                {t.reason && <div className="text-xs text-muted-foreground mt-1">{t.reason}</div>}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Monitor reports */}
      {reports.length > 0 && reports.map((r) => (
        <ReportWithToc key={r.key} report={r} onOpenFile={onOpenFile} />
      ))}

      {open.length === 0 && reports.length === 0 && !rerateTriggers?.length && monitors.length === 0 && (
        <div className="text-center py-12 text-muted-foreground text-sm">
          暂无监控数据。请按下方说明让 Agent 注册监控。
        </div>
      )}
    </div>
  );
}

// ─── 监控类型 Badge ─────────────────────────────────
function MonitorTypeBadge({ type }: { type: string }) {
  const config: Record<string, { label: string; cls: string }> = {
    metric:     { label: '指标阈值', cls: 'bg-blue-500/10 text-blue-500 border-blue-500/30' },
    event:      { label: '事件型',   cls: 'bg-purple-500/10 text-purple-400 border-purple-500/30' },
    industry:   { label: '产业链',   cls: 'bg-amber-500/10 text-amber-500 border-amber-500/30' },
    competitor: { label: '竞品',     cls: 'bg-rose-500/10 text-rose-400 border-rose-500/30' },
    custom:     { label: '自定义',   cls: 'bg-gray-500/10 text-gray-400 border-gray-500/30' },
  };
  const c = config[type] ?? config.custom;
  return (
    <span className={`text-[10px] px-1.5 py-0.5 rounded border ${c.cls}`}>{c.label}</span>
  );
}

// ─── 未配置监控时的引导卡 ────────────────────────────
function MonitorSetupGuide() {
  const examples = [
    {
      type: 'metric' as const,
      title: '指标阈值监控（最常用）',
      desc: '监控 reverse_valuation 输出的关键指标，越线即重估',
      examples: [
        '机器人 LiDAR 季度出货量跌破 80% YoY 增速',
        '集团毛利率跌破 15%',
        '季度盈利转正（首次）',
      ],
    },
    {
      type: 'event' as const,
      title: '事件型监控',
      desc: '关注公司新发布的财报/公告/电话会，触发增量分析',
      examples: [
        '公司发布新财报或业绩预告（季报披露日）',
        '公司召开新品发布会、开发者大会',
        '管理层重大变动、再融资公告',
      ],
    },
    {
      type: 'industry' as const,
      title: '产业链监控',
      desc: '跟踪上下游关键数据，支撑业务量预测',
      examples: [
        '车厂月度新能源车销量（影响 ADAS LiDAR 装机）',
        '人形机器人量产/出货新闻（机器人 LiDAR 总盘子）',
        '关键零部件价格、供应链事件',
      ],
    },
    {
      type: 'competitor' as const,
      title: '竞品监控',
      desc: '跟踪 peer 估值/股价/产品发布对比',
      examples: [
        '禾赛科技、Innoviz、Luminar 季度业绩与出货',
        '竞品发布会、新车型定点',
        'Peer 估值倍数变化（PS/PE 中位数偏离）',
      ],
    },
  ];

  return (
    <div className="rounded-lg border bg-card">
      <div className="px-4 py-3 border-b">
        <div className="text-sm font-medium">还没配置监控？告诉 Agent 注册一个</div>
        <div className="text-xs text-muted-foreground mt-1">
          监控通过 <code className="text-foreground">automation</code> skill 注册定时任务。
          每次触发会扫描新材料 → 影响估值则调用 <code className="text-foreground">evi-revaluation-updater</code> 自动重估。
        </div>
      </div>
      <div className="divide-y">
        {examples.map((ex) => (
          <div key={ex.type} className="px-4 py-3">
            <div className="flex items-center gap-2 mb-1">
              <MonitorTypeBadge type={ex.type} />
              <span className="text-sm font-medium">{ex.title}</span>
            </div>
            <div className="text-xs text-muted-foreground mb-2">{ex.desc}</div>
            <ul className="text-xs text-muted-foreground space-y-1 ml-2">
              {ex.examples.map((e, i) => (
                <li key={i}>· {e}</li>
              ))}
            </ul>
          </div>
        ))}
      </div>
      <div className="px-4 py-3 border-t bg-muted/20 text-xs text-muted-foreground">
        💡 直接对 Agent 说："<span className="text-foreground">每周一帮我扫一次速腾的机器人销量、车厂月度交付、禾赛业绩，发现重大变化就重估</span>"
        即可自动注册。
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tab: 数据收集
// ---------------------------------------------------------------------------

function DataTab({
  checklist, reports, factsSummary, onOpenFile,
}: {
  checklist: Checklist | null;
  reports: ReportEntry[];
  factsSummary: FactsSummary;
  onOpenFile?: (f: string) => void;
}) {
  return (
    <div className="space-y-4">
      {checklist && <ChecklistCard checklist={checklist} />}

      {/* Facts summary */}
      {factsSummary.total !== undefined && factsSummary.total > 0 && (
        <div className="rounded-lg border bg-card p-3 flex items-center gap-4 text-xs flex-wrap">
          <span className="font-medium">事实索引：</span>
          <span>共 <b>{factsSummary.total}</b> 条</span>
          {factsSummary.high_reliability_pct !== undefined && (
            <span>高可靠性 <b>{(factsSummary.high_reliability_pct * 100).toFixed(0)}%</b></span>
          )}
          {factsSummary.by_segment && Object.keys(factsSummary.by_segment).length > 0 && (
            <span className="text-muted-foreground">
              分布：{Object.entries(factsSummary.by_segment).map(([k, v]) => `${k}(${v})`).join(' · ')}
            </span>
          )}
        </div>
      )}

      {/* Checklist details */}
      {checklist?.items && checklist.items.length > 0 && (
        <div className="rounded-lg border bg-card">
          <div className="px-4 py-3 border-b text-sm font-medium flex items-center gap-2">
            <Database className="h-4 w-4" />
            数据收集明细
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="bg-muted/30">
                <tr>
                  <th className="text-left px-3 py-2 font-medium">数据项</th>
                  <th className="text-center px-3 py-2 font-medium">状态</th>
                  <th className="text-left px-3 py-2 font-medium">详情</th>
                  <th className="text-left px-3 py-2 font-medium">优先级</th>
                  <th className="text-left px-3 py-2 font-medium">更新时间</th>
                </tr>
              </thead>
              <tbody>
                {checklist.items.map((it) => {
                  const meta = STATUS_STYLE[it.status] ?? STATUS_STYLE.missing;
                  const Icon = meta.Icon;
                  return (
                    <tr key={it.key} className="border-t hover:bg-muted/20">
                      <td className="px-3 py-2 font-medium">{it.label}</td>
                      <td className="px-3 py-2 text-center">
                        <Icon className={`h-3.5 w-3.5 inline ${meta.color}`} />
                      </td>
                      <td className="px-3 py-2 text-muted-foreground">{it.detail || '—'}</td>
                      <td className="px-3 py-2">
                        <span className={`text-[10px] px-1.5 py-0.5 rounded ${
                          it.severity === 'blocking' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' :
                          it.severity === 'important' ? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400' :
                          'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400'
                        }`}>
                          {it.severity || 'nice_to_have'}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-muted-foreground whitespace-nowrap">{it.last_updated || '—'}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Data reports */}
      {reports.length > 0 && reports.map((r) => (
        <ReportWithToc key={r.key} report={r} onOpenFile={onOpenFile} />
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared: SummaryCard, SegmentContributionCard, ReportWithToc, etc.
// ---------------------------------------------------------------------------

function SummaryCard({ facets }: { facets: Facets }) {
  const fv = asScenario(facets.fair_value);
  const cur = facets.current_price;
  const upside = pickBase(facets.upside_pct);

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="text-sm font-medium mb-3">估值结论</div>
      <div className="grid grid-cols-3 gap-2 sm:gap-4">
        <Stat label="Bear" value={fmt(fv.bear)} unit={facets.currency_unit} tone="bear" />
        <Stat label="Base" value={fmt(fv.base)} unit={facets.currency_unit} tone="base" highlight />
        <Stat label="Bull" value={fmt(fv.bull)} unit={facets.currency_unit} tone="bull" />
      </div>
      <div className="mt-4 grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
        <KV label="当前价" value={fmt(cur)} />
        <KV
          label="基准空间"
          value={fmtPct(upside)}
          valueClass={
            upside !== undefined && upside > 0 ? 'text-emerald-600' :
            upside !== undefined && upside < 0 ? 'text-red-500' : ''
          }
        />
        {facets.n_segments !== undefined && (
          <KV label="分部数" value={`${facets.n_segments}`} />
        )}
        {facets.structure_type && (
          <KV label="估值方法" value={facets.structure_type === 'multi_segment' ? 'SOTP' : '整体估值'} />
        )}
      </div>

      {facets.key_drivers && facets.key_drivers.length > 0 && (
        <div className="mt-3 text-xs">
          <span className="text-muted-foreground">核心驱动：</span>
          {facets.key_drivers.join(' · ')}
        </div>
      )}
      {facets.key_risks && facets.key_risks.length > 0 && (
        <div className="text-xs mt-1">
          <span className="text-muted-foreground">核心风险：</span>
          {facets.key_risks.join(' · ')}
        </div>
      )}
    </div>
  );
}

function SegmentContributionCard({
  segments, currencyUnit,
}: { segments: SegmentSummary[]; currencyUnit?: string }) {
  const total = segments.reduce((acc, s) => acc + (s.fair_value_share?.base || 0), 0);

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="text-sm font-medium mb-3">分部贡献（Base）</div>
      <div className="space-y-2">
        {segments.map((s) => {
          const baseV = s.fair_value_share?.base || 0;
          const pct = total ? (baseV / total) * 100 : 0;
          return (
            <div key={s.segment_id} className="flex items-center gap-3 text-xs">
              <span className="w-36 truncate font-medium">{s.name || s.segment_id}</span>
              <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
                <div
                  className="h-2 bg-primary/70 rounded-full"
                  style={{ width: `${Math.max(0, Math.min(100, pct)).toFixed(1)}%` }}
                />
              </div>
              <span className="w-24 text-right font-mono">{fmt(baseV)}</span>
              <span className="w-12 text-right text-muted-foreground">{pct.toFixed(0)}%</span>
            </div>
          );
        })}
      </div>
      {currencyUnit && (
        <div className="mt-2 text-[10px] text-muted-foreground text-right">{currencyUnit}</div>
      )}
    </div>
  );
}

function ReportWithToc({
  report, onOpenFile,
}: { report: ReportEntry; onOpenFile?: (f: string) => void }) {
  const toc = useMemo(() => extractToc(report.markdown ?? ''), [report.markdown]);
  const contentRef = useRef<HTMLDivElement>(null);
  const [tocCollapsed, setTocCollapsed] = useState(false);

  const scrollToHeading = useCallback((id: string) => {
    if (!contentRef.current) return;
    let target: Element | null = contentRef.current.querySelector(`[id="${id}"], [data-heading-id="${id}"]`);
    if (!target) {
      // fallback：按 text 匹配
      const headings = contentRef.current.querySelectorAll('h2, h3, h4');
      for (const h of headings) {
        const hId = (h.textContent ?? '')
          .toLowerCase()
          .replace(/[^\w\u4e00-\u9fff]+/g, '-')
          .replace(/^-|-$/g, '');
        if (hId === id) { target = h; break; }
      }
    }
    if (target) {
      // ⚠️ 只滚动 contentRef 容器内部，不触发整页滚动
      const container = contentRef.current;
      const targetTop = (target as HTMLElement).offsetTop;
      container.scrollTo({ top: targetTop - 12, behavior: 'smooth' });
    }
  }, []);

  return (
    <div className="rounded-lg border bg-card overflow-hidden">
      <div className="px-4 py-2.5 border-b flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm font-medium">
          <FileText className="h-4 w-4" />
          {report.title}
        </div>
        <div className="flex items-center gap-3 text-[11px] text-muted-foreground">
          {report.size_chars !== undefined && <span>{report.size_chars.toLocaleString()} 字符</span>}
          {report.updated_at && <span>{report.updated_at}</span>}
          {toc.length > 0 && (
            <button
              onClick={() => setTocCollapsed((v) => !v)}
              className="flex items-center gap-1 hover:text-foreground"
              title={tocCollapsed ? '展开目录' : '收起目录'}
            >
              <List className="h-3.5 w-3.5" />
              {tocCollapsed ? '展开目录' : '收起目录'}
            </button>
          )}
          {onOpenFile && (
            <button
              onClick={() => onOpenFile(report.path)}
              className="hover:text-foreground"
              title="在文件浏览器打开"
            >
              📁
            </button>
          )}
        </div>
      </div>

      <div className="flex">
        {toc.length > 0 && !tocCollapsed && (
          <div className="w-52 shrink-0 border-r border-border/40 bg-transparent overflow-y-auto max-h-[70vh] sticky top-0">
            <nav className="py-3 px-2 space-y-0.5">
              {toc.map((item, i) => (
                <button
                  key={`${item.id}-${i}`}
                  onClick={() => scrollToHeading(item.id)}
                  className={`block w-full text-left text-[11px] leading-tight py-1 px-2 rounded hover:bg-muted/30 transition-colors truncate ${
                    item.level === 2 ? 'font-medium text-foreground' :
                    item.level === 3 ? 'pl-4 text-muted-foreground' :
                    'pl-6 text-muted-foreground/70'
                  }`}
                  title={item.text}
                >
                  {item.text}
                </button>
              ))}
            </nav>
          </div>
        )}

        <div className="flex-1 min-w-0 overflow-y-auto max-h-[70vh] px-5 py-4" ref={contentRef}>
          <article className="prose prose-sm dark:prose-invert max-w-none">
            <Markdown content={report.markdown ?? ''} variant="panel" onOpenFile={onOpenFile} />
          </article>
        </div>
      </div>
    </div>
  );
}

function ChecklistCard({ checklist }: { checklist: Checklist }) {
  const s = checklist.summary ?? {};
  const overall = s.overall ?? 'partial';
  const overallText = {
    ok: '✅ 数据齐备',
    partial: '⚠️ 部分缺失',
    blocked: '❌ 阻塞性缺失',
  }[overall] ?? '—';
  const overallClass = {
    ok: 'text-emerald-600',
    partial: 'text-amber-600',
    blocked: 'text-red-500',
  }[overall] ?? '';

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className={`text-sm font-medium ${overallClass}`}>{overallText}</span>
          <span className="text-xs text-muted-foreground">
            {s.ok ?? 0} 通过 / {s.partial ?? 0} 部分 / {s.missing ?? 0} 缺失
            {checklist.required_periods ? ` · 要求 ${checklist.required_periods} 期` : ''}
          </span>
        </div>
        {checklist.generated_at && (
          <span className="text-[10px] text-muted-foreground">{checklist.generated_at}</span>
        )}
      </div>
      {(s.blocking_missing ?? []).length > 0 && (
        <div className="mt-2 text-xs text-red-600">
          阻塞项：{(s.blocking_missing ?? []).join(', ')}
        </div>
      )}
    </div>
  );
}

function RawPayloadCard({ payload }: { payload: EviPayload }) {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-lg border bg-card">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-2.5 text-sm font-medium"
      >
        <span>Raw Payload (debug)</span>
        <span className="text-xs text-muted-foreground">{open ? '收起' : '展开'}</span>
      </button>
      {open && (
        <pre className="px-4 pb-3 text-[11px] leading-relaxed overflow-auto max-h-96 bg-muted/30">
          {JSON.stringify(payload, null, 2)}
        </pre>
      )}
    </div>
  );
}

function Stat({
  label, value, unit, tone, highlight,
}: { label: string; value: string; unit?: string; tone: 'bear' | 'base' | 'bull'; highlight?: boolean }) {
  const toneClass =
    tone === 'bear' ? 'border-red-500/20 bg-red-500/5' :
    tone === 'bull' ? 'border-emerald-500/20 bg-emerald-500/5' :
    'border-primary/20 bg-primary/5';
  return (
    <div className={`rounded-md border p-3 ${toneClass} ${highlight ? 'ring-1 ring-primary/30' : ''}`}>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="text-xl font-semibold mt-0.5">{value}</div>
      {unit && <div className="text-[10px] text-muted-foreground mt-0.5 truncate">{unit}</div>}
    </div>
  );
}

function KV({
  label, value, valueClass = '',
}: { label: string; value: string; valueClass?: string }) {
  return (
    <div className="flex flex-col">
      <span className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className={`font-medium ${valueClass}`}>{value}</span>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Error boundary
// ---------------------------------------------------------------------------

interface EBProps { entry: TemplateEntry; children: React.ReactNode; }
interface EBState { error: Error | null; }

class EviReportErrorBoundary extends React.Component<EBProps, EBState> {
  constructor(props: EBProps) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error: Error): EBState {
    return { error };
  }
  componentDidCatch(error: Error, info: React.ErrorInfo) {
    console.error('[EviReportPanel] render error:', error, info);
  }
  render() {
    if (this.state.error) {
      const { entry } = this.props;
      return (
        <div className="px-4 sm:px-6 lg:px-8 py-6 max-w-5xl mx-auto">
          <div className="rounded-lg border border-red-300/50 bg-red-500/5 p-4 text-sm space-y-3">
            <div className="font-medium text-red-600 dark:text-red-400">报告面板渲染失败</div>
            <pre className="text-[11px] leading-relaxed bg-background/60 border rounded p-2 overflow-auto max-h-48">
              {String(this.state.error?.message ?? this.state.error)}
            </pre>
            <details className="text-xs">
              <summary className="cursor-pointer text-muted-foreground hover:text-foreground">
                展开原始 payload
              </summary>
              <pre className="mt-2 text-[11px] leading-relaxed bg-background/60 border rounded p-2 overflow-auto max-h-96">
                {JSON.stringify(entry.payload, null, 2)}
              </pre>
            </details>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

export default EviReportPanel;
