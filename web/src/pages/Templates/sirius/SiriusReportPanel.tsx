/**
 * SiriusReportPanel — Structured visualization of a Sirius valuation entry.
 *
 * Sections:
 *  1. Valuation Summary — key numbers, verdict badge, method details, WACC
 *  2. D1-D7 tab panel — per-dimension: score badge + metrics table + key findings + analysis
 *  3. Financial Context — raw FMP markdown (collapsible)
 */
import { useState } from 'react';
import {
  ChevronDown, ChevronRight, TrendingUp, TrendingDown,
  Minus, AlertTriangle, CheckCircle2, Info,
} from 'lucide-react';
import type { TemplateEntry } from '@/types/template';

// ---------------------------------------------------------------------------
// Data types
// ---------------------------------------------------------------------------

interface ReferenceItem {
  // v2.0 format
  id?: string;
  type?: string;
  title?: string;
  source?: string;
  detail?: string;
  logic?: string;
  cited_by?: string[];
  section?: string;
  // Legacy format (agent may output this)
  ref?: string;
  text?: string;
}

interface DimensionData {
  score?: number;
  title?: string;
  summary?: string;
  analysis?: string;
  metrics?: Record<string, unknown>;
  risks?: string[];
  key_findings?: string[];
  confidence?: number;
  dimension?: string;
  references?: ReferenceItem[];
}

interface EngineResult {
  classification?: {
    type?: string;
    methods?: string[];
    weights?: Record<string, number>;
    roe_avg?: number;
    rev_cagr_pct?: number;
    np_cagr_pct?: number;
  };
  wacc?: {
    rf?: number; beta?: number; erp?: number;
    ke?: number; kd_pre?: number; wacc?: number;
    e_weight?: number; d_weight?: number; tax_rate?: number;
  };
  methods?: Array<{ method: string; intrinsic?: number; [k: string]: unknown }>;
  crossValidation?: {
    weighted_avg?: number; current_price?: number;
    safety_margin?: number; judgment?: string;
    cv?: number; consistency?: string;
  };
}

interface SiriusPayload {
  engine_result?: EngineResult;
  dimensions?: Record<string, DimensionData | null>;
  financial_context_md?: string;
}

interface Props {
  entry: TemplateEntry;
  onOpenFile?: (filePath: string) => void;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const JUDGMENT_MAP: Record<string, { color: string; bg: string; Icon: React.ComponentType<{ className?: string }> }> = {
  低估: { color: 'text-emerald-600 dark:text-emerald-400', bg: 'bg-emerald-500/10', Icon: TrendingUp },
  合理: { color: 'text-amber-600 dark:text-amber-400', bg: 'bg-amber-500/10', Icon: Minus },
  高估: { color: 'text-red-500 dark:text-red-400', bg: 'bg-red-500/10', Icon: TrendingDown },
};

const SCORE_COLOR = (s: number) =>
  s >= 8 ? 'text-emerald-600 dark:text-emerald-400'
  : s >= 6 ? 'text-amber-600 dark:text-amber-400'
  : 'text-red-500 dark:text-red-400';

const SCORE_BG = (s: number) =>
  s >= 8 ? 'bg-emerald-500/10'
  : s >= 6 ? 'bg-amber-500/10'
  : 'bg-red-500/10';

function fmt(n: number | null | undefined): string {
  if (n == null) return '—';
  if (Math.abs(n) >= 100) return n.toFixed(0);
  if (Math.abs(n) >= 1) return n.toFixed(2);
  return n.toFixed(4);
}

const DIM_ORDER = ['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7'];
const DIM_SHORT: Record<string, string> = {
  D1: '商业模式', D2: '护城河', D3: '外部环境',
  D4: '管理层', D5: 'MD&A', D6: '综合评估', D7: '定性调整',
};

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

export function SiriusReportPanel({ entry, onOpenFile }: Props) {
  const payload = entry.payload as SiriusPayload;
  const er = payload?.engine_result;
  const dims = payload?.dimensions ?? {};
  const cv = er?.crossValidation;

  // Build the data directory prefix for file paths (e.g. "data/auto_1002d0af1b/")
  const dataPrefix = entry.entry_key ? `data/${entry.entry_key.replace(/[^A-Za-z0-9_]/g, '_')}/` : 'data/';

  const [collapsed, setCollapsed] = useState(false);
  const [activeTab, setActiveTab] = useState<string>(
    DIM_ORDER.find((d) => dims[d]) ?? 'D1',
  );

  const hasDimensions = DIM_ORDER.some((d) => dims[d]);
  const judgment = cv?.judgment ?? '';
  const jStyle = JUDGMENT_MAP[judgment];
  const JIcon = jStyle?.Icon ?? Minus;

  if (!er && !hasDimensions) return null;

  return (
    <div className="w-full flex flex-col gap-0">
      {/* ── PANEL HEADER ── */}
      <button
        className="flex items-center justify-between px-5 py-3 border-b hover:bg-foreground/[0.03] transition-colors"
        style={{ borderColor: 'var(--color-border-muted)' }}
        onClick={() => setCollapsed(!collapsed)}
      >
        <div className="flex items-center gap-2.5">
          <TrendingUp className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
          <span className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
            Sirius 估值报告
          </span>
          {judgment && jStyle && (
            <span className={`inline-flex items-center gap-1 text-xs font-semibold px-2.5 py-0.5 rounded-full ${jStyle.color} ${jStyle.bg}`}>
              <JIcon className="h-3 w-3" />
              {judgment}
              {cv?.safety_margin != null && (
                <span>({cv.safety_margin > 0 ? '+' : ''}{cv.safety_margin.toFixed(1)}%)</span>
              )}
            </span>
          )}
        </div>
        {collapsed
          ? <ChevronRight className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
          : <ChevronDown className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />}
      </button>

      {!collapsed && (
        <div className="flex flex-col gap-5 px-5 py-4">

          {/* ── 1. VALUATION SUMMARY PANEL ── */}
          {er && <ValuationSummaryPanel er={er} />}

          {/* ── 2. D1-D7 TAB PANEL ── */}
          {hasDimensions && (
            <div>
              <SectionLabel>维度分析</SectionLabel>
              {/* Tabs */}
              <div className="flex gap-1 flex-wrap border-b mb-4" style={{ borderColor: 'var(--color-border-muted)' }}>
                {DIM_ORDER.filter((d) => dims[d]).map((d) => {
                  const dim = dims[d]!;
                  const score = dim.score;
                  const isActive = activeTab === d;
                  return (
                    <button
                      key={d}
                      onClick={() => setActiveTab(d)}
                      className={`flex items-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors border-b-2 -mb-px ${
                        isActive
                          ? 'border-[var(--color-accent-primary)]'
                          : 'border-transparent hover:border-foreground/20'
                      }`}
                      style={{ color: isActive ? 'var(--color-accent-primary)' : 'var(--color-text-secondary)' }}
                    >
                      {d}
                      <span className="opacity-60">{DIM_SHORT[d]}</span>
                      {score != null && (
                        <span className={`inline-flex items-center justify-center w-4 h-4 rounded-full text-[10px] font-bold ${SCORE_BG(score)} ${SCORE_COLOR(score)}`}>
                          {score}
                        </span>
                      )}
                    </button>
                  );
                })}
              </div>

              {/* Active tab content */}
              {dims[activeTab] && (
                <DimensionPane dim={dims[activeTab]!} dimKey={activeTab} onOpenFile={onOpenFile} dataPrefix={dataPrefix} />
              )}
            </div>
          )}

          {/* ── 3. FINANCIAL CONTEXT ── */}
          {payload?.financial_context_md && (
            <FinancialContextSection md={payload.financial_context_md} />
          )}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Valuation Summary Panel
// ---------------------------------------------------------------------------

function ValuationSummaryPanel({ er }: { er: EngineResult }) {
  const cv = er.crossValidation;
  const cls = er.classification;

  const judgment = cv?.judgment ?? '';
  const jStyle = JUDGMENT_MAP[judgment];

  return (
    <div>
      <SectionLabel>估值摘要</SectionLabel>
      <div className="flex flex-col gap-3">

        {/* Key metrics row */}
        {cv && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            <MetricCard
              label="公允价值"
              value={fmt(cv.weighted_avg)}
              accent
            />
            <MetricCard label="当前价格" value={fmt(cv.current_price)} />
            <MetricCard
              label="安全边际"
              value={cv.safety_margin != null
                ? `${cv.safety_margin > 0 ? '+' : ''}${cv.safety_margin.toFixed(1)}%`
                : '—'}
              valueClass={cv.safety_margin != null
                ? cv.safety_margin > 0 ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-500'
                : ''}
            />
            <MetricCard
              label="估值判断"
              value={judgment || '—'}
              valueClass={jStyle?.color ?? ''}
            />
          </div>
        )}

        {/* Classification chips */}
        {cls && (
          <div className="flex flex-wrap items-center gap-2 text-xs">
            {cls.type && <Chip label="类型" value={cls.type} />}
            {cls.rev_cagr_pct != null && <Chip label="收入 CAGR" value={`${cls.rev_cagr_pct.toFixed(1)}%`} />}
            {cls.np_cagr_pct != null && <Chip label="净利 CAGR" value={`${cls.np_cagr_pct.toFixed(1)}%`} />}
            {cls.roe_avg != null && <Chip label="ROE 均值" value={`${cls.roe_avg.toFixed(1)}%`} />}
            {cv?.consistency && <Chip label="方法一致性" value={cv.consistency} />}
            {cv?.cv != null && <Chip label="CV 离散度" value={`${cv.cv}%`} />}
          </div>
        )}

        {/* Valuation methods table */}
        {er.methods && er.methods.length > 0 && (
          <ValuationMethodsTable methods={er.methods} weights={cls?.weights} />
        )}

        {/* WACC */}
        {er.wacc && <WaccPanel wacc={er.wacc} />}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Valuation Methods Table
// ---------------------------------------------------------------------------

function ValuationMethodsTable({
  methods, weights,
}: {
  methods: NonNullable<EngineResult['methods']>;
  weights?: Record<string, number>;
}) {
  return (
    <div>
      <p className="text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-tertiary)' }}>估值方法明细</p>
      <div className="rounded-lg overflow-hidden border" style={{ borderColor: 'var(--color-border-muted)' }}>
        <table className="w-full text-xs">
          <thead>
            <tr style={{ backgroundColor: 'var(--color-bg-subtle)', color: 'var(--color-text-tertiary)' }}>
              <th className="px-3 py-2 text-left font-medium">方法</th>
              <th className="px-3 py-2 text-right font-medium">内在价值</th>
              <th className="px-3 py-2 text-right font-medium">权重</th>
              <th className="px-3 py-2 text-right font-medium">补充</th>
            </tr>
          </thead>
          <tbody>
            {methods.map((m, i) => (
              <tr
                key={m.method}
                className="border-t"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  backgroundColor: i % 2 === 0 ? 'transparent' : 'var(--color-bg-subtle)',
                }}
              >
                <td className="px-3 py-2 font-medium" style={{ color: 'var(--color-text-primary)' }}>{m.method}</td>
                <td className="px-3 py-2 text-right font-mono" style={{ color: 'var(--color-text-primary)' }}>
                  {fmt(m.intrinsic as number)}
                </td>
                <td className="px-3 py-2 text-right" style={{ color: 'var(--color-text-secondary)' }}>
                  {weights?.[m.method] ?? '—'}%
                </td>
                <td className="px-3 py-2 text-right" style={{ color: 'var(--color-text-tertiary)' }}>
                  <MethodDetail m={m} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function MethodDetail({ m }: { m: Record<string, unknown> }) {
  if (m.method === 'PEG') {
    return <>{`PE=${String(m.pe ?? '—').slice(0, 5)} g=${Number(m.g_pct ?? 0).toFixed(0)}%`}</>;
  }
  if (m.method === 'PS') {
    return <>{`区间 ${fmt(m.low as number)}–${fmt(m.high as number)}`}</>;
  }
  if (m.method === 'DCF_Scenarios') {
    const w = m.scenario_weights as number[] | undefined;
    return (
      <>{`乐${w?.[0]}%/${fmt(m.v_optimistic as number)} 基${w?.[1]}%/${fmt(m.v_base as number)} 悲${w?.[2]}%/${fmt(m.v_pessimistic as number)}`}</>
    );
  }
  return null;
}

// ---------------------------------------------------------------------------
// WACC Panel
// ---------------------------------------------------------------------------

function WaccPanel({ wacc }: { wacc: NonNullable<EngineResult['wacc']> }) {
  const [open, setOpen] = useState(false);
  return (
    <div>
      <button
        className="flex items-center gap-1 text-xs mb-1.5 hover:opacity-80 transition-opacity"
        style={{ color: 'var(--color-text-tertiary)' }}
        onClick={() => setOpen(!open)}
      >
        {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        <span className="font-medium">WACC = {wacc.wacc?.toFixed(2)}%</span>
      </button>
      {open && (
        <div
          className="rounded-lg border p-3 grid grid-cols-3 sm:grid-cols-5 gap-3"
          style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-subtle)' }}
        >
          {[
            ['Rf', `${wacc.rf}%`], ['Beta', wacc.beta?.toFixed(2)],
            ['ERP', `${wacc.erp}%`], ['Ke', `${wacc.ke?.toFixed(2)}%`],
            ['Kd', `${wacc.kd_pre?.toFixed(2)}%`], ['股权占比', `${wacc.e_weight?.toFixed(1)}%`],
            ['负债占比', `${wacc.d_weight?.toFixed(1)}%`], ['税率', `${wacc.tax_rate?.toFixed(1)}%`],
          ].map(([label, val]) => (
            <div key={label}>
              <div className="text-[10px]" style={{ color: 'var(--color-text-tertiary)' }}>{label}</div>
              <div className="text-xs font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>{val ?? '—'}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Dimension Pane (tab content)
// ---------------------------------------------------------------------------

function DimensionPane({ dim, dimKey, onOpenFile, dataPrefix }: { dim: DimensionData; dimKey: string; onOpenFile?: (path: string) => void; dataPrefix?: string }) {
  const [showAnalysis, setShowAnalysis] = useState(false);

  return (
    <div className="flex flex-col gap-3">
      {/* Title + score + confidence */}
      <div className="flex items-center gap-2.5 flex-wrap">
        <span className="text-sm font-semibold" style={{ color: 'var(--color-text-primary)' }}>
          {dim.title ?? `${dimKey} ${DIM_SHORT[dimKey]}`}
        </span>
        {dim.score != null && (
          <span className={`inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-bold ${SCORE_BG(dim.score)} ${SCORE_COLOR(dim.score)}`}>
            {dim.score}/10
          </span>
        )}
        {dim.confidence != null && (
          <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
            置信度 {(dim.confidence * 100).toFixed(0)}%
          </span>
        )}
      </div>

      {/* Summary */}
      {dim.summary && (
        <p className="text-sm leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
          {dim.summary}
        </p>
      )}

      {/* Metrics table (if any) */}
      {dim.metrics && Object.keys(dim.metrics).length > 0 && (
        <MetricsTable metrics={dim.metrics} />
      )}

      {/* Key findings */}
      {dim.key_findings && dim.key_findings.length > 0 && (
        <div>
          <p className="text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
            <CheckCircle2 className="h-3 w-3 inline mr-1" />
            核心发现
          </p>
          <ul className="flex flex-col gap-1">
            {dim.key_findings.map((f, i) => (
              <li key={i} className="flex items-start gap-2 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                <span className="mt-0.5 flex-shrink-0 h-3.5 w-3.5 rounded-full bg-emerald-500/15 text-emerald-600 dark:text-emerald-400 flex items-center justify-center text-[10px] font-bold">
                  {i + 1}
                </span>
                {f}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Risks */}
      {dim.risks && dim.risks.length > 0 && (
        <div>
          <p className="text-xs font-medium mb-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
            <AlertTriangle className="h-3 w-3 inline mr-1 text-amber-500" />
            风险提示
          </p>
          <ul className="flex flex-col gap-1">
            {dim.risks.map((r, i) => (
              <li key={i} className="flex items-start gap-2 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                <span className="mt-0.5 flex-shrink-0 h-3.5 w-3.5 rounded-full bg-amber-500/15 text-amber-600 dark:text-amber-400 flex items-center justify-center text-[10px] font-bold">!</span>
                {r}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Analysis (long text — collapsible) */}
      {dim.analysis && (
        <div>
          <button
            className="flex items-center gap-1 text-xs hover:opacity-80 transition-opacity mb-1.5"
            style={{ color: 'var(--color-text-tertiary)' }}
            onClick={() => setShowAnalysis(!showAnalysis)}
          >
            {showAnalysis ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
            <Info className="h-3 w-3" />
            <span>详细分析</span>
          </button>
          {showAnalysis && (
            <p
              className="text-xs leading-relaxed whitespace-pre-wrap pl-1"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {dim.analysis}
            </p>
          )}
        </div>
      )}

      {/* References (citations) */}
      <ReferencesSection
        references={dim.references ?? []}
        dimKey={dimKey}
        onOpenFile={onOpenFile}
        dataPrefix={dataPrefix}
      />
    </div>
  );
}

// ---------------------------------------------------------------------------
// Metrics Table — two-column, recursively handles nested objects/arrays
// ---------------------------------------------------------------------------

const METRICS_LABELS: Record<string, string> = {
  business_model_clarity: '商业模式清晰度', capital_intensity: '资本消耗', collection_mode: '收款模式',
  cash_impact: '现金影响', market_structure: '行业格局', moat_existence: '护城河存在性',
  moat_evidence_strength: '证据强度', moat_type: '护城河类型', moat_flywheel: '护城河飞轮',
  moat_rating: '护城河评级', moat_sustainability: '可持续性', pricing_power: '定价权',
  supply_side_rating: '供给侧', demand_side_rating: '需求侧', scale_economy_rating: '规模经济',
  human_capital_dep: '人力资本依赖', entry_barrier: '进入壁垒',
  cyclicality: '周期性', cycle_position: '周期位置', regulatory_risk: '监管风险',
  disruption_risk: '颠覆风险', macro_sensitivity: '宏观敏感度', industry_trend: '行业趋势',
  management_rating: '管理层评级', capital_allocation_record: '资本配置', related_party_risk: '关联交易风险',
  mda_credibility: '可信度', mda_impact: '对投资判断影响', mda_forward_guidance: '前瞻指引',
  distribution_signal: '分红意向', holding_structure: '控股结构分析',
  sotp_value_mm: 'SOTP 估值(百万)', sotp_discount_pct: '控股折价',
  roe_5y_avg: 'ROE 5年均值', roe_5y_std: 'ROE 标准差', market_cr4: 'CR4 市占率',
  core_revenue_growth_pct: '核心收入增速', core_op_profit_growth_pct: '核心经营利润增速',
  reported_revenue_growth_pct: '报表收入增速', reported_profit_growth_pct: '报表利润增速',
  non_recurring_income_ratio_pct: '非经常性损益占比', ocf_to_net_income_ratio: 'OCF/净利润',
  capex_to_depreciation_ratio: 'Capex/折旧', interest_coverage_ratio: '利息覆盖率',
  policy_dependency_pct: '政策依赖度', profit_peak_to_trough_pct: '利润峰谷差',
  revenue_peak_to_trough_pct: '收入峰谷差', industry_growth_driver: '增长驱动',
  competitor_ranking: '竞争排名', advantage_gap_sustainability: '优势差距可持续',
  moat_framework_primary: '主分析框架', moat_monitor_kpis: '护城河 KPI 监控',
  competitors: '竞争对手', false_advantages: '排除的虚假优势', industry_keywords: '行业关键词',
};

/** Classify a value for rendering */
function classifyValue(v: unknown): 'primitive' | 'bool' | 'string_list' | 'object_list' | 'plain_object' {
  if (typeof v === 'boolean') return 'bool';
  if (!Array.isArray(v) && typeof v !== 'object') return 'primitive';
  if (Array.isArray(v)) {
    if (v.length === 0) return 'string_list';
    return typeof v[0] === 'object' && v[0] !== null ? 'object_list' : 'string_list';
  }
  return 'plain_object';
}

function MetricsTable({ metrics }: { metrics: Record<string, unknown> }) {
  const entries = Object.entries(metrics).filter(([, v]) => {
    if (v == null) return false;
    if (Array.isArray(v) && v.length === 0) return false;
    return true;
  });
  if (!entries.length) return null;

  // Primitives/booleans → two-column table; everything else → full-width block below
  const primitives = entries.filter(([, v]) => {
    const t = classifyValue(v);
    return t === 'primitive' || t === 'bool';
  });
  const complex = entries.filter(([, v]) => {
    const t = classifyValue(v);
    return t !== 'primitive' && t !== 'bool';
  });

  // Split primitives into two columns
  const half = Math.ceil(primitives.length / 2);
  const col1 = primitives.slice(0, half);
  const col2 = primitives.slice(half);

  return (
    <div className="flex flex-col gap-3">
      {/* Two-column table for primitives */}
      {primitives.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
          {[col1, col2].map((col, ci) => (
            col.length > 0 && (
              <div key={ci} className="rounded-lg overflow-hidden border" style={{ borderColor: 'var(--color-border-muted)' }}>
                <table className="w-full text-xs">
                  <tbody>
                    {col.map(([k, v], i) => (
                      <tr
                        key={k}
                        className="border-t first:border-t-0"
                        style={{
                          borderColor: 'var(--color-border-muted)',
                          backgroundColor: i % 2 === 0 ? 'transparent' : 'var(--color-bg-subtle)',
                        }}
                      >
                        <td className="px-2.5 py-1.5 w-2/5 font-medium whitespace-nowrap" style={{ color: 'var(--color-text-tertiary)' }}>
                          {METRICS_LABELS[k] ?? k}
                        </td>
                        <td className="px-2.5 py-1.5" style={{ color: 'var(--color-text-primary)' }}>
                          <MetricValue k={k} v={v} />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          ))}
        </div>
      )}

      {/* Complex values: object lists, string lists, plain objects */}
      {complex.map(([k, v]) => (
        <ComplexField key={k} fieldKey={k} value={v} />
      ))}
    </div>
  );
}

/** Renders a single complex field (array or object) with appropriate UI */
function ComplexField({ fieldKey, value }: { fieldKey: string; value: unknown }) {
  const label = METRICS_LABELS[fieldKey] ?? fieldKey;
  const type = classifyValue(value);

  return (
    <div>
      <p className="text-[10px] font-semibold uppercase tracking-wide mb-1.5" style={{ color: 'var(--color-text-tertiary)' }}>
        {label}
      </p>
      {type === 'string_list' && (
        <div className="flex flex-wrap gap-1.5">
          {(value as unknown[]).map((item, i) => (
            <span
              key={i}
              className="px-2 py-0.5 text-xs rounded-full"
              style={{
                backgroundColor: 'var(--color-bg-subtle)',
                border: '1px solid var(--color-border-muted)',
                color: 'var(--color-text-secondary)',
              }}
            >
              {String(item)}
            </span>
          ))}
        </div>
      )}
      {type === 'object_list' && (
        <ObjectListRenderer fieldKey={fieldKey} items={value as Record<string, unknown>[]} />
      )}
      {type === 'plain_object' && (
        <MetricsTable metrics={value as Record<string, unknown>} />
      )}
    </div>
  );
}

/** Render list of objects intelligently based on the field key */
function ObjectListRenderer({
  fieldKey, items,
}: { fieldKey: string; items: Record<string, unknown>[] }) {
  if (!items.length) return null;

  // --- competitors: [{name, ticker}] → badge pills ---
  if (fieldKey === 'competitors') {
    return (
      <div className="flex flex-wrap gap-1.5">
        {items.map((item, i) => (
          <span
            key={i}
            className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-lg"
            style={{
              backgroundColor: 'var(--color-bg-subtle)',
              border: '1px solid var(--color-border-muted)',
              color: 'var(--color-text-primary)',
            }}
          >
            <span className="font-medium">{item.name as string ?? '?'}</span>
            {item.ticker && (
              <span className="font-mono opacity-60 text-[10px]">{item.ticker as string}</span>
            )}
          </span>
        ))}
      </div>
    );
  }

  // --- moat_monitor_kpis: [{kpi, current, threshold}] → compact table ---
  if (fieldKey === 'moat_monitor_kpis') {
    return (
      <div className="rounded-lg overflow-hidden border" style={{ borderColor: 'var(--color-border-muted)' }}>
        <table className="w-full text-xs">
          <thead style={{ backgroundColor: 'var(--color-bg-subtle)' }}>
            <tr>
              <th className="px-2.5 py-1.5 text-left font-medium" style={{ color: 'var(--color-text-tertiary)' }}>KPI</th>
              <th className="px-2.5 py-1.5 text-left font-medium" style={{ color: 'var(--color-text-tertiary)' }}>当前值</th>
              <th className="px-2.5 py-1.5 text-left font-medium text-amber-600" style={{}}>预警阈值</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item, i) => (
              <tr
                key={i}
                className="border-t"
                style={{ borderColor: 'var(--color-border-muted)' }}
              >
                <td className="px-2.5 py-1.5 font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {item.kpi as string ?? '—'}
                </td>
                <td className="px-2.5 py-1.5 font-mono" style={{ color: 'var(--color-text-primary)' }}>
                  {item.current as string ?? '—'}
                </td>
                <td className="px-2.5 py-1.5 font-mono text-amber-600 dark:text-amber-400">
                  {item.threshold as string ?? '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }

  // --- generic object array: recurse per item ---
  return (
    <div className="flex flex-col gap-2">
      {items.map((item, i) => (
        <div
          key={i}
          className="rounded-lg p-2.5 text-xs"
          style={{
            backgroundColor: 'var(--color-bg-subtle)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          {Object.entries(item).map(([k, v]) => (
            <div key={k} className="flex gap-1.5">
              <span className="min-w-16 font-medium" style={{ color: 'var(--color-text-tertiary)' }}>
                {METRICS_LABELS[k] ?? k}
              </span>
              <span style={{ color: 'var(--color-text-primary)' }}>{String(v ?? '—')}</span>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}

function MetricValue({ k, v }: { k: string; v: unknown }) {
  const s = String(v);

  if (typeof v === 'boolean') {
    return v
      ? <span className="text-emerald-600 dark:text-emerald-400 font-medium">是</span>
      : <span style={{ color: 'var(--color-text-tertiary)' }}>否</span>;
  }

  const HIGH_RISK_VALS = new Set(['高', '强周期', '损害价值', '高可持续']);
  const MID_RISK_VALS = new Set(['中', '中高', '弱周期', '中等可持续', '可能存在', '合格', '观察期', '弱', '中等证据']);
  const GOOD_VALS = new Set(['低', '非周期', '存在', '优秀', '强', '较强', '强证据']);

  const isRiskField = k.toLowerCase().includes('risk') || ['cyclicality', 'moat_rating',
    'management_rating', 'moat_existence', 'moat_evidence_strength', 'entry_barrier',
    'pricing_power', 'supply_side_rating', 'demand_side_rating', 'scale_economy_rating',
    'moat_sustainability', 'human_capital_dep', 'advantage_gap_sustainability'].includes(k);

  if (isRiskField) {
    if (HIGH_RISK_VALS.has(s)) return <span className="font-medium text-red-500">{s}</span>;
    if (GOOD_VALS.has(s)) return <span className="font-medium text-emerald-600 dark:text-emerald-400">{s}</span>;
    if (MID_RISK_VALS.has(s)) return <span className="font-medium text-amber-600 dark:text-amber-400">{s}</span>;
  }

  if ((k.endsWith('_pct') || k.endsWith('_ratio') || k.includes('cagr') || k.endsWith('_avg') || k.endsWith('_std'))
      && !isNaN(Number(v))) {
    const n = Number(v);
    const c = n >= 10 ? 'text-emerald-600 dark:text-emerald-400' : n < 0 ? 'text-red-500' : '';
    return <span className={c}>{s}{k.endsWith('_pct') || k.includes('cagr') || k.endsWith('_avg') ? '%' : ''}</span>;
  }

  return <>{s}</>;
}


// ---------------------------------------------------------------------------
// Financial Context (collapsible, show first N lines)
// ---------------------------------------------------------------------------

function FinancialContextSection({ md }: { md: string }) {
  const [open, setOpen] = useState(false);
  const preview = md.slice(0, 800);
  return (
    <div>
      <SectionLabel>财务数据（FMP）</SectionLabel>
      <button
        className="flex items-center gap-1.5 text-xs mb-2 hover:opacity-80 transition-opacity"
        style={{ color: 'var(--color-text-tertiary)' }}
        onClick={() => setOpen(!open)}
      >
        {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
        {open ? '收起' : '展开查看原始财务数据'}
      </button>
      {open && (
        <div
          className="rounded-lg border p-3 overflow-auto max-h-72"
          style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-subtle)' }}
        >
          <pre className="text-xs leading-relaxed font-sans whitespace-pre-wrap" style={{ color: 'var(--color-text-tertiary)' }}>
            {md.length > 6000 ? md.slice(0, 6000) + '\n\n…（仅显示前 6000 字符）' : md}
          </pre>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Atoms
// ---------------------------------------------------------------------------

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="text-[10px] font-semibold uppercase tracking-widest mb-2" style={{ color: 'var(--color-text-tertiary)' }}>
      {children}
    </p>
  );
}

function MetricCard({
  label, value, accent = false, valueClass = '',
}: { label: string; value: string; accent?: boolean; valueClass?: string }) {
  return (
    <div
      className="flex flex-col gap-1 rounded-lg px-3 py-2"
      style={{
        backgroundColor: 'var(--color-bg-subtle)',
        border: `1px solid ${accent ? 'var(--color-accent-overlay)' : 'var(--color-border-muted)'}`,
      }}
    >
      <div className="text-[10px] uppercase tracking-wide" style={{ color: 'var(--color-text-tertiary)' }}>{label}</div>
      <div
        className={`text-lg font-semibold leading-tight ${valueClass}`}
        style={!valueClass ? { color: accent ? 'var(--color-accent-primary)' : 'var(--color-text-primary)' } : undefined}
      >
        {value}
      </div>
    </div>
  );
}

function Chip({ label, value }: { label: string; value: string }) {
  return (
    <span
      className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px]"
      style={{
        backgroundColor: 'var(--color-bg-subtle)',
        border: '1px solid var(--color-border-muted)',
        color: 'var(--color-text-secondary)',
      }}
    >
      <span style={{ color: 'var(--color-text-tertiary)' }}>{label}</span>
      <span className="font-medium" style={{ color: 'var(--color-text-primary)' }}>{value}</span>
    </span>
  );
}

// ---------------------------------------------------------------------------
// References Section — citation cards with hover/click expand
// ---------------------------------------------------------------------------

const REF_TYPE_LABELS: Record<string, { label: string; color: string }> = {
  data: { label: '数据', color: 'text-blue-500' },
  calculation: { label: '计算', color: 'text-purple-500' },
  external: { label: '外部', color: 'text-green-600' },
  knowledge: { label: '知识库', color: 'text-amber-600' },
  user_memo: { label: '备忘', color: 'text-pink-500' },
};

function ReferencesSection({ references, dimKey, onOpenFile, dataPrefix = '' }: { references: ReferenceItem[]; dimKey: string; onOpenFile?: (path: string) => void; dataPrefix?: string }) {
  const [expandedId, setExpandedId] = useState<string | null>(null);

  // Build report file path from dimension key (D1 -> data/{symbol}/reports/d1_report.md)
  const dimNum = dimKey.replace('D', '');
  const reportPath = `${dataPrefix}reports/d${dimNum}_report.md`;

  // Report file is always the first reference
  const allRefs: Array<ReferenceItem | { id: string; type: 'report'; title: string; source: string; detail: string; cited_by: string[]; logic?: string; ref?: string; text?: string; section?: string }> = [
    {
      id: `report_${dimKey}`,
      type: 'report' as const,
      title: `${dimKey} 分析报告`,
      source: reportPath,
      detail: `完整的 ${dimKey} 维度分析报告，包含逻辑链推导和脚注引用`,
      cited_by: [],
    },
    ...references,
  ];

  const handleFileClick = (source: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (onOpenFile) {
      // If source is a relative path (not starting with / or http), prepend dataPrefix
      const fullPath = source.startsWith('/') || source.startsWith('http') || source.startsWith('data/')
        ? source
        : `${dataPrefix}${source}`;
      onOpenFile(fullPath);
    }
  };

  return (
    <div>
      <p className="text-xs font-medium mb-2" style={{ color: 'var(--color-text-tertiary)' }}>
        <span className="inline-block w-3.5 h-3.5 text-center leading-[14px] rounded-full bg-blue-500/15 text-blue-600 dark:text-blue-400 text-[10px] font-bold mr-1">
          ↗
        </span>
        溯源引用（{allRefs.length}）
      </p>
      <div className="flex flex-col gap-1.5">
        {allRefs.map((ref, idx) => {
          // Normalize: support both v2.0 format and legacy {ref, text, section} format
          const refId = ref.id || ref.ref || `ref_${idx}`;
          const refType = ref.type || 'data';
          const refTitle = ref.title || ref.text?.slice(0, 40) || refId;
          const refSource = ref.source || '';
          const refDetail = ref.detail || ref.text || '';
          const refCitedBy = ref.cited_by || [];
          const refSection = ref.section || '';

          const isReport = refType === 'report';
          const typeInfo = isReport
            ? { label: '报告', color: 'text-indigo-500' }
            : (REF_TYPE_LABELS[refType] ?? { label: refType || '引用', color: '' });
          const isExpanded = expandedId === refId;

          return (
            <div
              key={refId}
              className="rounded-lg border transition-all cursor-pointer"
              style={{
                borderColor: isExpanded ? 'var(--color-accent-overlay)' : 'var(--color-border-muted)',
                backgroundColor: isExpanded ? 'var(--color-bg-subtle)' : 'transparent',
              }}
              onClick={() => {
                // Report type: click directly opens the file
                if (isReport && onOpenFile) {
                  onOpenFile(refSource || reportPath);
                  return;
                }
                setExpandedId(isExpanded ? null : refId);
              }}
            >
              {/* Header row */}
              <div className="flex items-center gap-2 px-2.5 py-1.5">
                <span className={`text-[10px] font-semibold uppercase ${typeInfo.color}`}>
                  {typeInfo.label}
                </span>
                <span className="text-xs font-medium flex-1 truncate" style={{ color: 'var(--color-text-primary)' }}>
                  {refTitle}
                </span>
                {refSource && (
                  <span className="text-[10px] font-mono" style={{ color: 'var(--color-text-tertiary)' }}>
                    {refSource.length > 35 ? '...' + refSource.slice(-32) : refSource}
                  </span>
                )}
                {!isReport && (
                  isExpanded
                    ? <ChevronDown className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                    : <ChevronRight className="h-3 w-3 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                )}
              </div>

              {/* Expanded detail */}
              {isExpanded && (
                <div className="px-2.5 pb-2.5 flex flex-col gap-1.5 border-t" style={{ borderColor: 'var(--color-border-muted)' }}>
                  <p className="text-xs mt-1.5 leading-relaxed" style={{ color: 'var(--color-text-secondary)' }}>
                    {refDetail}
                  </p>
                  {ref.logic && (
                    <div
                      className="rounded-md p-2 text-xs font-mono whitespace-pre-wrap leading-relaxed"
                      style={{ backgroundColor: 'var(--color-bg-page)', color: 'var(--color-text-secondary)' }}
                    >
                      {ref.logic}
                    </div>
                  )}
                  {refCitedBy.length > 0 && (
                  <div className="flex flex-wrap gap-1 mt-1">
                    <span className="text-[10px]" style={{ color: 'var(--color-text-tertiary)' }}>引用于：</span>
                    {refCitedBy.map((c, i) => (
                      <span
                        key={i}
                        className="px-1.5 py-0.5 rounded text-[10px] font-mono"
                        style={{
                          backgroundColor: 'var(--color-bg-subtle)',
                          border: '1px solid var(--color-border-muted)',
                          color: 'var(--color-text-secondary)',
                        }}
                      >
                        {c}
                      </span>
                    ))}
                  </div>
                  )}
                  {/* Source as clickable link if it's a URL */}
                  {refSource.startsWith('http') && (
                    <a
                      href={refSource}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-[10px] underline mt-1"
                      style={{ color: 'var(--color-accent-primary)' }}
                      onClick={(e) => e.stopPropagation()}
                    >
                      打开原始来源 ↗
                    </a>
                  )}
                  {/* Source is a sandbox file path — clickable to open */}
                  {refSource && !refSource.startsWith('http') && refSource !== 'analysis' && (
                    <button
                      className="text-[10px] font-mono mt-1 flex items-center gap-1 hover:underline"
                      style={{ color: 'var(--color-accent-primary)' }}
                      onClick={(e) => handleFileClick(refSource, e)}
                    >
                      📄 {refSource} →
                    </button>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
