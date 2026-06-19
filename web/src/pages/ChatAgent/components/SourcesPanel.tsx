import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';
import {
  ExternalLink,
  FileText,
  StickyNote,
  Brain,
  LineChart,
  Wrench,
  FileSearch,
  ChevronRight,
  ChevronDown,
  Fingerprint,
  Lock,
} from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog';
import { provenanceDisplayKey, countDedupedSources, type ProvenanceRecord } from '@/types/chat';
import type { ProvenanceSourceType } from '@/types/sse';
import { AnimatedTabs } from '@/components/ui/animated-tabs';
import { workspaceRelativePath } from '@/pages/ChatAgent/utils/agentPaths';
import { Favicon } from './Favicon';
import './SourcesPanel.css';

/** Source types that carry a URL/domain and render a {@link Favicon}. */
const URL_SOURCE_TYPES = new Set<ProvenanceSourceType>(['web_search', 'web_fetch', 'sec_filing']);

/** Source types that resolve to an agent file path (routed via onOpenFile). */
const FILE_SOURCE_TYPES = new Set<ProvenanceSourceType>(['file_read', 'memo_read', 'memory_read']);

/** Stable display order of source-type groups. */
const GROUP_ORDER: ProvenanceSourceType[] = [
  'web_search',
  'web_fetch',
  'sec_filing',
  'market_data',
  'mcp_tool',
  'file_read',
  'memo_read',
  'memory_read',
];

/** Deck geometry — kept in step with the widget-context deck so a provenance
 *  stack fans out with the same spacing/peek as the chat-input snapshot deck. */
const CARD_HEIGHT = 52;
const CARD_GAP = 6;
const PEEK_STEP = 6;
const MAX_PEEK_LAYERS = 4;

/** Shared card chrome (visuals only — positioning/height is set per use). Every
 *  card is filled with `--color-bg-card` so a leaf card and the front of a
 *  stack read alike. */
const CARD_CHROME =
  'flex items-center gap-2.5 rounded-lg border px-2.5 text-left outline-none cursor-pointer ' +
  'border-[var(--color-border-muted)] bg-[var(--color-bg-card)] ' +
  'hover:border-[var(--color-border-default)] hover:bg-[var(--color-bg-elevated)] ' +
  'focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]';

const TERTIARY = { color: 'var(--color-text-tertiary)' as const };

/** Lucide icon for non-URL source types. */
function NonUrlIcon({ type, size = 14 }: { type: ProvenanceSourceType; size?: number }): React.ReactElement {
  const cls = 'flex-shrink-0';
  const props = { width: size, height: size, className: cls, style: TERTIARY };
  switch (type) {
    case 'file_read':
      return <FileText {...props} />;
    case 'memo_read':
      return <StickyNote {...props} />;
    case 'memory_read':
      return <Brain {...props} />;
    case 'market_data':
      return <LineChart {...props} />;
    case 'mcp_tool':
      return <Wrench {...props} />;
    default:
      return <FileSearch {...props} />;
  }
}

/** The row/header thumbnail: a favicon for URL sources, else a typed icon, in a
 *  small rounded tile so every source reads as a card. */
function SourceThumb({
  record,
  size = 28,
}: {
  record: ProvenanceRecord;
  size?: number;
}): React.ReactElement {
  const isUrl = URL_SOURCE_TYPES.has(record.source_type);
  // Inner glyph tracks the tile so the larger dialog tile doesn't look hollow;
  // row/deck thumbs (≤28) keep their original 14px icon.
  const inner = size <= 28 ? 14 : Math.round(size * 0.5);
  return (
    <span
      className="flex flex-shrink-0 items-center justify-center rounded-md"
      style={{ width: size, height: size, background: 'var(--color-bg-subtle)' }}
    >
      {isUrl ? (
        <Favicon domain={domainFromUrl(record.identifier)} size={inner} />
      ) : (
        <NonUrlIcon type={record.source_type} size={inner} />
      )}
    </span>
  );
}

/** hostname (sans leading www.) for URL identifiers; '' when unparseable. */
function domainFromUrl(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

function shortSha(sha?: string): string {
  if (!sha) return '';
  return sha.length > 12 ? sha.slice(0, 12) : sha;
}

function formatSize(bytes?: number): string {
  if (bytes == null) return '';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTimestamp(ts?: string): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return ts;
  return d.toLocaleString();
}

/** `task:<id>` agent attribution → true. */
function isSubagentRecord(agent?: string): boolean {
  return typeof agent === 'string' && agent.startsWith('task:');
}

/** "mcp_tool" → "Mcp Tool": humanize an unmapped enum for the i18n fallback. */
function humanizeType(type: string): string {
  return type
    .split('_')
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

/** Server-side redaction sentinel — these argument values are rendered muted. */
const REDACTED = '[redacted]';

/**
 * One-line render of ALL captured args as `key: value` pairs joined by ` · `,
 * shown verbatim (no curation) so every card surfaces exactly what the tool was
 * called with. Redaction sentinels pass through as-is (the whole subtitle is
 * already muted). Empty/null values are dropped. Returns null when there is
 * nothing to show.
 */
function argsSummary(a?: Record<string, unknown> | null): string | null {
  if (!a) return null;
  const parts = Object.entries(a)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${k}: ${argValueText(v)}`);
  return parts.length ? parts.join(' · ') : null;
}

/** Compact display for an args value: redaction sentinel verbatim, strings as
 *  themselves, everything else JSON-stringified. */
function argValueText(value: unknown): string {
  if (value === REDACTED) return REDACTED;
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

/** Which scope of provenance the panel is showing. */
type SourceScope = 'turn' | 'thread';

export interface SourcesPanelProps {
  /** Provenance records for the targeted turn, keyed by record/tool-call id. */
  provenanceRecords?: Record<string, ProvenanceRecord>;
  /**
   * Provenance records aggregated across every turn in the thread (already
   * merged by the parent; this panel dedups them). When it carries more
   * distinct sources than the turn set, a "This turn / All sources" switch
   * appears so the user can pivot between the two scopes.
   */
  allRecords?: Record<string, ProvenanceRecord>;
  /** Routes file/memo/memory identifiers through ChatView's path-aware router. */
  onOpenFile?: (path: string, workspaceId?: string) => void;
}

interface SourceRowData {
  /** Stable key for React (representative record_id, or dedup key fallback). */
  key: string;
  /** Representative record (the first seen) — drives the row label/icon. */
  record: ProvenanceRecord;
  /** Every record sharing this row's (source_type, identifier), in arrival
   *  order. A single ticker can collect several data products here; the row
   *  becomes a deck of one card per distinct access. */
  records: ProvenanceRecord[];
}

interface SourceGroup {
  type: ProvenanceSourceType;
  rows: SourceRowData[];
}

/**
 * Group records by `source_type` in {@link GROUP_ORDER}, then collapse to one
 * row per {@link provenanceDisplayKey} (e.g. one row per ticker). Records that
 * share a key are NOT dropped — they're collected on the row so it can become a
 * deck of the distinct data products behind it. Unrecognized types are appended
 * so nothing is silently dropped.
 */
function buildGroups(records?: Record<string, ProvenanceRecord>): SourceGroup[] {
  const all = Object.values(records || {});
  const byType = new Map<ProvenanceSourceType, SourceRowData[]>();
  const rowByKey = new Map<string, SourceRowData>();
  for (const record of all) {
    // Shares provenanceDisplayKey with the Sources pill's countDedupedSources
    // so the panel's row count matches the pill's number.
    const dedupKey = provenanceDisplayKey(record);
    const existing = rowByKey.get(dedupKey);
    if (existing) {
      existing.records.push(record);
      continue;
    }
    const row: SourceRowData = {
      key: record.record_id || dedupKey,
      record,
      records: [record],
    };
    rowByKey.set(dedupKey, row);
    const arr = byType.get(record.source_type);
    if (arr) arr.push(row);
    else byType.set(record.source_type, [row]);
  }
  const ordered: SourceGroup[] = [];
  for (const type of GROUP_ORDER) {
    const rows = byType.get(type);
    if (rows && rows.length > 0) ordered.push({ type, rows });
  }
  for (const [type, rows] of byType) {
    if (!GROUP_ORDER.includes(type)) ordered.push({ type, rows });
  }
  return ordered;
}

/** i18n label for a data-kind slug (e.g. "company_overview" -> "Company
 *  overview"), falling back to a humanized slug. Empty when no slug. */
function kindLabel(t: TFunction, slug?: string): string {
  if (!slug) return '';
  return t(`chat.sources.kind.${slug}`, { defaultValue: humanizeType(slug) });
}

/** Distinct records by content hash (so identical re-fetches collapse but
 *  different data products / periods stay), preserving arrival order. Falls
 *  back to the data-kind when a record carries no sha; records that share an
 *  identifier with neither a hash nor a kind are indistinguishable, so they
 *  collapse to one rather than padding the deck with look-alikes. */
function distinctByContent(records: ProvenanceRecord[]): ProvenanceRecord[] {
  const seen = new Set<string>();
  const out: ProvenanceRecord[] = [];
  for (const r of records) {
    const k = r.result_sha256 || r.detail || '';
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

/** The display title for a row/record: title, then identifier, then a localized
 *  fallback. */
function recordTitle(t: TFunction, record: ProvenanceRecord): string {
  // File-ish sources carry an absolute sandbox path (e.g. /home/workspace/x.md)
  // as their identifier and no title. Show it workspace-relative — the same
  // normalization the path router uses when the row is clicked — while the DB
  // keeps the full path.
  if (FILE_SOURCE_TYPES.has(record.source_type) && !record.title && record.identifier) {
    const rel = workspaceRelativePath(record.identifier);
    return rel || t('chat.sources.workspaceRoot');
  }
  return record.title || record.identifier || t('chat.sources.unknownSource');
}

/**
 * Lists a message's provenance records grouped by `source_type` with per-group
 * counts. A single-access row is a card that opens its detail dialog. A row
 * that collapses several accesses (e.g. one ticker read several ways) becomes a
 * peeking deck of one card per access; clicking it fans the deck open (the same
 * motion as the chat-input widget deck) and each card then opens its own detail
 * dialog.
 *
 * Display is deduped by `(source_type, identifier)` — the same URL fetched twice
 * in one turn shows once. The first record for a key wins; later duplicates are
 * collected on the row to populate its deck.
 */
export default function SourcesPanel({
  provenanceRecords,
  allRecords,
  onOpenFile,
}: SourcesPanelProps): React.ReactElement {
  const { t } = useTranslation();
  const [scope, setScope] = useState<SourceScope>('turn');
  const [selected, setSelected] = useState<ProvenanceRecord | null>(null);
  // Only one deck fans at a time (matches the widget deck's single-deck model).
  const [fannedKey, setFannedKey] = useState<string | null>(null);

  const turnCount = useMemo(() => countDedupedSources(provenanceRecords), [provenanceRecords]);
  const threadCount = useMemo(() => countDedupedSources(allRecords), [allRecords]);

  // Offer the turn/thread switch only when the whole thread has genuinely more
  // distinct sources than this turn — single-turn threads keep the original
  // per-turn-only chrome. When it's hidden, scope can't escape 'turn'.
  const showScopeSwitch = threadCount > turnCount;
  const effectiveScope: SourceScope = showScopeSwitch ? scope : 'turn';
  const activeRecords = effectiveScope === 'thread' ? allRecords : provenanceRecords;

  const groups = useMemo<SourceGroup[]>(() => buildGroups(activeRecords), [activeRecords]);

  const scopeSwitch = showScopeSwitch ? (
    <div className="flex-shrink-0 px-3 pt-3">
      <AnimatedTabs
        tabs={[
          { id: 'turn', label: `${t('chat.sources.scope.turn')} (${turnCount})` },
          { id: 'thread', label: `${t('chat.sources.scope.thread')} (${threadCount})` },
        ]}
        value={effectiveScope}
        onChange={(id) => {
          setScope(id as SourceScope);
          // Row keys differ between scopes; drop any fanned deck on a switch.
          setFannedKey(null);
        }}
        layoutId="sources-scope-tabs"
      />
    </div>
  ) : null;

  if (groups.length === 0) {
    return (
      <div className="flex h-full flex-col">
        {scopeSwitch}
        <div className="flex flex-1 items-center justify-center px-6">
          <p className="text-sm" style={TERTIARY}>
            {t(effectiveScope === 'thread' ? 'chat.sources.emptyThread' : 'chat.sources.empty')}
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      {scopeSwitch}
      <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
        {groups.map((group) => {
          const groupLabel = t(`chat.sources.groups.${group.type}`, { defaultValue: humanizeType(group.type) });
          return (
            <div key={group.type} className="mb-4">
              <div className="mb-1.5 flex items-center gap-2 px-1">
                <span
                  className="text-xs font-semibold uppercase tracking-wide"
                  style={TERTIARY}
                >
                  {groupLabel}
                </span>
                <span
                  className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium"
                  style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
                >
                  {group.rows.length}
                </span>
              </div>
              <div className="flex flex-col gap-1.5">
                {group.rows.map((row) => (
                  <SourceRow
                    key={row.key}
                    row={row}
                    fanned={fannedKey === row.key}
                    onToggleFan={() => setFannedKey((k) => (k === row.key ? null : row.key))}
                    onCollapse={() => setFannedKey((k) => (k === row.key ? null : k))}
                    onOpenRecord={setSelected}
                  />
                ))}
              </div>
            </div>
          );
        })}
      </div>
      <SourceDetailDialog
        record={selected}
        onClose={() => setSelected(null)}
        onOpenFile={onOpenFile}
      />
    </div>
  );
}

/** The card's inner content: thumb, title/subtitle, optional subagent chip, and
 *  a trailing affordance. Shared by leaf cards and deck cards. */
function SourceCardBody({
  record,
  title,
  subtitle,
  subagent,
  trailing,
}: {
  record: ProvenanceRecord;
  title: string;
  subtitle?: string;
  subagent?: boolean;
  trailing: React.ReactNode;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <>
      <SourceThumb record={record} />
      <span className="flex min-w-0 flex-1 flex-col">
        <span className="truncate text-sm" style={{ color: 'var(--color-text-primary)' }}>
          {title}
        </span>
        {subtitle && (
          <span className="truncate text-xs" style={TERTIARY} title={subtitle}>
            {subtitle}
          </span>
        )}
      </span>
      {subagent && (
        <span
          className="inline-flex flex-shrink-0 items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium"
          style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
        >
          {t('chat.sources.subagent')}
        </span>
      )}
      {trailing}
    </>
  );
}

/** Hover-revealed "open details" chevron for a card whose click opens a dialog. */
function ViewChevron(): React.ReactElement {
  return (
    <ChevronRight
      className="h-4 w-4 flex-shrink-0 opacity-0 transition-opacity group-hover:opacity-50"
      style={TERTIARY}
    />
  );
}

/**
 * One display row. A single-access row is a leaf card that opens its detail
 * dialog. A multi-access row is a {@link SourceDeck}.
 */
function SourceRow({
  row,
  fanned,
  onToggleFan,
  onCollapse,
  onOpenRecord,
}: {
  row: SourceRowData;
  fanned: boolean;
  onToggleFan: () => void;
  onCollapse: () => void;
  onOpenRecord: (record: ProvenanceRecord) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const { record, records } = row;
  const distinct = distinctByContent(records);
  const title = recordTitle(t, record);

  if (distinct.length <= 1) {
    const kind = kindLabel(t, record.detail);
    // Always surface the full captured args (not a curated subset). Fall back to
    // the data-kind, then the identifier, only when there are no args to show.
    let subtitle = argsSummary(record.args) ?? '';
    if (!subtitle) {
      if (kind) subtitle = kind;
      // For file rows the title already encodes the (normalized) identifier, so
      // the raw identifier would just re-introduce the sandbox path — skip it.
      else if (
        !FILE_SOURCE_TYPES.has(record.source_type) &&
        record.identifier &&
        record.identifier !== title
      )
        subtitle = record.identifier;
    }
    return (
      <button
        type="button"
        onClick={() => onOpenRecord(record)}
        aria-label={`${title} — ${t('chat.sources.viewDetails')}`}
        className={`group relative ${CARD_CHROME}`}
        style={{ height: CARD_HEIGHT, boxShadow: '0 1px 2px rgba(20, 20, 23, 0.05)' }}
      >
        <SourceCardBody
          record={record}
          title={title}
          subtitle={subtitle}
          subagent={isSubagentRecord(record.agent)}
          trailing={<ViewChevron />}
        />
      </button>
    );
  }

  return (
    <SourceDeck
      records={distinct}
      ticker={title}
      fanned={fanned}
      onToggleFan={onToggleFan}
      onCollapse={onCollapse}
      onOpenRecord={onOpenRecord}
    />
  );
}

/**
 * A deck of cards — one per distinct access of a single source (e.g. a ticker
 * read via company overview + daily prices + options chain). Collapsed, the
 * cards peek behind the front one and the front shows the access count; clicking
 * fans them out (the widget-context deck's exact motion) and each card then
 * opens its own detail dialog. Clicking outside, or pressing Escape, collapses.
 */
function SourceDeck({
  records,
  ticker,
  fanned,
  onToggleFan,
  onCollapse,
  onOpenRecord,
}: {
  records: ProvenanceRecord[];
  ticker: string;
  fanned: boolean;
  onToggleFan: () => void;
  onCollapse: () => void;
  onOpenRecord: (record: ProvenanceRecord) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const n = records.length;
  const peekLayers = Math.min(n - 1, MAX_PEEK_LAYERS);
  const stackHeight = fanned
    ? n * (CARD_HEIGHT + CARD_GAP) - CARD_GAP
    : CARD_HEIGHT + peekLayers * PEEK_STEP;

  // Outside-click / Escape collapse while fanned, deferred one frame so the
  // click that fanned the deck can't immediately re-collapse it. Clicks inside
  // a Radix dialog (an opened detail view) are carved out.
  useEffect(() => {
    if (!fanned) return;
    const onDown = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (target.closest && target.closest('[role="dialog"]')) return;
      if (!document.body.contains(target)) return;
      if (rootRef.current && rootRef.current.contains(target)) return;
      onCollapse();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCollapse();
    };
    let attached = false;
    const raf = requestAnimationFrame(() => {
      document.addEventListener('mousedown', onDown);
      document.addEventListener('keydown', onKey);
      attached = true;
    });
    return () => {
      cancelAnimationFrame(raf);
      if (attached) {
        document.removeEventListener('mousedown', onDown);
        document.removeEventListener('keydown', onKey);
      }
    };
  }, [fanned, onCollapse]);

  return (
    <div
      ref={rootRef}
      className="source-deck-stack"
      data-testid="source-stack"
      data-fanned={fanned}
      style={{ height: stackHeight }}
    >
      {records.map((r, i) => {
        const top = fanned ? i * (CARD_HEIGHT + CARD_GAP) : 0;
        const peekY = fanned ? 0 : i * PEEK_STEP;
        const peekScale = fanned ? 1 : Math.max(1 - i * 0.03, 0.85);
        const peekOpacity = fanned ? 1 : i === 0 ? 1 : Math.max(0.85 - (i - 1) * 0.2, 0.25);
        const interactive = fanned || i === 0;
        const isTop = i === 0;
        const kind = kindLabel(t, r.detail);
        // Always show the full captured args; fall back to the data-kind.
        const cardLabel = argsSummary(r.args) ?? kind;

        // Collapsed, the front card summarizes the deck ("N sources"); fanned,
        // every card shows its full args (or data-kind when it has none).
        const subtitle = !fanned && isTop ? t('chat.sources.sourceCount', { count: n }) : cardLabel;
        const ariaLabel =
          !fanned && isTop
            ? `${ticker} — ${t('chat.sources.expand')}`
            : `${ticker}${kind ? ` · ${kind}` : ''} — ${t('chat.sources.viewDetails')}`;
        const trailing =
          !fanned && isTop ? (
            <span className="inline-flex flex-shrink-0 items-center gap-1">
              <span
                className="inline-flex items-center justify-center rounded-full px-1 text-[10px] font-medium"
                style={{
                  minWidth: 16,
                  height: 16,
                  backgroundColor: 'var(--color-border-muted)',
                  color: 'var(--color-text-tertiary)',
                }}
              >
                {n}
              </span>
              <ChevronDown className="h-4 w-4 opacity-60" style={TERTIARY} />
            </span>
          ) : (
            <ViewChevron />
          );

        return (
          <button
            key={r.record_id || i}
            type="button"
            aria-hidden={interactive ? undefined : true}
            tabIndex={interactive ? undefined : -1}
            aria-label={ariaLabel}
            onClick={() => (fanned ? onOpenRecord(r) : onToggleFan())}
            className={`group source-deck-card absolute left-0 right-0 ${CARD_CHROME}`}
            style={{
              top,
              height: CARD_HEIGHT,
              transform: `translateY(${peekY}px) scale(${peekScale})`,
              opacity: peekOpacity,
              zIndex: n - i,
              pointerEvents: interactive ? 'auto' : 'none',
              boxShadow: fanned
                ? '0 4px 12px rgba(20, 20, 23, 0.06), 0 1px 2px rgba(20, 20, 23, 0.04)'
                : isTop
                  ? '0 1px 2px rgba(20, 20, 23, 0.06)'
                  : 'none',
            }}
          >
            <SourceCardBody
              record={r}
              title={ticker}
              subtitle={subtitle}
              subagent={isSubagentRecord(r.agent)}
              trailing={trailing}
            />
          </button>
        );
      })}
    </div>
  );
}

/** Editorial section divider: an uppercase, letter-spaced caption trailed by a
 *  hairline rule. Gives the detail dialog a consistent document-like rhythm. */
function SectionLabel({ children }: { children: React.ReactNode }): React.ReactElement {
  return (
    <div className="mb-2 flex items-center gap-2.5">
      <span
        className="whitespace-nowrap text-[10px] font-semibold uppercase tracking-[0.12em]"
        style={TERTIARY}
      >
        {children}
      </span>
      <span className="h-px flex-1" style={{ background: 'var(--color-border-muted)' }} />
    </div>
  );
}

/**
 * Centered modal (mobile: bottom sheet) showing one source's details — the same
 * click-to-open pattern as the dashboard's widget-context preview. Reads as a
 * chain-of-custody card: a source-type eyebrow + title, an "Open link"/"Open
 * file" action for URL/file sources, then the content fingerprint.
 */
function SourceDetailDialog({
  record,
  onClose,
  onOpenFile,
}: {
  record: ProvenanceRecord | null;
  onClose: () => void;
  onOpenFile?: (path: string, workspaceId?: string) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const open = record !== null;
  const isUrl = record ? URL_SOURCE_TYPES.has(record.source_type) : false;
  const isFile = record ? FILE_SOURCE_TYPES.has(record.source_type) : false;
  const title = record ? recordTitle(t, record) : '';
  const typeLabel = record
    ? t(`chat.sources.groups.${record.source_type}`, { defaultValue: humanizeType(record.source_type) })
    : '';

  // Subtitle under the title: the identifier when it adds info beyond the title,
  // else the lone data-kind.
  let subtitle = '';
  if (record) {
    if (record.identifier && record.identifier !== title) subtitle = record.identifier;
    else {
      const k = kindLabel(t, record.detail);
      if (k) subtitle = k;
    }
  }

  const canOpenLink = isUrl && !!record?.identifier && /^https?:\/\//.test(record.identifier);
  const canOpenFile = isFile && !!onOpenFile && !!record?.identifier;

  const handleOpen = () => {
    if (!record) return;
    if (canOpenLink) {
      window.open(record.identifier, '_blank', 'noopener,noreferrer');
      return;
    }
    if (canOpenFile) {
      onOpenFile?.(record.identifier);
      onClose();
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o) onClose(); }}>
      <DialogContent
        className="max-w-lg [&>*]:min-w-0"
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          borderColor: 'var(--color-border-default)',
        }}
      >
        <DialogHeader className="text-left">
          <div className="flex items-start gap-3 pr-6">
            {record && <SourceThumb record={record} size={40} />}
            <div className="flex min-w-0 flex-1 flex-col gap-1">
              <div className="flex items-center gap-2">
                <span
                  className="truncate text-[10px] font-semibold uppercase tracking-[0.12em]"
                  style={TERTIARY}
                >
                  {typeLabel}
                </span>
                {record && isSubagentRecord(record.agent) && (
                  <span
                    className="inline-flex flex-shrink-0 items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium"
                    style={{ backgroundColor: 'var(--color-accent-soft)', color: 'var(--color-accent-primary)' }}
                  >
                    {t('chat.sources.subagent')}
                  </span>
                )}
              </div>
              <DialogTitle
                className="truncate text-base leading-tight"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {title}
              </DialogTitle>
              {subtitle && (
                <DialogDescription className="truncate text-xs" style={TERTIARY} title={subtitle}>
                  {subtitle}
                </DialogDescription>
              )}
            </div>
          </div>
        </DialogHeader>

        {(canOpenLink || canOpenFile) && (
          <button
            type="button"
            onClick={handleOpen}
            className="group inline-flex w-fit items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-medium outline-none transition-all hover:gap-2 focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
            style={{
              backgroundColor: 'var(--color-accent-soft)',
              borderColor: 'var(--color-accent-soft)',
              color: 'var(--color-accent-primary)',
            }}
          >
            {canOpenLink ? <ExternalLink className="h-3.5 w-3.5" /> : <FileText className="h-3.5 w-3.5" />}
            {canOpenLink ? t('chat.sources.actions.openLink') : t('chat.sources.actions.openFile')}
          </button>
        )}

        <div className="max-h-[60vh] overflow-y-auto">
          {record && <FingerprintRows record={record} />}
        </div>
      </DialogContent>
    </Dialog>
  );
}

/** The content fingerprint for a single record: a provider/agent/accessed/
 *  checksum/size spec card, the captured args, and the snippet. The title is
 *  owned by the dialog header, so it isn't repeated here. */
function FingerprintRows({ record }: { record: ProvenanceRecord }): React.ReactElement {
  const { t } = useTranslation();
  const meta: { label: string; value: string; mono?: boolean; icon?: React.ReactNode }[] = [];
  if (record.provider) meta.push({ label: t('chat.sources.fingerprint.provider'), value: record.provider });
  if (record.agent) meta.push({ label: t('chat.sources.fingerprint.agent'), value: record.agent, mono: true });
  if (record.timestamp) meta.push({ label: t('chat.sources.fingerprint.timestamp'), value: formatTimestamp(record.timestamp) });
  if (record.result_sha256)
    meta.push({
      label: t('chat.sources.fingerprint.checksum'),
      value: shortSha(record.result_sha256),
      mono: true,
      icon: <Fingerprint className="h-3 w-3 flex-shrink-0" style={TERTIARY} />,
    });
  if (record.result_size != null) meta.push({ label: t('chat.sources.fingerprint.size'), value: formatSize(record.result_size), mono: true });

  const argEntries = record.args ? Object.entries(record.args) : [];

  return (
    <div className="source-detail-body flex flex-col gap-4">
      {meta.length > 0 && (
        <div
          className="overflow-hidden rounded-xl border"
          style={{ borderColor: 'var(--color-border-muted)', background: 'var(--color-bg-subtle)' }}
        >
          {meta.map((r, i) => (
            <div
              key={r.label}
              className="flex items-center justify-between gap-3 px-3 py-2 text-xs"
              style={i > 0 ? { borderTop: '1px solid var(--color-border-muted)' } : undefined}
            >
              <span className="flex-shrink-0" style={TERTIARY}>
                {r.label}
              </span>
              <span
                className={`flex min-w-0 items-center gap-1.5 ${r.mono ? 'font-mono' : ''}`}
                style={{ color: 'var(--color-text-secondary)' }}
                title={r.value}
              >
                {r.icon}
                <span className="truncate">{r.value}</span>
              </span>
            </div>
          ))}
        </div>
      )}
      {argEntries.length > 0 && (
        <section>
          <SectionLabel>{t('chat.sources.fingerprint.arguments')}</SectionLabel>
          <dl className="flex flex-col gap-1.5">
            {argEntries.map(([key, value]) => {
              const isRedacted = value === REDACTED;
              return (
                <div key={key} className="flex items-baseline justify-between gap-3 text-xs">
                  <dt className="flex-shrink-0 font-mono" style={TERTIARY}>
                    {key}
                  </dt>
                  <dd
                    className="flex min-w-0 items-center gap-1 break-words text-right font-mono"
                    style={{ color: isRedacted ? 'var(--color-text-tertiary)' : 'var(--color-text-secondary)' }}
                  >
                    {isRedacted && <Lock className="h-2.5 w-2.5 flex-shrink-0" aria-hidden />}
                    {argValueText(value)}
                  </dd>
                </div>
              );
            })}
          </dl>
        </section>
      )}
      {record.result_snippet && (
        <section>
          <SectionLabel>{t('chat.sources.fingerprint.snippet')}</SectionLabel>
          <div
            className="max-h-64 overflow-y-auto whitespace-pre-wrap break-words rounded-lg px-3 py-2.5 font-mono text-xs leading-relaxed"
            style={{
              color: 'var(--color-text-secondary)',
              background: 'var(--color-bg-code)',
              border: '1px solid var(--color-border-muted)',
            }}
          >
            {record.result_snippet}
          </div>
        </section>
      )}
    </div>
  );
}
