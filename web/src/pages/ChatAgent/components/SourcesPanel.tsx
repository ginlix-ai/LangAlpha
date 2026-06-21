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

/** Deck geometry — spacing/fan motion kept in step with the widget-context deck
 *  (the chat-input snapshot deck). MAX_PEEK_LAYERS is intentionally shallower
 *  here: provenance decks can hold many results, so a collapsed deck only hints
 *  "there's more behind" with a couple of peek cards rather than a deep stack. */
const CARD_HEIGHT = 52;
const CARD_GAP = 6;
const PEEK_STEP = 6;
const MAX_PEEK_LAYERS = 2;

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

/** pathname + query of a URL — the part after the origin, used to label a page
 *  within a domain deck (the domain is already the deck label). '' for a bare
 *  origin or an unparseable URL, so callers can fall back to the full URL. */
function urlPath(url: string): string {
  try {
    const u = new URL(url);
    const p = u.pathname + u.search;
    return p === '/' ? '' : p;
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
 * One-line render of the captured args as `key: value` pairs joined by ` · `,
 * shown verbatim (no curation) so every card surfaces what the tool was called
 * with. Redaction sentinels pass through as-is (the whole subtitle is already
 * muted). Empty/null values are dropped. `omit` skips keys already conveyed
 * elsewhere (e.g. a domain deck's `url`, since the card title is the path).
 * Returns null when there is nothing to show.
 */
function argsSummary(a?: Record<string, unknown> | null, omit?: Set<string>): string | null {
  if (!a) return null;
  const parts = Object.entries(a)
    .filter(([k, v]) => v !== undefined && v !== null && v !== '' && !omit?.has(k))
    .map(([k, v]) => `${k}: ${argValueText(v)}`);
  return parts.length ? parts.join(' · ') : null;
}

/** Arg keys already shown by a URL-derived card title (the page path, or the
 *  full URL on a lone web_fetch), so they're dropped from the subtitle rather
 *  than repeating the URL. */
const URL_REDUNDANT_ARGS = new Set(['url']);

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

/** The search query captured on a web_search record (its `args.query`), if any. */
function queryText(record: ProvenanceRecord): string {
  const q = record.args?.query;
  return typeof q === 'string' ? q : '';
}

/**
 * Row-grouping key. Two source types collapse beyond plain identifier dedup:
 *  - `web_search`: results from one query share a `tool_call_id`, so a whole
 *    search (20 links) becomes one row labeled by the query.
 *  - `web_fetch`: pages from the same origin collapse into one row labeled by
 *    the domain, so reading several pages off one site doesn't spread into a
 *    wall of rows. A lone page from a domain stays a single (leaf) row.
 * Both fall back to the `(source_type, identifier)` dedup when there's nothing
 * to group under (no tool_call_id/query; an unparseable URL). Note this
 * intentionally diverges from the Sources pill's {@link countDedupedSources}:
 * the pill still counts distinct URLs (how many pages were read), while the
 * panel groups them by search / by site.
 */
function rowKey(record: ProvenanceRecord): string {
  if (record.source_type === 'web_search') {
    const search = record.tool_call_id || queryText(record) || record.identifier;
    return `web_search:${search}`;
  }
  if (record.source_type === 'web_fetch') {
    const domain = domainFromUrl(record.identifier);
    if (domain) return `web_fetch@${domain}`;
  }
  return provenanceDisplayKey(record);
}

/**
 * Group records by `source_type` in {@link GROUP_ORDER}, then collapse to one
 * row per {@link rowKey} (one row per ticker; one row per web search). Records
 * that share a key are NOT dropped — they're collected on the row so it can
 * become a deck of the distinct accesses/results behind it. Unrecognized types
 * are appended so nothing is silently dropped.
 */
function buildGroups(records?: Record<string, ProvenanceRecord>): SourceGroup[] {
  const all = Object.values(records || {});
  const byType = new Map<ProvenanceSourceType, SourceRowData[]>();
  const rowByKey = new Map<string, SourceRowData>();
  for (const record of all) {
    const dedupKey = rowKey(record);
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

/** Distinct records, preserving arrival order. `byIdentifier` picks what
 *  "distinct" means for the deck's shape:
 *   - List decks (web_search/web_fetch) — each card is a distinct URL, so key on
 *     `identifier`. Two different URLs that happen to return byte-identical
 *     content (block/paywall/redirect pages share a hash) stay separate cards
 *     rather than one silently swallowing the other; a true re-fetch of the same
 *     URL still collapses.
 *   - Entity decks (a shared identifier, e.g. a ticker read several ways) — key
 *     on content hash so the same data product collapses but different
 *     products/periods stay; fall back to data-kind, then identifier, so a
 *     hash-less, kind-less pair collapses rather than padding with look-alikes. */
function distinctByContent(
  records: ProvenanceRecord[],
  byIdentifier = false,
): ProvenanceRecord[] {
  const seen = new Set<string>();
  const out: ProvenanceRecord[] = [];
  for (const r of records) {
    const k = byIdentifier
      ? r.identifier || r.result_sha256 || r.detail || ''
      : r.result_sha256 || r.detail || r.identifier || '';
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
  // Per-category fold: a source-type in this set has its whole group collapsed
  // to just its header, so a noisy category can be tucked away entirely.
  const [collapsedGroups, setCollapsedGroups] = useState<Set<ProvenanceSourceType>>(
    () => new Set(),
  );

  const toggleGroup = (type: ProvenanceSourceType) => {
    // Folding unmounts the group's rows; drop any fanned deck so it can't linger
    // open behind a collapsed header.
    setFannedKey(null);
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  };

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
          const collapsed = collapsedGroups.has(group.type);
          return (
            <div key={group.type} className="mb-4">
              <button
                type="button"
                onClick={() => toggleGroup(group.type)}
                aria-expanded={!collapsed}
                aria-label={`${groupLabel} — ${t(collapsed ? 'chat.sources.expand' : 'chat.sources.collapse')}`}
                className="mb-1.5 flex w-full items-center gap-2 rounded-md px-1 py-0.5 text-left outline-none transition-colors hover:bg-[var(--color-bg-subtle)] focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
              >
                {collapsed ? (
                  <ChevronRight className="h-3.5 w-3.5 flex-shrink-0" style={TERTIARY} />
                ) : (
                  <ChevronDown className="h-3.5 w-3.5 flex-shrink-0" style={TERTIARY} />
                )}
                <span
                  className="text-xs font-semibold uppercase tracking-wide"
                  style={TERTIARY}
                >
                  {groupLabel}
                </span>
                <span
                  data-testid={`group-count-${group.type}`}
                  className="inline-flex items-center rounded-full px-1.5 py-0.5 text-[10px] font-medium"
                  style={{ backgroundColor: 'var(--color-border-muted)', color: 'var(--color-text-tertiary)' }}
                >
                  {group.rows.length}
                </span>
              </button>
              {!collapsed && (
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
              )}
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
  const isWebSearch = record.source_type === 'web_search';
  const isWebFetch = record.source_type === 'web_fetch';
  // List decks dedup by URL; entity decks dedup by content hash (see distinctByContent).
  const distinct = distinctByContent(records, isWebSearch || isWebFetch);
  const title = recordTitle(t, record);

  if (distinct.length <= 1) {
    const kind = kindLabel(t, record.detail);
    // Surface the captured args (not a curated subset). For a lone web_fetch the
    // title is already the full URL, so drop the redundant `url` arg. Fall back
    // to the data-kind, then the identifier, only when there are no args to show.
    let subtitle = argsSummary(record.args, isWebFetch ? URL_REDUNDANT_ARGS : undefined) ?? '';
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

  // Three deck shapes, by how the row was grouped (see rowKey):
  //  - 'query' (web_search): labeled by the query; each card is a result page.
  //  - 'domain' (web_fetch): labeled by the site; each card is a page on it.
  //  - 'entity' (default, e.g. a ticker): labeled by the entity; cards share it.
  const variant = isWebSearch ? 'query' : isWebFetch ? 'domain' : 'entity';
  const frontLabel = isWebSearch
    ? queryText(record) || t('chat.sources.groups.web_search')
    : isWebFetch
      ? domainFromUrl(record.identifier) || title
      : title;

  return (
    <SourceDeck
      records={distinct}
      frontLabel={frontLabel}
      variant={variant}
      fanned={fanned}
      onToggleFan={onToggleFan}
      onCollapse={onCollapse}
      onOpenRecord={onOpenRecord}
    />
  );
}

/**
 * A deck of cards behind one front card. Three shapes, set by `variant`:
 *  - `entity`: one source read several ways (e.g. a ticker via company overview
 *    + daily prices + options chain). Every card shares `frontLabel` (the
 *    entity); cards differ by their data-kind/args subtitle.
 *  - `query`: one web search's many results. The front is labeled by the query;
 *    each card keeps its own result title and shows its domain.
 *  - `domain`: pages fetched from one site. The front is labeled by the domain;
 *    each card shows its page path (the domain is already on the front).
 *
 * Collapsed, the cards peek behind the front one and the front shows the count;
 * clicking fans them out (the widget-context deck's exact motion) and each card
 * then opens its own detail dialog. Clicking outside, or pressing Escape,
 * collapses.
 */
function SourceDeck({
  records,
  frontLabel,
  variant,
  fanned,
  onToggleFan,
  onCollapse,
  onOpenRecord,
}: {
  records: ProvenanceRecord[];
  frontLabel: string;
  variant: 'entity' | 'query' | 'domain';
  fanned: boolean;
  onToggleFan: () => void;
  onCollapse: () => void;
  onOpenRecord: (record: ProvenanceRecord) => void;
}): React.ReactElement {
  const { t } = useTranslation();
  // A "list" deck (query/domain) labels each card by its own page; an entity
  // deck shares the front label across cards and subtitles by data-kind.
  const isEntity = variant === 'entity';
  const rootRef = useRef<HTMLDivElement | null>(null);
  const n = records.length;
  const peekLayers = Math.min(n - 1, MAX_PEEK_LAYERS);
  const stackHeight = fanned
    ? n * (CARD_HEIGHT + CARD_GAP) - CARD_GAP
    : CARD_HEIGHT + peekLayers * PEEK_STEP;
  // Collapsed, only render the front + a capped number of peek cards so a big
  // deck doesn't stack arbitrarily deep (the true count stays on the front
  // badge). Fanned, render every card. Capping the rendered set — not just the
  // stack height — keeps the deepest peek card flush with the stack's bottom.
  const visible = fanned ? records : records.slice(0, peekLayers + 1);
  // The collapsed front card's count noun tracks the grouping (sources /
  // results / pages). It's deck-level — depends only on variant — so compute
  // it once here rather than per card in the map below.
  const countKey =
    variant === 'query'
      ? 'chat.sources.resultCount'
      : variant === 'domain'
        ? 'chat.sources.pageCount'
        : 'chat.sources.sourceCount';

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
      {visible.map((r, i) => {
        const top = fanned ? i * (CARD_HEIGHT + CARD_GAP) : 0;
        const peekY = fanned ? 0 : i * PEEK_STEP;
        const peekScale = fanned ? 1 : Math.max(1 - i * 0.03, 0.85);
        const peekOpacity = fanned ? 1 : i === 0 ? 1 : Math.max(0.85 - (i - 1) * 0.2, 0.25);
        const interactive = fanned || i === 0;
        const isTop = i === 0;
        const collapsedFront = !fanned && isTop;
        const kind = kindLabel(t, r.detail);
        // Per-card title: entity cards share the front label; query cards show
        // the result's own page title; domain cards show the page path (the
        // domain is already on the front).
        const cardTitle =
          variant === 'query'
            ? recordTitle(t, r)
            : variant === 'domain'
              ? urlPath(r.identifier) || recordTitle(t, r)
              : frontLabel;
        // Per-card subtitle (fanned): entity → full args / data-kind; query →
        // the result's domain; domain → the args (minus the redundant `url`), so
        // a page's fetch prompt still shows even though the path is the title.
        const cardLabel =
          variant === 'query'
            ? domainFromUrl(r.identifier)
            : variant === 'domain'
              ? (argsSummary(r.args, URL_REDUNDANT_ARGS) ?? '')
              : (argsSummary(r.args) ?? kind);

        // Collapsed, the front card summarizes the deck (count noun via
        // countKey above); fanned, each card shows its own label.
        const subtitle = collapsedFront ? t(countKey, { count: n }) : cardLabel;
        const ariaLabel = collapsedFront
          ? `${frontLabel} — ${t('chat.sources.expand')}`
          : !isEntity
            ? `${cardTitle} — ${t('chat.sources.viewDetails')}`
            : `${frontLabel}${kind ? ` · ${kind}` : ''} — ${t('chat.sources.viewDetails')}`;
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
            {/* Collapsed peek cards (behind the front) render as blank card
                surfaces — showing their content would bleed favicons/titles out
                below the front card as a garbled "tail". Only the front card (or
                every card once fanned) shows its body. */}
            {interactive && (
              <SourceCardBody
                record={r}
                title={collapsedFront ? frontLabel : cardTitle}
                subtitle={subtitle}
                subagent={isSubagentRecord(r.agent)}
                trailing={trailing}
              />
            )}
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
