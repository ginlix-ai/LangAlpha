import { useMemo, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Search, X } from 'lucide-react';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { TagBadge } from '@/components/mcp/McpPrimitives';
import type {
  McpServerBindingPatch,
  McpToolBinding,
  McpToolSummary,
} from '@/pages/ChatAgent/utils/api';
import type { CapabilityGroup } from '../brokerages';
import { SelectCheckbox } from './SelectCheckbox';
import {
  bindingOptions,
  checkStateOf,
  commonBinding,
  filterSections,
  isSelectable,
  NO_SELECTION,
  planBulkBinding,
  planBulkReset,
  selectableIn,
  toolSections,
  type ToolListSelection,
  type ToolSection,
} from './toolSelection';

/**
 * A server's tools, with the two things a list of eighty-eight of them needs:
 * a filter, and a way to change more than one at a time. A broker publishes
 * its tools in capability groups of sixty-odd, and moving a group used to be
 * sixty-odd trips through a dropdown.
 *
 * `toolSections` buckets the list the way it is read, and that bucketing is
 * the only shape below this line: the filter narrows sections, the selection
 * counts across sections, and each one draws itself. A server with no
 * capability groups is one headerless bucket rather than a second list shape.
 *
 * Selection keys are tool names and are resolved against the whole snapshot,
 * not against what the filter is showing. Someone ticks a group, then types to
 * check one member of it, and the bar must still be holding the group it was
 * given -- a selection that shrank as they typed would apply to fewer tools
 * than the count they read before pressing anything. Select-all is the
 * opposite case and does read the filter: it is an action on what is in front
 * of the user, so it ticks the visible rows and nothing else.
 */

/** A lone tool needs no filter, and an input over one row reads as clutter. */
const FILTER_FROM = 2;

export function ToolList({
  tools,
  groups,
  granted,
  renderControl,
  onBulkPatch,
  bulkBusy = false,
  bulkError,
  children,
}: {
  tools: McpToolSummary[];
  /** Empty = the server has no capability groups, so the list is one bucket. */
  groups: CapabilityGroup[];
  granted: string[] | null | undefined;
  /** A control per reachable tool; absent = the list is read-only. */
  renderControl?: (tool: McpToolSummary) => ReactNode;
  /** One write for the whole selection. Absent = no bulk affordances. */
  onBulkPatch?: (body: McpServerBindingPatch) => void;
  bulkBusy?: boolean;
  /** The server's refusal of the last bulk write, verbatim. */
  bulkError?: string | null;
  /** The discovery line: below the list, above the bar. */
  children?: ReactNode;
}) {
  const { t } = useTranslation();
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<ReadonlySet<string>>(new Set());
  // Counted from the last apply, so the bar can say what stayed put. Cleared
  // by any change to the selection, which is what the number was about.
  const [skipped, setSkipped] = useState(0);

  const sections = useMemo(
    () => toolSections(groups, granted, tools),
    [groups, granted, tools],
  );
  const shown = useMemo(() => filterSections(sections, query), [sections, query]);
  const movableAll = useMemo(() => selectableIn(sections), [sections]);
  const movableShown = useMemo(() => selectableIn(shown), [shown]);
  const targets = useMemo(
    () => movableAll.filter((tool) => selected.has(tool.name)),
    [movableAll, selected],
  );

  const selection: ToolListSelection | undefined =
    onBulkPatch && renderControl
      ? {
          selected,
          onToggle: (name) => {
            setSkipped(0);
            setSelected((prev) => {
              const next = new Set(prev);
              if (next.has(name)) next.delete(name);
              else next.add(name);
              return next;
            });
          },
          onToggleMany: (names, on) => {
            setSkipped(0);
            setSelected((prev) => {
              const next = new Set(prev);
              for (const name of names) {
                if (on) next.add(name);
                else next.delete(name);
              }
              return next;
            });
          },
        }
      : undefined;

  const allState = checkStateOf(
    movableShown.map((tool) => tool.name),
    selected,
  );

  function apply(value: McpToolBinding) {
    const plan = planBulkBinding(targets, value);
    setSkipped(plan.skipped.length);
    if (plan.patch) onBulkPatch?.(plan.patch);
  }

  function reset() {
    const plan = planBulkReset(targets);
    setSkipped(0);
    if (plan.patch) onBulkPatch?.(plan.patch);
  }

  return (
    <div className="flex flex-col gap-2.5">
      {tools.length >= FILTER_FROM && (
        <ToolFilter value={query} onChange={setQuery} />
      )}

      {selection && movableShown.length > 0 && (
        <div className="flex items-center gap-2">
          <SelectCheckbox
            state={allState}
            label={t('plugins.detail.bulkSelectAllAria')}
            onToggle={() =>
              selection.onToggleMany(
                movableShown.map((tool) => tool.name),
                allState !== 'all',
              )
            }
          />
          <button
            type="button"
            onClick={() =>
              selection.onToggleMany(
                movableShown.map((tool) => tool.name),
                allState !== 'all',
              )
            }
            className="text-[0.6875rem] hover:underline underline-offset-2"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            {t('plugins.detail.bulkSelectAll', { count: movableShown.length })}
          </button>
        </div>
      )}

      {shown.length === 0 ? (
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('plugins.detail.searchEmpty')}
        </p>
      ) : (
        <div className="flex flex-col gap-4">
          {shown.map((section) => (
            <ToolSectionBlock
              key={section.key}
              section={section}
              copy={sectionCopy(section, granted != null, t)}
              renderControl={renderControl}
              selection={selection}
            />
          ))}
        </div>
      )}

      {children}

      {selection && targets.length > 0 && (
        <ToolBulkBar
          count={targets.length}
          value={commonBinding(targets)}
          skipped={skipped}
          busy={bulkBusy}
          error={bulkError ?? null}
          onApply={apply}
          onReset={reset}
          onClear={() =>
            selection.onToggleMany(
              targets.map((tool) => tool.name),
              false,
            )
          }
        />
      )}
    </div>
  );
}

/** The name filter. Substring and case-insensitive, on the name alone: the
 *  names are what the agent calls and what the user is looking for. */
function ToolFilter({
  value,
  onChange,
}: {
  value: string;
  onChange: (next: string) => void;
}) {
  const { t } = useTranslation();
  const label = t('plugins.detail.searchPlaceholder');
  return (
    <div className="relative rings-within">
      <Search
        className="h-3 w-3 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none"
        style={{ color: 'var(--color-text-tertiary)' }}
      />
      <input
        role="searchbox"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={label}
        aria-label={label}
        spellCheck={false}
        className="text-xs pl-7 pr-7 py-1.5 rounded-md w-full"
        style={{
          color: 'var(--color-text-primary)',
          backgroundColor: 'var(--color-bg-input)',
          border: '1px solid var(--color-border-muted)',
        }}
      />
      {value && (
        <button
          type="button"
          aria-label={t('plugins.detail.searchClear')}
          onClick={() => onChange('')}
          className="absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 rounded hover:bg-foreground/10"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          <X className="h-3 w-3" />
        </button>
      )}
    </div>
  );
}

/** What a bucket says about itself, and how densely its rows read. */
interface SectionCopy {
  /** Absent = the bucket is the whole list, which needs no header naming it. */
  label: string | null;
  /** Why this bucket is what it is, on the buckets that owe an explanation. */
  note: string | null;
  /** A name under a capability header is a chip; a name standing alone brings
   *  its description with it, because nothing above the row says what it does. */
  names: 'chip' | 'prose';
}

/**
 * A bucket's words, in quoted keys rather than a template built from the kind,
 * so the tree-wide locale sweep sees every line this list can draw.
 */
function sectionCopy(
  section: ToolSection,
  settled: boolean,
  t: (key: string) => string,
): SectionCopy {
  switch (section.kind) {
    case 'flat':
      return { label: null, note: null, names: 'prose' };
    case 'group':
      return {
        label: t(`plugins.brokerages.capabilities.${section.key}.label`),
        note: null,
        names: 'chip',
      };
    case 'never':
      return {
        label: t('plugins.brokerages.detail.neverAvailable'),
        note: t('plugins.brokerages.detail.neverAvailableNote'),
        names: 'chip',
      };
    case 'unclassified':
      return {
        label: t('plugins.brokerages.detail.unclassified'),
        // "The agent can still call them" is only true of a connection there
        // is something to call with. With nothing connected the sentence
        // promised reach the page had just finished saying does not exist.
        note: t(
          settled
            ? 'plugins.brokerages.detail.unclassifiedNote'
            : 'plugins.brokerages.detail.unclassifiedNoteUnconnected',
        ),
        names: 'chip',
      };
  }
}

/**
 * One bucket: its header, the reason it exists when it needs one, and its
 * tools. The header's checkbox stands for the rows below it that a change can
 * actually move, so a bucket of pinned tools gets a header with no box rather
 * than one that selects nothing.
 */
function ToolSectionBlock({
  section,
  copy,
  renderControl,
  selection,
}: {
  section: ToolSection;
  copy: SectionCopy;
  /** A control for each reachable tool; a dimmed bucket's tools get none. */
  renderControl?: (tool: McpToolSummary) => ReactNode;
  /** Present = rows carry checkboxes and each header stands for its bucket. */
  selection?: ToolListSelection;
}) {
  const { t } = useTranslation();
  // A control on a tool the agent cannot call would be a setting with nothing
  // to act on, so a dimmed bucket keeps neither control nor box.
  const control = renderControl && !section.dimmed ? renderControl : undefined;
  const movable = selection && control ? section.tools.filter(isSelectable) : [];
  const names = movable.map((tool) => tool.name);
  const state = checkStateOf(names, selection?.selected ?? NO_SELECTION);
  return (
    <div className="flex flex-col gap-1.5">
      {copy.label && (
        <div className="flex items-center gap-2 flex-wrap">
          {selection && names.length > 0 && (
            <SelectCheckbox
              state={state}
              label={t('plugins.detail.bulkSelectGroupAria', { name: copy.label })}
              onToggle={() => selection.onToggleMany(names, state !== 'all')}
            />
          )}
          <span
            className="text-[0.6875rem] font-medium"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {copy.label}
          </span>
          <span
            className="text-[0.6875rem]"
            style={{ color: 'var(--color-text-quaternary)' }}
            data-testid={`tool-count-${section.key}`}
          >
            {section.tools.length}
          </span>
          {section.declined && (
            <TagBadge soft>{t('plugins.brokerages.detail.declined')}</TagBadge>
          )}
        </div>
      )}
      {copy.note && (
        <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-quaternary)' }}>
          {copy.note}
        </p>
      )}
      {copy.names === 'chip' && !control ? (
        // Nothing to put at the right edge, so the names read as a cloud
        // rather than as rows of one word each.
        <div className="flex flex-wrap gap-1">
          {section.tools.map((tool) => (
            <ToolChip key={tool.name} tool={tool} dimmed={section.dimmed} />
          ))}
        </div>
      ) : (
        <div className={copy.names === 'prose' ? 'flex flex-col gap-2' : 'flex flex-col gap-1'}>
          {section.tools.map((tool) => (
            <div key={tool.name} className="flex items-start justify-between gap-2">
              <div className="flex items-start gap-2 min-w-0">
                {selection && (
                  <span className="pt-0.5 inline-flex">
                    <ToolRowCheckbox tool={tool} selection={selection} />
                  </span>
                )}
                {copy.names === 'prose' ? (
                  <ToolProse tool={tool} />
                ) : (
                  <ToolChip tool={tool} dimmed={section.dimmed} />
                )}
              </div>
              {control?.(tool)}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/** A name under a header that already said what the bucket is for. */
function ToolChip({ tool, dimmed }: { tool: McpToolSummary; dimmed: boolean }) {
  return (
    <span
      title={tool.description || undefined}
      className="text-[0.625rem] px-1.5 py-0.5 rounded break-all"
      style={{
        color: dimmed ? 'var(--color-text-quaternary)' : 'var(--color-text-tertiary)',
        backgroundColor: 'var(--color-bg-tag)',
        fontFamily: "'JetBrains Mono', 'Menlo', monospace",
      }}
    >
      {tool.name}
    </span>
  );
}

/** A name with nothing above it to explain the bucket, so it brings its own. */
function ToolProse({ tool }: { tool: McpToolSummary }) {
  return (
    <div className="flex flex-col gap-0.5 min-w-0">
      <span
        className="text-[0.6875rem] font-medium break-all"
        style={{
          color: 'var(--color-text-secondary)',
          fontFamily: "'JetBrains Mono', 'Menlo', monospace",
        }}
      >
        {tool.name}
      </span>
      {tool.description && (
        <span
          className="text-[0.6875rem] line-clamp-2"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {tool.description}
        </span>
      )}
    </div>
  );
}

/**
 * A row's own box, or the space one would take. The blank keeps a pinned tool
 * lined up with the rows around it -- the pinned label already says why it has
 * no box, and a row that simply lost its indent read as a rendering slip.
 */
function ToolRowCheckbox({
  tool,
  selection,
}: {
  tool: McpToolSummary;
  selection: ToolListSelection;
}) {
  const { t } = useTranslation();
  if (!isSelectable(tool)) {
    return <span aria-hidden className="h-3.5 w-3.5 flex-shrink-0" />;
  }
  return (
    <SelectCheckbox
      state={selection.selected.has(tool.name) ? 'all' : 'none'}
      label={t('plugins.detail.bulkSelectToolAria', { name: tool.name })}
      onToggle={() => selection.onToggle(tool.name)}
    />
  );
}

/**
 * What the selection can be told to do, pinned to the bottom of the dialog's
 * own scroll port. Sticky rather than fixed: the bar belongs to this list, and
 * a fixed one would hang over the sections below it once the user scrolled
 * past the tools entirely.
 *
 * The segments read as state, not as three fire-once buttons: a selection that
 * already sits on one binding shows it, and a mixed one shows none.
 */
function ToolBulkBar({
  count,
  value,
  skipped,
  busy,
  error,
  onApply,
  onReset,
  onClear,
}: {
  count: number;
  value: McpToolBinding | null;
  /** Selected tools the last apply could not move, and why the note is there. */
  skipped: number;
  busy: boolean;
  error: string | null;
  onApply: (value: McpToolBinding) => void;
  onReset: () => void;
  onClear: () => void;
}) {
  const { t } = useTranslation();
  return (
    <div
      className="sticky bottom-0 z-10 pt-2"
      style={{ backgroundColor: 'var(--color-bg-elevated)' }}
      data-testid="tool-bulk-bar"
    >
      {/* A stuck box parks on the scroll port's CONTENT edge, so the port's own
          bottom padding is a strip the rows keep scrolling through underneath
          the bar, which reads as the bar floating over a hole. This paints that
          strip. Painted rather than reached with a negative bottom margin,
          which pushes the border box past the containing block and drops the
          box out of position entirely; `h-5` is the overlay body's `py-5`. */}
      <span
        aria-hidden
        className="absolute left-0 right-0 top-full h-5"
        style={{ backgroundColor: 'var(--color-bg-elevated)' }}
      />
      <div
        className="flex items-center gap-2 flex-wrap px-2.5 py-2 rounded-lg"
        style={{
          backgroundColor: 'var(--color-bg-card)',
          border: '1px solid var(--color-border-muted)',
          boxShadow: 'var(--shadow-card)',
        }}
      >
        <span
          className="text-[0.6875rem] font-medium"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {t('plugins.detail.bulkSelected', { count })}
        </span>
        <SegmentedControl
          size="compact"
          value={value}
          label={t('plugins.detail.bulkBindingAria')}
          disabled={busy}
          options={bindingOptions(t)}
          onChange={onApply}
        />
        <button
          type="button"
          disabled={busy}
          onClick={onReset}
          className="px-1.5 py-0.5 text-[0.6875rem] rounded transition-colors hover:bg-foreground/10 disabled:opacity-50"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('plugins.detail.bulkReset')}
        </button>
        <button
          type="button"
          onClick={onClear}
          className="px-1.5 py-0.5 text-[0.6875rem] rounded transition-colors hover:bg-foreground/10"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('plugins.detail.bulkClear')}
        </button>
      </div>
      {skipped > 0 && (
        <p className="pt-1 text-[0.625rem]" style={{ color: 'var(--color-text-quaternary)' }}>
          {t('plugins.detail.bulkSkipped', { count: skipped })}
        </p>
      )}
      {error && (
        <p role="alert" className="pt-1 text-[0.625rem]" style={{ color: 'var(--color-loss)' }}>
          {error}
        </p>
      )}
    </div>
  );
}
