import { describe, it, expect } from 'vitest';
import type { McpToolSummary } from '@/pages/ChatAgent/utils/api';
import type { CapabilityGroup } from '../brokerages';
import {
  checkStateOf,
  commonBinding,
  filterSections,
  isSelectable,
  matchesToolFilter,
  permitsBinding,
  planBulkBinding,
  planBulkReset,
  selectableIn,
  toolSections,
} from '../components/toolSelection';

/**
 * The rules a bulk change is made of, away from the dialog that draws them.
 * A broker publishes up to 88 tools in groups of sixty-odd, so "what does this
 * checkbox stand for" and "what does one Apply send" are the two answers the
 * whole feature rests on.
 */

const GROUPS: CapabilityGroup[] = [
  { key: 'market_data', tone: 'neutral' } as CapabilityGroup,
  { key: 'trading', tone: 'danger', rung: true } as CapabilityGroup,
];

const tool = (over: Partial<McpToolSummary>): McpToolSummary =>
  ({ name: 'a_tool', description: '', input_schema: {}, ...over }) as McpToolSummary;

const QUOTE = tool({ name: 'quote_get_price', capability: 'market_data', binding: 'ptc' });
const DEPTH = tool({ name: 'quote_get_depth', capability: 'market_data', binding: 'ptc' });
// Every order tool is pinned to direct by the server, whatever it does to the
// order, so both of these carry the same pin and the same one-value list.
const CANCEL = tool({
  name: 'trading_order_cancel',
  capability: 'trading',
  binding: 'direct',
  binding_source: 'policy',
  allowed: ['direct'],
  approval: true,
});
const PLACE = tool({
  name: 'trading_order_place',
  capability: 'trading',
  binding: 'direct',
  binding_source: 'policy',
  allowed: ['direct'],
  approval: true,
});
// The restriction that is not a pin: a curated vendor permits a tool no
// capability group names, but only from the sandbox. Nothing on the row pins
// it, so it takes a box and a bulk change can reach it.
const ODD = tool({
  name: 'misc_ping',
  capability: null,
  always_denied: false,
  binding: 'ptc',
  binding_source: 'default',
  allowed: ['ptc'],
});
const WITHHELD = tool({ name: 'admin_wipe', capability: null, always_denied: true });
const LEGACY = tool({ name: 'legacy_thing', capability: null });

describe('toolSections', () => {
  it('reads a whole list as one bucket when the server has no groups', () => {
    const sections = toolSections([], null, [QUOTE, CANCEL]);

    expect(sections).toHaveLength(1);
    expect(sections[0].kind).toBe('flat');
    expect(sections[0].dimmed).toBe(false);
    expect(sections[0].tools.map((x) => x.name)).toEqual([
      'quote_get_price',
      'trading_order_cancel',
    ]);
  });

  it('dims a group the connection was offered and refused', () => {
    const sections = toolSections(GROUPS, ['market_data'], [QUOTE, CANCEL]);

    expect(sections.map((s) => [s.key, s.dimmed, s.declined])).toEqual([
      ['market_data', false, false],
      ['trading', true, true],
    ]);
  });

  // Nothing is granted yet, so nothing is declined either: the list is what the
  // vendor offers, not what a connection that does not exist may do.
  it('declines nothing before there is a connection', () => {
    const sections = toolSections(GROUPS, null, [QUOTE, CANCEL]);

    expect(sections.every((s) => !s.declined && !s.dimmed)).toBe(true);
  });

  // The two trailing buckets are opposite states, and drawing one as the other
  // is the failure that matters on a page about a trading account.
  it('splits the ungrouped tools into withheld and unclassified', () => {
    const sections = toolSections(GROUPS, ['market_data'], [ODD, WITHHELD, LEGACY]);

    expect(sections.map((s) => s.kind)).toEqual(['never', 'unclassified']);
    // Absent `always_denied` is an older server, whose policy was an allowlist.
    expect(sections[0].tools.map((x) => x.name)).toEqual(['admin_wipe', 'legacy_thing']);
    expect(sections[1].tools.map((x) => x.name)).toEqual(['misc_ping']);
  });

  it('drops a group the snapshot has no tools for', () => {
    const sections = toolSections(GROUPS, ['market_data', 'trading'], [QUOTE]);

    expect(sections.map((s) => s.key)).toEqual(['market_data']);
  });
});

describe('the name filter', () => {
  it('matches a substring in any case', () => {
    expect(matchesToolFilter(QUOTE, 'GET_PR')).toBe(true);
    expect(matchesToolFilter(QUOTE, ' quote ')).toBe(true);
    expect(matchesToolFilter(QUOTE, 'order')).toBe(false);
  });

  it('matches everything on an empty query', () => {
    expect(matchesToolFilter(QUOTE, '')).toBe(true);
    expect(matchesToolFilter(QUOTE, '   ')).toBe(true);
  });

  // The count beside a group header is the bucket's length, so narrowing the
  // list has to narrow the buckets rather than hide rows underneath a stale
  // number. A group with nothing left drops out entirely.
  it('narrows each bucket and drops the ones left empty', () => {
    const sections = toolSections(GROUPS, ['market_data', 'trading'], [
      QUOTE,
      DEPTH,
      CANCEL,
    ]);
    const shown = filterSections(sections, 'depth');

    expect(shown.map((s) => [s.key, s.tools.length])).toEqual([['market_data', 1]]);
  });
});

describe('what a bulk change may reach', () => {
  // A pinned tool answers 422 to anything but its pin, so it never enters a
  // selection: the row says where it runs instead of offering a box that fails.
  // A tool the server merely restricts is a different case and keeps its box.
  it('never offers a box on a tool the server pins', () => {
    expect(isSelectable(PLACE)).toBe(false);
    expect(isSelectable(CANCEL)).toBe(false);
    expect(isSelectable(ODD)).toBe(true);
    expect(isSelectable(QUOTE)).toBe(true);
  });

  it('leaves out the pinned rows and everything in a dimmed bucket', () => {
    const sections = toolSections(GROUPS, ['market_data'], [QUOTE, PLACE, CANCEL, WITHHELD]);

    // trading is declined here, so both its tools are unreachable, and the
    // withheld bucket is refused whatever is granted.
    expect(selectableIn(sections).map((x) => x.name)).toEqual(['quote_get_price']);
  });
});

describe('the group checkbox state', () => {
  it('reads none, some and all off the rows it stands for', () => {
    const names = ['a', 'b', 'c'];

    expect(checkStateOf(names, new Set())).toBe('none');
    expect(checkStateOf(names, new Set(['b']))).toBe('some');
    expect(checkStateOf(names, new Set(['a', 'b', 'c']))).toBe('all');
  });

  // A selection holding tools this group does not draw must not read as full.
  it('ignores selected names that are not its own', () => {
    expect(checkStateOf(['a'], new Set(['a', 'z']))).toBe('all');
    expect(checkStateOf([], new Set(['z']))).toBe('none');
  });
});

describe('one write for the whole selection', () => {
  it('builds a single patch naming every tool it moves', () => {
    const plan = planBulkBinding([QUOTE, DEPTH], 'direct');

    expect(plan.patch).toEqual({
      tool_binding_set: { quote_get_price: 'direct', quote_get_depth: 'direct' },
    });
    expect(plan.skipped).toEqual([]);
  });

  // The server refuses the whole patch on the first bad pair, so a value one
  // tool does not take would cost the other changes in the same write.
  it('drops a tool whose allowed list refuses the value, and counts it', () => {
    const plan = planBulkBinding([QUOTE, PLACE], 'ptc');

    expect(plan.patch).toEqual({ tool_binding_set: { quote_get_price: 'ptc' } });
    expect(plan.skipped).toEqual(['trading_order_place']);
  });

  // `allowed` is the whole rule, and it restricts tools the row does not pin:
  // a tool no capability group names is reachable, but only from the sandbox.
  // It keeps its box, so a selection can hold one and the skip has to survive.
  it('drops a tool whose allowed list refuses the value even unpinned', () => {
    expect(permitsBinding(ODD, 'both')).toBe(false);
    expect(permitsBinding(ODD, 'direct')).toBe(false);
    expect(permitsBinding(ODD, 'ptc')).toBe(true);

    const plan = planBulkBinding([QUOTE, ODD], 'both');

    expect(plan.patch).toEqual({ tool_binding_set: { quote_get_price: 'both' } });
    expect(plan.skipped).toEqual(['misc_ping']);
  });

  // A response without `allowed` is a stale one, not a locked tool: every
  // value stays on offer so an older server never freezes a whole selection.
  it('treats a missing allowed list as unrestricted', () => {
    expect(permitsBinding(QUOTE, 'both')).toBe(true);
    expect(permitsBinding(QUOTE, 'direct')).toBe(true);
  });

  it('sends nothing when the value is allowed on none of them', () => {
    const plan = planBulkBinding([PLACE], 'ptc');

    expect(plan.patch).toBeNull();
    expect(plan.applied).toEqual([]);
  });

  // Reset names the tools rather than writing a value: precedence is the
  // server's, and removing the override is the only honest way back.
  it('resets as one unset list', () => {
    expect(planBulkReset([QUOTE, DEPTH]).patch).toEqual({
      tool_binding_unset: ['quote_get_price', 'quote_get_depth'],
    });
    expect(planBulkReset([]).patch).toBeNull();
  });
});

describe('the bar segments', () => {
  it('shows a binding the whole selection shares and none otherwise', () => {
    expect(commonBinding([QUOTE, DEPTH])).toBe('ptc');
    expect(commonBinding([QUOTE, CANCEL])).toBeNull();
    expect(commonBinding([])).toBeNull();
  });

  // An unset binding is `ptc` on the wire's own default, so two tools nobody
  // has touched read as one answer rather than as a mixed selection.
  it('reads an absent binding as ptc', () => {
    expect(commonBinding([tool({ name: 'x' }), QUOTE])).toBe('ptc');
  });
});
