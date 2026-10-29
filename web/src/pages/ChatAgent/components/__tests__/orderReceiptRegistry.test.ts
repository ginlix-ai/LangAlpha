/**
 * The gate that lets an order receipt render inline.
 *
 * A direct MCP tool is named `mcp__<server>__<tool>`, one name per user per
 * connection, so `INLINE_ARTIFACT_TOOLS` could never enumerate it: the gate has
 * to key on the artifact instead. These lock that in both directions, because
 * getting it wrong either drops the card (an order with no receipt on screen)
 * or promotes every direct tool result into a card slot it has nothing to draw.
 */
import { describe, it, expect } from 'vitest';
import {
  INLINE_ARTIFACT_MAP,
  INLINE_ARTIFACT_TOOLS,
  isInlineArtifactReady,
} from '../charts/InlineArtifactCards';

const TOOL = 'mcp__moomoo__sim_trade_input_order';
const RECEIPT = {
  type: 'order_receipt',
  direct_mcp: { server: 'moomoo', tool: 'sim_trade_input_order' },
  order_receipt: { attempt_id: 'a1', outcome: { status: 'submitted' } },
};

describe('the inline artifact registry', () => {
  it('draws a card for an order receipt', () => {
    expect(INLINE_ARTIFACT_MAP.order_receipt).toBeTypeOf('function');
  });

  it('opens the gate on the artifact type, with no tool name registered', () => {
    expect(INLINE_ARTIFACT_TOOLS.has(TOOL)).toBe(false);
    expect(isInlineArtifactReady(TOOL, RECEIPT)).toBe(true);
  });

  it('leaves every other direct tool result out of the card zone', () => {
    expect(isInlineArtifactReady(TOOL, { direct_mcp: { server: 'moomoo' } })).toBe(false);
    expect(isInlineArtifactReady(TOOL, undefined)).toBe(false);
    expect(isInlineArtifactReady(TOOL, null)).toBe(false);
  });

  it('still opens on a registered tool name, which is the older half', () => {
    expect(isInlineArtifactReady('get_quote', { type: 'quote' })).toBe(true);
    expect(isInlineArtifactReady('get_quote', { some: 'artifact' })).toBe(true);
  });
});
