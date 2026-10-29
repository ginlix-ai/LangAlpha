/**
 * Direct MCP tools arrive as `mcp__<server>__<tool>` with a content-block
 * envelope for a result. These lock the name split, the masked summary line,
 * and the result classification the chat renders from.
 */
import { describe, it, expect } from 'vitest';
import {
  directToolIdentity,
  parseDirectToolName,
  isDirectToolName,
  directToolDisplayName,
  isAccountIdKey,
  maskAccountId,
  maskedAccountValue,
  summarizeDirectToolArgs,
  parseDirectToolResult,
  directToolRejectionReason,
} from '../directTools';
import { getDisplayName, getCompletedSummary, getToolIcon } from '../../components/toolDisplayConfig';

describe('parseDirectToolName', () => {
  it('splits server and tool on the double underscore', () => {
    expect(parseDirectToolName('mcp__moomoo__sim_trade_account_list')).toEqual({
      server: 'moomoo',
      tool: 'sim_trade_account_list',
    });
  });

  it('keeps a tool name that itself contains a double underscore intact', () => {
    expect(parseDirectToolName('mcp__srv__a__b')).toEqual({ server: 'srv', tool: 'a__b' });
  });

  it('is null for every other tool', () => {
    expect(parseDirectToolName('Bash')).toBeNull();
    expect(parseDirectToolName('mcp_x')).toBeNull();
    expect(parseDirectToolName('mcp__')).toBeNull();
    expect(parseDirectToolName(undefined)).toBeNull();
    expect(isDirectToolName('get_quote')).toBe(false);
  });
});

describe('directToolDisplayName', () => {
  it('turns underscores into spaces and sentence-cases', () => {
    expect(directToolDisplayName('mcp__moomoo__sim_trade_account_list')).toBe('Sim trade account list');
    expect(directToolDisplayName('mcp__moomoo__trading_order_place')).toBe('Trading order place');
  });

  it('feeds the display name for a direct tool and nothing else', () => {
    expect(directToolDisplayName('mcp__moomoo__trading_order_place')).toBe('Trading order place');
    expect(directToolDisplayName('Bash')).toBeNull();
    expect(getDisplayName('mcp__moomoo__trading_order_place')).toBe('Trading order place');
    expect(getDisplayName('Bash')).toBe('Bash');
  });

  it('gives a direct tool the plug icon', () => {
    expect(getToolIcon('mcp__moomoo__sim_trade_cash_info').displayName).toBe('Plug');
  });
});

describe('account id masking', () => {
  it('matches acc_id, account_id and keys ending in account_id', () => {
    expect(isAccountIdKey('acc_id')).toBe(true);
    expect(isAccountIdKey('account_id')).toBe(true);
    expect(isAccountIdKey('trading_account_id')).toBe(true);
    expect(isAccountIdKey('code')).toBe(false);
    expect(isAccountIdKey('account_type')).toBe(false);
  });

  // Robinhood says account_number and rhs_account_number, IBKR says acctId,
  // and a broker that only ever sends one says account. One key set covers
  // them, because every surface that masks the field reads it from here.
  it('matches the number spellings and the bare account', () => {
    expect(isAccountIdKey('account_number')).toBe(true);
    expect(isAccountIdKey('rhs_account_number')).toBe(true);
    expect(isAccountIdKey('accountNumber')).toBe(true);
    expect(isAccountIdKey('acc_no')).toBe(true);
    expect(isAccountIdKey('account')).toBe(true);
    expect(isAccountIdKey('acctId')).toBe(true);
  });

  it('leaves a key that only starts the same way alone', () => {
    expect(isAccountIdKey('access_id')).toBe(false);
    expect(isAccountIdKey('accounting')).toBe(false);
    expect(isAccountIdKey('account_status')).toBe(false);
  });

  // `account` is one of the spellings, and a key by that name can hold the
  // whole record. Four dots and the tail of "[object Object]" would lose it.
  it('masks only the scalar an id can be', () => {
    expect(maskedAccountValue('account_number', '123456789')).toBe('••••6789');
    expect(maskedAccountValue('acc_id', 12345678)).toBe('••••5678');
    expect(maskedAccountValue('symbol', '123456789')).toBeNull();
    expect(maskedAccountValue('account', { id: '1234567', name: 'Margin' })).toBeNull();
    expect(maskedAccountValue('account_id', null)).toBeNull();
  });

  it('shows only the last four characters', () => {
    expect(maskAccountId('1234567')).toBe('••••4567');
    expect(maskAccountId(12345678)).toBe('••••5678');
    expect(maskAccountId('123')).toBe('••••');
  });

  it('masks in the collapsed summary only', () => {
    const args = { acc_id: '1234567', code: 'US.AAPL', qty: '1' };
    expect(summarizeDirectToolArgs(args)).toBe('acc_id ••••4567 · code US.AAPL · qty 1');
    expect(getCompletedSummary('mcp__moomoo__trading_order_place', { args })).toBe(
      'acc_id ••••4567 · code US.AAPL · qty 1',
    );
  });

  it('masks a camelCase account id', () => {
    expect(summarizeDirectToolArgs({ accountId: '1234567' })).toBe('accountId ••••4567');
  });

  it('masks an account id nested inside an object or array', () => {
    expect(summarizeDirectToolArgs({ order: { account_id: '1234567', qty: 1 } })).toBe(
      'order {"account_id":"••••4567","qty":1}',
    );
    expect(summarizeDirectToolArgs({ legs: [{ acctId: '1234567' }] })).toBe(
      'legs [{"acctId":"••••4567"}]',
    );
  });

  it('caps the summary and counts the rest', () => {
    const args = { a: 1, b: 2, c: 3, d: 4, e: 5, f: 6 };
    expect(summarizeDirectToolArgs(args)).toBe('a 1 · b 2 · c 3 · d 4 · +2');
    expect(summarizeDirectToolArgs({})).toBeNull();
  });
});

describe('parseDirectToolResult', () => {
  const inner = { ret_code: 0, ret_msg: 'success', data: { acc_list: [{ acc_id: 1234567 }] } };
  const envelope = JSON.stringify([{ type: 'text', text: JSON.stringify(inner), id: 'lc_1' }]);

  it('unwraps content blocks and parses their JSON text', () => {
    const r = parseDirectToolResult(envelope);
    expect(r.kind).toBe('blocks');
    if (r.kind !== 'blocks') return;
    expect(r.blocks).toHaveLength(1);
    expect(r.blocks[0].json).toEqual(inner);
  });

  it('keeps a text block that is not JSON as text', () => {
    const r = parseDirectToolResult(JSON.stringify([{ type: 'text', text: 'plain words' }]));
    expect(r.kind).toBe('blocks');
    if (r.kind !== 'blocks') return;
    expect(r.blocks[0].json).toBeUndefined();
    expect(r.blocks[0].text).toBe('plain words');
  });

  it('classifies a refusal with its reason', () => {
    expect(parseDirectToolResult('Refused: trading is not granted on this connection')).toEqual({
      kind: 'refused',
      reason: 'trading is not granted on this connection',
    });
  });

  it('classifies a user rejection and recovers the feedback', () => {
    const r = parseDirectToolResult(
      'User rejected the tool call for `mcp__moomoo__trading_order_place` with reason: User rejected this action with the following feedback: wrong account',
    );
    expect(r.kind).toBe('rejected');
    if (r.kind !== 'rejected') return;
    expect(directToolRejectionReason(r.reason)).toBe('wrong account');
  });

  it('does not read a succeeding call as a refusal', () => {
    // The words are the vendor's own, on a call the backend stamped success.
    // Answering a broker query with the connector's refusal notice is the one
    // misread that matters on this surface.
    expect(parseDirectToolResult('Refused: 7 applications', false)).toEqual({
      kind: 'text',
      text: 'Refused: 7 applications',
    });
    expect(
      parseDirectToolResult(
        'User rejected the tool call for `x` with reason: nope',
        false,
      ),
    ).toEqual({
      kind: 'text',
      text: 'User rejected the tool call for `x` with reason: nope',
    });
  });

  it('still classifies a refusal on a call that did fail', () => {
    expect(parseDirectToolResult('Refused: trading is not granted', true)).toEqual({
      kind: 'refused',
      reason: 'trading is not granted',
    });
  });

  it('keeps a record array whose rows carry a type of their own', () => {
    // An options chain is the shape that used to be read as content blocks and
    // stripped to a row of empty fences.
    const chain = [
      { type: 'call', strike: 190, bid: 2.15 },
      { type: 'put', strike: 190, bid: 1.9 },
    ];
    expect(parseDirectToolResult(JSON.stringify(chain))).toEqual({
      kind: 'json',
      json: chain,
    });
  });

  it('renders a block with no text of its own as the block itself', () => {
    const image = { type: 'image', base64: 'AAAA', mime_type: 'image/png' };
    expect(parseDirectToolResult([image])).toEqual({
      kind: 'blocks',
      blocks: [{ type: 'image', text: '', json: image }],
    });
  });

  it('falls back to raw JSON and raw text', () => {
    expect(parseDirectToolResult('{"a":1}')).toEqual({ kind: 'json', json: { a: 1 } });
    expect(parseDirectToolResult('not json')).toEqual({ kind: 'text', text: 'not json' });
    expect(parseDirectToolResult('')).toEqual({ kind: 'empty' });
    expect(parseDirectToolResult(null)).toEqual({ kind: 'empty' });
  });
});

describe('directToolIdentity', () => {
  it('prefers the stamp over the name it cannot parse back', () => {
    expect(
      directToolIdentity('mcp__desk_prod_3a2b092c__place', {
        direct_mcp: { server: 'desk__prod', tool: 'place' },
      }),
    ).toEqual({ server: 'desk__prod', tool: 'place' });
  });

  it('falls back to the name while a call is still in flight', () => {
    expect(directToolIdentity('mcp__moomoo__quote', undefined)).toEqual({
      server: 'moomoo',
      tool: 'quote',
    });
  });

  it('ignores a malformed stamp rather than rendering half of one', () => {
    expect(
      directToolIdentity('mcp__moomoo__quote', { direct_mcp: { server: 'moomoo' } }),
    ).toEqual({ server: 'moomoo', tool: 'quote' });
  });

  it('is null for a tool that is not a direct one', () => {
    expect(directToolIdentity('WebSearch', undefined)).toBeNull();
  });
});

describe('directToolDisplayName with a completed result', () => {
  it('titles from the stamp rather than the digest in the name', () => {
    expect(
      directToolDisplayName('mcp__desk_prod_3a2b092c__place_order', {
        direct_mcp: { server: 'desk__prod', tool: 'place_order' },
      }),
    ).toBe('Place order');
  });

  it('still titles from the name while the call is in flight', () => {
    expect(directToolDisplayName('mcp__moomoo__trading_order_place')).toBe(
      'Trading order place',
    );
  });
});
