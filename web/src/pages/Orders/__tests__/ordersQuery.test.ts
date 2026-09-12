import { describe, expect, it } from 'vitest';
import { ordersQueryParams } from '@/pages/ChatAgent/utils/api/orders';
import { parseOrderDetail, withOrderDetail } from '../utils/detailParam';

/**
 * The filter set is also the list's cache key, so "no filter" and "an empty
 * filter" have to produce the same object: otherwise clearing a select would
 * start a second, identical list beside the one already loaded.
 */
describe('ordersQueryParams', () => {
  it('sends only the facets that were chosen', () => {
    expect(
      ordersQueryParams({
        vendor: 'moomoo',
        mode: 'paper',
        status: 'filled',
        asset_class: 'equity',
      }),
    ).toEqual({
      vendor: 'moomoo',
      mode: 'paper',
      status: ['filled'],
      asset_class: 'equity',
    });
  });

  it('expands a status group into every ledger status it covers', () => {
    expect(ordersQueryParams({ status: 'rejected' }).status).toEqual([
      'rejected_by_user',
      'refused',
      'rejected_by_vendor',
    ]);
    // Unknown is open: the server still reconciles it, so a surface keeps polling.
    expect(ordersQueryParams({ status: 'open' }).status).toContain('unknown');
    expect(ordersQueryParams({ status: 'failed' }).status).toEqual(['failed']);
  });

  it('drops an unset facet rather than sending it empty', () => {
    expect(ordersQueryParams({ vendor: 'moomoo', mode: null })).toEqual({
      vendor: 'moomoo',
    });
  });

  it('reads an empty string as no filter at all', () => {
    expect(ordersQueryParams({ vendor: '', status: undefined })).toEqual({});
    expect(ordersQueryParams({})).toEqual({});
  });
});

describe('the ?detail=order: param', () => {
  it('reads an attempt id out of the detail param', () => {
    const params = new URLSearchParams('vendor=moomoo&detail=order:abc-123');
    expect(parseOrderDetail(params)).toBe('abc-123');
  });

  it('ignores another surface\'s detail kind', () => {
    expect(parseOrderDetail(new URLSearchParams('detail=plugin:moomoo'))).toBeNull();
    expect(parseOrderDetail(new URLSearchParams('detail=order:'))).toBeNull();
    expect(parseOrderDetail(new URLSearchParams(''))).toBeNull();
  });

  it('sets and clears the param without touching the filters', () => {
    const params = new URLSearchParams('vendor=moomoo&mode=paper');
    const opened = withOrderDetail(params, 'abc-123');
    expect(opened.get('detail')).toBe('order:abc-123');
    expect(opened.get('vendor')).toBe('moomoo');
    const closed = withOrderDetail(opened, null);
    expect(closed.get('detail')).toBeNull();
    expect(closed.get('mode')).toBe('paper');
  });
});
