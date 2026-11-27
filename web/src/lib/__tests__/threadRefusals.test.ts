import { afterEach, describe, expect, it, vi } from 'vitest';
import { noteThreadRefused, noteThreadResponseRefused, onThreadRefused } from '../threadRefusals';
import { accountSuspendedNotice } from '../accountSuspension';

const mockReportEdgeBlock = vi.fn();
vi.mock('../sentry', () => ({ reportEdgeBlock: (...args: unknown[]) => mockReportEdgeBlock(...args) }));

const sent = () => true;
const page = () => new Response('<html></html>', { status: 403, headers: { 'content-type': 'text/html', 'cf-mitigated': 'challenge' } });

const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });

describe('a thread stream refused on reconnect', () => {
  let off: () => void = () => {};
  afterEach(() => off());

  it('reports a 403 or 404 for the thread, and nothing else', async () => {
    const seen = vi.fn();
    off = onThreadRefused(seen);
    await noteThreadResponseRefused('t-1', json(403, { detail: 'Forbidden' }), sent);
    await noteThreadResponseRefused('t-2', json(404, { detail: 'Thread not found' }), sent);
    await noteThreadResponseRefused('t-3', json(500, { detail: 'boom' }), sent);
    await noteThreadResponseRefused('t-4', new Response('not json', { status: 403 }), sent);
    noteThreadRefused('t-5', 403, 'Forbidden');
    noteThreadRefused('t-6', undefined, null);
    expect(seen.mock.calls.map(([id]) => id)).toEqual(['t-1', 't-2', 't-4', 't-5']);
  });

  it("leaves a suspended account's refusal to the app-wide notice", async () => {
    const seen = vi.fn();
    off = onThreadRefused(seen);
    await noteThreadResponseRefused('t-1', json(403, { detail: { type: 'account_suspended', organization: 'Northfield Capital' } }), sent);
    expect(seen).not.toHaveBeenCalled();
    expect(accountSuspendedNotice()).not.toBeNull();
  });

  it('reads a firewall page as no refusal of the thread, reported for the account that sent it', async () => {
    const seen = vi.fn();
    off = onThreadRefused(seen);
    mockReportEdgeBlock.mockReset();
    await noteThreadResponseRefused('t-1', page(), sent);
    await noteThreadResponseRefused('t-2', page(), () => false);
    expect(seen).not.toHaveBeenCalled();
    expect(mockReportEdgeBlock).toHaveBeenCalledTimes(1);
    expect(mockReportEdgeBlock.mock.calls[0][0]).toBe('challenge');
  });
});
