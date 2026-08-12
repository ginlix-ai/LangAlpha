import { describe, it, expect } from 'vitest';
import {
  parseMcpServersJson,
  normalizeMcpServers,
  coerceMcpName,
  normalizeTransport,
} from '../mcpImport';

describe('coerceMcpName', () => {
  it('underscores illegal characters and flags the rename', () => {
    expect(coerceMcpName('my-stock-mcp.v2')).toEqual({ name: 'my_stock_mcp_v2', renamed: true });
  });
  it('prefixes a leading digit', () => {
    expect(coerceMcpName('3rd-party')).toEqual({ name: '_3rd_party', renamed: true });
  });
  it('passes through an already-legal name', () => {
    expect(coerceMcpName('already_ok')).toEqual({ name: 'already_ok', renamed: false });
  });
});

describe('normalizeTransport', () => {
  it.each([
    ['streamablehttp', 'http'],
    ['streamable-http', 'http'],
    ['streamable_http', 'http'],
    ['streamableHttp', 'http'],
    ['http', 'http'],
    ['sse', 'sse'],
    ['stdio', 'stdio'],
  ])('maps %s -> %s', (raw, expected) => {
    expect(normalizeTransport(raw, false, true)).toBe(expected);
  });
  it('infers from fields when type is absent', () => {
    expect(normalizeTransport(null, true, false)).toBe('stdio');
    expect(normalizeTransport(null, false, true)).toBe('http');
    expect(normalizeTransport(null, false, false)).toBeNull();
    expect(normalizeTransport('nonsense', false, true)).toBeNull();
  });
});

describe('parseMcpServersJson', () => {
  it('unwraps mcpServers and maps a remote server', () => {
    const text = JSON.stringify({
      mcpServers: {
        'my-stock-mcp': {
          type: 'streamablehttp',
          url: 'https://api.example.com/ds/stock',
          headers: { Authorization: 'EXAMPLE-OPAQUE-TOKEN' },
        },
      },
    });
    const { servers, error } = parseMcpServersJson(text);
    expect(error).toBeUndefined();
    expect(servers).toHaveLength(1);
    const s = servers[0];
    expect(s.originalName).toBe('my-stock-mcp');
    expect(s.name).toBe('my_stock_mcp');
    expect(s.renamed).toBe(true);
    expect(s.transport).toBe('http');
    expect(s.url).toBe('https://api.example.com/ds/stock');
    // Literal secret is filled inline for the user to vault via the picker.
    expect(s.headers).toEqual({ Authorization: 'EXAMPLE-OPAQUE-TOKEN' });
  });

  it('infers stdio from command and drops unknown keys', () => {
    const { servers } = normalizeMcpServers({
      mcpServers: { local_time: { command: 'uvx', args: ['pkg'], disabled: true } },
    });
    expect(servers[0].transport).toBe('stdio');
    expect(servers[0].command).toBe('uvx');
    expect(servers[0].args).toEqual(['pkg']);
  });

  it('handles a bare {name: def} map', () => {
    const { servers } = normalizeMcpServers({
      srv_a: { command: 'npx' },
      srv_b: { url: 'https://api.example.com/m' },
    });
    const byName = Object.fromEntries(servers.map((s) => [s.name, s]));
    expect(byName.srv_a.transport).toBe('stdio');
    expect(byName.srv_b.transport).toBe('http');
  });

  it('marks an entry with no determinable transport as an error', () => {
    const { servers } = normalizeMcpServers({ mcpServers: { weird: { foo: 'bar' } } });
    expect(servers[0].error).toBeTruthy();
  });

  it('reports invalid JSON', () => {
    expect(parseMcpServersJson('{ not json').error).toBe('Not valid JSON.');
  });

  it('reports an empty config', () => {
    expect(normalizeMcpServers({ mcpServers: {} }).error).toBeTruthy();
  });
});
