import { describe, it, expect } from 'vitest';
import {
  argsChanged,
  discoverySecretsForced,
  draftArgv,
  draftBlankKeyPath,
  draftDuplicateKeyPath,
  draftHeaderMap,
  draftPayload,
  draftSuggestedName,
  draftTransport,
  entryChanged,
  envChanged,
  headersChanged,
  initialDraft,
  probeTarget,
  transportPinned,
  type Draft,
  type DraftMeta,
} from '../mcpServerDraft';
import { newHeaderRow, nextRowId, type HeaderRow } from '../McpKeyValueEditor';

const META: DraftMeta = {
  name: 'srv',
  description: '',
  instruction: '',
  exposure: 'summary',
  discoveryUsesSecrets: false,
};

const type = (text: string, from: Draft = initialDraft()) => entryChanged(from, text);

const bearer = (inner: string): HeaderRow => ({
  id: nextRowId(),
  choice: 'bearer',
  customName: '',
  inner,
});

describe('mcpServerDraft: the field decides what the server is', () => {
  it('starts on nothing and names no transport', () => {
    const draft = initialDraft();
    expect(draft.kind).toBe('empty');
    expect(draftTransport(draft)).toBeNull();
    expect(draftPayload(draft, META)).toBeNull();
  });

  it('reads an address as a remote server and a command line as a local one', () => {
    expect(draftTransport(type('https://mcp.example.com/mcp'))).toBe('http');
    const local = type('npx -y @scope/thing --flag "two words"');
    expect(draftTransport(local)).toBe('stdio');
    expect(draftArgv(local)).toEqual({
      command: 'npx',
      args: ['-y', '@scope/thing', '--flag', 'two words'],
    });
  });

  it('keeps a credential row while the address is cleared and retyped', () => {
    const withHeader = headersChanged(type('https://mcp.example.com/mcp'), [bearer('k')]);
    const cleared = entryChanged(withHeader, '');
    expect(cleared.kind).toBe('empty');
    const retyped = entryChanged(cleared, 'https://mcp.example.com/v2');
    expect(draftHeaderMap(retyped)).toEqual({ Authorization: 'Bearer k' });
  });

  it('keeps a credential row through the half-typed address on the way back', () => {
    // Select-all and retype: 'h' is a command line, so the draft is a local
    // server for a keystroke, and dropping the rows there lost the credential
    // by the time the address was whole again.
    const withHeader = headersChanged(type('https://mcp.example.com/mcp'), [bearer('k')]);
    const halfTyped = entryChanged(withHeader, 'h');
    expect(halfTyped.kind).toBe('stdio');
    const whole = entryChanged(halfTyped, 'https://api.example.com/mcp');
    expect(draftHeaderMap(whole)).toEqual({ Authorization: 'Bearer k' });
  });

  it('keeps an environment row through an address typed over the command', () => {
    const withEnv = envChanged(type('npx -y @scope/thing'), [
      { id: nextRowId(), key: 'API_TOKEN', value: 'k' },
    ]);
    const asAddress = entryChanged(withEnv, 'https://api.example.com/mcp');
    expect(asAddress.kind).toBe('remote');
    const back = entryChanged(asAddress, 'npx -y @scope/other');
    expect(draftPayload(back, META)).toMatchObject({ env: { API_TOKEN: 'k' } });
  });
});

describe('mcpServerDraft: pasted configs', () => {
  const config = JSON.stringify({
    mcpServers: {
      'fuyao-meta': {
        type: 'http',
        url: 'https://fuyao.example.com/mcp/meta',
        headers: { 'X-api-key': '${vault:FUYAO}' },
      },
    },
  });

  it('fills the whole draft and shows the line the config amounts to', () => {
    const draft = type(config);
    expect(draft.kind).toBe('remote');
    expect(draft.entry).toBe('https://fuyao.example.com/mcp/meta');
    expect(draftHeaderMap(draft)).toEqual({ 'X-api-key': '${vault:FUYAO}' });
    expect(draftSuggestedName(draft)).toBe('fuyao_meta');
    // A header that resolves from the vault forces discovery to resolve it too.
    expect(discoverySecretsForced(draft)).toBe(true);
  });

  it('keeps nothing submittable when a malformed config lands on a valid one', () => {
    const valid = type(config);
    expect(draftPayload(valid, META)).not.toBeNull();

    // The reproduced bug: the field showed the broken text while the payload
    // still held the config before it, and submit sent that.
    const broken = entryChanged(valid, '{"mcpServers": {');
    expect(broken.kind).toBe('invalid');
    expect(broken.entry).toBe('{"mcpServers": {');
    expect(draftPayload(broken, META)).toBeNull();
    expect(draftTransport(broken)).toBeNull();
    expect(probeTarget(broken)).toBeNull();
    expect(draftHeaderMap(broken)).toEqual({});
  });

  it('recovers as soon as the field says something again', () => {
    const broken = entryChanged(type(config), '{ not json');
    const fixed = entryChanged(broken, 'https://mcp.example.com/mcp');
    expect(fixed.kind).toBe('remote');
    expect(draftPayload(fixed, META)).toMatchObject({
      transport: 'http',
      url: 'https://mcp.example.com/mcp',
    });
  });

  it('brings the credential rows back with it', () => {
    // A stray brace typed into the field is a malformed config, and the rows
    // the user filled in by hand are not recoverable from anywhere else.
    const withHeader = headersChanged(type('https://mcp.example.com/mcp'), [bearer('k')]);
    const broken = entryChanged(withHeader, '{"mcpServers": {');
    expect(broken.kind).toBe('invalid');
    // Held, not sendable: an invalid shape still has no payload and no address.
    expect(draftPayload(broken, META)).toBeNull();
    expect(draftHeaderMap(broken)).toEqual({});

    const fixed = entryChanged(broken, 'https://mcp.example.com/v2');
    expect(draftHeaderMap(fixed)).toEqual({ Authorization: 'Bearer k' });
  });

  it("brings a local server's environment rows back the same way", () => {
    const withEnv = envChanged(type('npx -y @scope/thing'), [
      { id: 'e1', key: 'API_TOKEN', value: 'k' },
    ]);
    const broken = entryChanged(withEnv, '{ not json');
    const fixed = entryChanged(broken, 'npx -y @scope/other');
    expect(draftPayload(fixed, META)).toMatchObject({ env: { API_TOKEN: 'k' } });
  });
});

describe('mcpServerDraft: the transport pin', () => {
  it('holds sse against an address that would read as http', () => {
    const pinned = transportPinned(type('https://mcp.example.com/sse'), 'sse');
    expect(draftTransport(pinned)).toBe('sse');
    const edited = entryChanged(pinned, 'https://mcp.example.com/sse/v2');
    expect(draftTransport(edited)).toBe('sse');
    expect(draftPayload(edited, META)).toMatchObject({ transport: 'sse' });
  });

  it('does not hold http, which is what any address reads as anyway', () => {
    const pinned = transportPinned(type('npx -y thing'), 'http');
    expect(draftTransport(pinned)).toBe('http');
    expect(draftTransport(entryChanged(pinned, 'npx -y thing again'))).toBe('stdio');
  });

  it('carries the line across a kind change, and sends only the rows that kind can', () => {
    const remote = headersChanged(type('https://mcp.example.com/mcp'), [bearer('k')]);
    const local = transportPinned(remote, 'stdio');
    expect(local.kind).toBe('stdio');
    expect(local.entry).toBe('https://mcp.example.com/mcp');
    // A stdio payload cannot smuggle headers, whatever the remote shape held.
    expect(draftPayload(local, META)).toMatchObject({ transport: 'stdio', env: {} });
    expect(draftPayload(local, META)).not.toHaveProperty('headers');
  });

  it('sees an edited argument list as a rewrite of the field', () => {
    const local = argsChanged(type('npx -y thing'), ['-y', 'thing', '--port', '80']);
    expect(local.entry).toBe('npx -y thing --port 80');
    expect(draftArgv(local).args).toEqual(['-y', 'thing', '--port', '80']);
  });

  it('keeps an argument the user has not filled in yet', () => {
    // The Add-argument button appends '', and the field is where args live: a
    // line that dropped it put the row back on screen already deleted.
    const added = argsChanged(type('npx -y thing'), ['-y', 'thing', '']);
    expect(added.entry).toBe("npx -y thing ''");
    expect(draftArgv(added).args).toEqual(['-y', 'thing', '']);
    // And clearing a filled row empties it rather than removing it.
    const cleared = argsChanged(added, ['-y', '', '']);
    expect(draftArgv(cleared).args).toEqual(['-y', '', '']);
  });
});

describe('mcpServerDraft: header rows', () => {
  it('sends nothing for a scheme chosen but not filled, and keeps the choice', () => {
    const empty: HeaderRow = { ...newHeaderRow(), choice: 'basic', inner: '' };
    const draft = headersChanged(type('https://mcp.example.com/mcp'), [empty]);
    // The pair `{Authorization: ''}` could not say which scheme was chosen, so
    // an unfilled Basic row used to re-render as Bearer.
    expect(draftHeaderMap(draft)).toEqual({});
    expect((draft as { headers: HeaderRow[] }).headers[0].choice).toBe('basic');

    const filled = headersChanged(draft, [{ ...empty, inner: 'dXNlcjpwdw==' }]);
    expect(draftHeaderMap(filled)).toEqual({ Authorization: 'Basic dXNlcjpwdw==' });
  });

  it('names the row that save would otherwise drop', () => {
    const blank: HeaderRow = { ...newHeaderRow(), choice: 'custom', customName: '', inner: 'k' };
    expect(draftBlankKeyPath(headersChanged(type('https://a.example.com/mcp'), [blank]))).toBe(
      'headers',
    );
    const env = envChanged(type('npx thing'), [{ id: nextRowId(), key: '', value: 'v' }]);
    expect(draftBlankKeyPath(env)).toBe('env');
    expect(draftBlankKeyPath(type('npx thing'))).toBeNull();
  });

  it('names a repeated header, folding case the way the wire does', () => {
    // Two Authorization rows fold to one entry before validation runs, so the
    // second scheme used to replace the first without a word.
    const both = headersChanged(type('https://a.example.com/mcp'), [
      { ...newHeaderRow(), choice: 'bearer', inner: 'EXAMPLE-TOKEN' },
      { ...newHeaderRow(), choice: 'basic', inner: 'EXAMPLE-BASIC' },
    ]);
    expect(draftDuplicateKeyPath(both)).toBe('headers');

    const spelledApart = headersChanged(both, [
      { ...newHeaderRow(), choice: 'bearer', inner: 'EXAMPLE-TOKEN' },
      { ...newHeaderRow(), choice: 'custom', customName: 'authorization', inner: 'EXAMPLE-BASIC' },
    ]);
    expect(draftDuplicateKeyPath(spelledApart)).toBe('headers');

    const distinct = headersChanged(both, [
      { ...newHeaderRow(), choice: 'bearer', inner: 'EXAMPLE-TOKEN' },
      { ...newHeaderRow(), choice: 'x-api-key', inner: 'EXAMPLE-KEY' },
    ]);
    expect(draftDuplicateKeyPath(distinct)).toBeNull();
  });

  it('names a repeated env key, and keeps case apart the way the shell does', () => {
    const repeated = envChanged(type('npx thing'), [
      { id: nextRowId(), key: 'API_TOKEN', value: 'EXAMPLE-TOKEN' },
      { id: nextRowId(), key: ' API_TOKEN ', value: 'EXAMPLE-OTHER' },
    ]);
    expect(draftDuplicateKeyPath(repeated)).toBe('env');

    const cased = envChanged(repeated, [
      { id: nextRowId(), key: 'API_TOKEN', value: 'EXAMPLE-TOKEN' },
      { id: nextRowId(), key: 'api_token', value: 'EXAMPLE-OTHER' },
    ]);
    expect(draftDuplicateKeyPath(cased)).toBeNull();
  });

  it('does not call a row nobody has filled in a duplicate', () => {
    // "Add entry" seeds a second Authorization row with no value, and
    // `headersToMap` never sends it, so the form must not refuse Save yet.
    const unfilled = headersChanged(type('https://a.example.com/mcp'), [
      { ...newHeaderRow(), choice: 'bearer', inner: 'EXAMPLE-TOKEN' },
      newHeaderRow(),
    ]);
    expect(draftDuplicateKeyPath(unfilled)).toBeNull();

    const unnamed = envChanged(type('npx thing'), [
      { id: nextRowId(), key: 'API_TOKEN', value: 'EXAMPLE-TOKEN' },
      { id: nextRowId(), key: '', value: '' },
    ]);
    expect(draftDuplicateKeyPath(unnamed)).toBeNull();
    expect(draftDuplicateKeyPath(type('npx thing'))).toBeNull();
  });
});

describe('mcpServerDraft: what gets checked and what gets sent', () => {
  it('checks a remote address with its headers, and never a local command', () => {
    const remote = headersChanged(type('https://mcp.example.com/mcp'), [bearer('k')]);
    expect(probeTarget(remote)).toEqual({
      url: 'https://mcp.example.com/mcp',
      headers: { Authorization: 'Bearer k' },
    });
    expect(probeTarget(type('npx -y thing'))).toBeNull();
    // An address the URL policy already refuses is not worth a round trip.
    expect(probeTarget(type('https://169.254.169.254/'))).toBeNull();
  });

  it('hydrates an edited server from its stored maps', () => {
    const draft = initialDraft({
      name: 'srv',
      transport: 'http',
      command: null,
      args: [],
      url: 'https://mcp.example.com/mcp',
      env_refs: [],
      header_refs: ['AUTH'],
      headers: { Authorization: 'Bearer ${vault:AUTH}' },
      description: '',
      instruction: '',
      tool_exposure_mode: 'summary',
    });
    expect(draftHeaderMap(draft)).toEqual({ Authorization: 'Bearer ${vault:AUTH}' });
    expect(draftPayload(draft, { ...META, name: 'srv' })).toMatchObject({
      name: 'srv',
      transport: 'http',
      // Forced on: a header that resolves from the vault is needed to list tools.
      discovery_uses_secrets: true,
    });
  });
});
