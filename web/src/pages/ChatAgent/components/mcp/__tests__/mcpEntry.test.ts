import { describe, it, expect } from 'vitest';
import { commandLine, parseEntry, shellSplit, suggestName } from '../mcpEntry';

describe('parseEntry', () => {
  it('reads a URL as remote http', () => {
    expect(parseEntry('  https://mcp.linear.app/mcp ')).toEqual({
      kind: 'remote',
      transport: 'http',
      url: 'https://mcp.linear.app/mcp',
    });
  });

  it('reads anything else as a command line, honoring quotes', () => {
    expect(parseEntry('npx -y @scope/pkg --path "/tmp/my dir"')).toEqual({
      kind: 'command',
      command: 'npx',
      args: ['-y', '@scope/pkg', '--path', '/tmp/my dir'],
    });
  });

  it('reads a pasted config as its first good server and counts the rest', () => {
    const res = parseEntry(
      JSON.stringify({
        mcpServers: {
          a: { type: 'http', url: 'https://a.example.com/mcp' },
          b: { command: 'uvx', args: ['b'] },
        },
      }),
    );
    expect(res.kind).toBe('json');
    if (res.kind !== 'json') throw new Error('unreachable');
    expect(res.server.name).toBe('a');
    expect(res.more).toBe(1);
  });

  it('reports a config it cannot use instead of treating the JSON as a command', () => {
    expect(parseEntry('{"nope": 1}').kind).toBe('json-error');
    expect(parseEntry('{not json').kind).toBe('json-error');
  });

  it('refuses a command line whose quote never closed', () => {
    // A lenient split hands back `abc`, which is not what the field spells, and
    // saving that difference launches the server with an argument nobody typed.
    const res = parseEntry('npx pkg --token "abc');
    expect(res.kind).toBe('command-error');
    if (res.kind !== 'command-error') throw new Error('unreachable');
    expect(res.error).toMatch(/unterminated quote/i);
  });

  it('reads a command line whose quotes close', () => {
    expect(parseEntry('npx pkg --token "abc"')).toEqual({
      kind: 'command',
      command: 'npx',
      args: ['pkg', '--token', 'abc'],
    });
  });

  it('round-trips a command line through shellSplit and commandLine', () => {
    const line = "python3 server.py --name 'two words'";
    const [command, ...args] = shellSplit(line);
    expect(commandLine(command, args)).toBe(line);
  });

  it('keeps an empty argument at its position through the round trip', () => {
    // A blank row is an argument the user has not filled in yet, and a flag
    // whose stored value is empty is still that flag's value.
    expect(commandLine('uvx', ['--prefix', ''])).toBe("uvx --prefix ''");
    expect(shellSplit("uvx --prefix ''")).toEqual(['uvx', '--prefix', '']);
    expect(commandLine('', ['--flag'])).toBe('--flag');
  });

});

describe('suggestName', () => {
  it('names a remote server from the service label and path, not the protocol words', () => {
    expect(suggestName('http', 'https://mcp.linear.app/mcp', '', [])).toBe('linear');
    expect(suggestName('http', 'https://fuyao.aicubes.cn/mcp/a-share', '', [])).toBe('fuyao_a_share');
    expect(suggestName('http', 'https://api.example.com/v1/sse', '', [])).toBe('example');
  });

  it('names a local server from its package, affixes stripped', () => {
    expect(suggestName('stdio', '', 'npx', ['-y', '@modelcontextprotocol/server-filesystem', '/tmp'])).toBe('filesystem');
    expect(suggestName('stdio', '', 'uvx', ['mcp-server-fetch'])).toBe('fetch');
    expect(suggestName('stdio', '', 'node', ['./build/index.js'])).toBe('index');
  });

  it('suggests nothing until there is something to name', () => {
    expect(suggestName(null, '', '', [])).toBe('');
  });

  it('names an address whose path is not valid percent-encoding', () => {
    // `new URL` accepts a lone `%`, so typing `%20` one key at a time passes
    // through here; the segment that would not decode is kept as it was typed.
    expect(suggestName('http', 'https://example.com/a%', '', [])).toBe('example_a_');
    expect(suggestName('http', 'https://example.com/a%20b', '', [])).toBe('example_a_b');
  });
});
