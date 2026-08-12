import { describe, it, expect } from 'vitest';
import {
  validateMcpServer,
  validateRemoteUrl,
  validateArg,
  isValidSecretValue,
  collectVaultRefs,
  ALLOWED_COMMANDS,
  DESCRIPTION_MAX,
  INSTRUCTION_MAX,
} from '../mcpSchemas';

// Mirror of the backend validator test matrix in
// src/server/models/mcp_server.py (allowlist, URL policy incl.
// 169.254.169.254, vault-ref vs bare ${VAR}, length caps). Neutral
// placeholder names throughout.

function stdio(overrides: Record<string, unknown> = {}) {
  return { name: 'my_server', transport: 'stdio', command: 'npx', args: [], env: {}, ...overrides };
}
function http(overrides: Record<string, unknown> = {}) {
  return { name: 'remote_server', transport: 'http', url: 'https://example.com/mcp', headers: {}, ...overrides };
}

describe('mcpSchemas — name shape', () => {
  it('accepts a valid identifier name', () => {
    expect(validateMcpServer(stdio({ name: 'valid_name_1' })).ok).toBe(true);
  });

  it.each([
    ['1leading_digit', '1bad'],
    ['hyphen', 'has-hyphen'],
    ['dot', 'has.dot'],
    ['empty', ''],
    ['too long', 'a'.repeat(65)],
  ])('rejects invalid name (%s)', (_label, name) => {
    expect(validateMcpServer(stdio({ name })).ok).toBe(false);
  });
});

describe('mcpSchemas — command allowlist (no bash)', () => {
  it.each(ALLOWED_COMMANDS)('accepts allowed command %s', (command) => {
    expect(validateMcpServer(stdio({ command })).ok).toBe(true);
  });

  it.each(['bash', 'sh', 'zsh', 'curl', 'rm', '/bin/bash'])(
    'rejects disallowed command %s',
    (command) => {
      expect(validateMcpServer(stdio({ command })).ok).toBe(false);
    },
  );
});

describe('mcpSchemas — URL policy', () => {
  it('accepts a plain https url', () => {
    expect(validateRemoteUrl('https://api.example.com/mcp')).toBeNull();
    expect(validateMcpServer(http({ url: 'https://api.example.com/mcp' })).ok).toBe(true);
  });

  it.each([
    ['OData batch path', 'https://api.example.com/$batch'],
    ['OData query options', "https://api.example.com/odata?$filter=status%20eq%20'active'&$top=100"],
  ])('accepts bare $$word URL conventions — %s', (_label, url) => {
    expect(validateRemoteUrl(url)).toBeNull();
  });

  it.each([
    ['brace env placeholder', 'https://api.example.com/${VAR}/mcp'],
    ['unclosed brace form', 'https://api.example.com/${vault:TOK'],
  ])('rejects brace placeholder forms — %s', (_label, url) => {
    expect(validateRemoteUrl(url)).not.toBeNull();
  });

  it.each([
    ['http scheme', 'http://example.com'],
    ['userinfo creds', 'https://user:pass@example.com'],
    ['localhost', 'https://localhost/mcp'],
    ['*.local', 'https://printer.local/mcp'],
    ['*.internal', 'https://svc.internal/mcp'],
    ['*.localhost', 'https://app.localhost/mcp'],
    ['loopback ip', 'https://127.0.0.1/mcp'],
    ['private 10.x', 'https://10.0.0.5/mcp'],
    ['private 192.168', 'https://192.168.1.1/mcp'],
    ['private 172.16', 'https://172.16.0.1/mcp'],
    ['link-local metadata', 'https://169.254.169.254/latest/meta-data'],
    ['unspecified', 'https://0.0.0.0/mcp'],
    ['decimal-int loopback', 'https://2130706433/mcp'],
    ['hex loopback', 'https://0x7f000001/mcp'],
    ['octal-octet loopback', 'https://0177.0.0.1/mcp'],
    ['short-dotted loopback', 'https://127.1/mcp'],
    ['decimal-int metadata', 'https://2852039166/mcp'],
    ['ipv6 loopback', 'https://[::1]/mcp'],
    ['vault smuggle', 'https://example.com/${vault:TOKEN}'],
  ])('rejects %s', (_label, url) => {
    expect(validateRemoteUrl(url)).not.toBeNull();
    expect(validateMcpServer(http({ url })).ok).toBe(false);
  });

  it('rejects the GCP/AWS metadata IP specifically (169.254.169.254)', () => {
    expect(validateRemoteUrl('https://169.254.169.254/')).toMatch(/disallowed IP/);
  });
});

describe('mcpSchemas — secret value policy (vault-ref vs bare $VAR)', () => {
  it('accepts a full ${vault:NAME} reference', () => {
    expect(isValidSecretValue('${vault:MY_TOKEN}')).toBe(true);
  });

  it('accepts a clean literal', () => {
    expect(isValidSecretValue('plain-literal-123')).toBe(true);
    expect(isValidSecretValue('')).toBe(true);
  });

  it.each([
    ['bare braced env', '${MY_TOKEN}'],
    ['bare dollar env', '$MY_TOKEN'],
    ['embedded bare env', 'prefix-${SECRET}-suffix'],
    ['malformed vault ref', '${vault:bad'],
    ['partial vault ref text', 'use ${vault:X} here'],
  ])('rejects %s', (_label, value) => {
    expect(isValidSecretValue(value)).toBe(false);
  });

  it('rejects an env map with a bare host-env value', () => {
    expect(validateMcpServer(stdio({ env: { API_KEY: '${HOST_VAR}' } })).ok).toBe(false);
  });

  it('accepts an env map with a vault ref', () => {
    expect(validateMcpServer(stdio({ env: { API_KEY: '${vault:API_KEY}' } })).ok).toBe(true);
  });

  it('rejects an invalid env key name', () => {
    expect(validateMcpServer(stdio({ env: { '1bad key': 'literal' } })).ok).toBe(false);
  });
});

describe('mcpSchemas — stdio args policy (vault refs vs host-env placeholders)', () => {
  // Embedded `${vault:NAME}` refs are legal in args (bulk import writes
  // `--flag=${vault:NAME}`); a malformed vault ref or a bare host-env
  // placeholder is rejected — mirroring env/header value policy.
  it.each([
    ['embedded vault ref', '--flag=${vault:TOKEN}'],
    ['standalone vault ref', '${vault:TOKEN}'],
    ['plain literal flag', '--verbose'],
    ['plain literal value', 'package-name'],
    ['empty string', ''],
    ['non-identifier dollar ($100)', '$100'],
    ['bare dollar before digit', '--cost=$50'],
  ])('accepts %s', (_label, arg) => {
    expect(validateArg(arg)).toBeNull();
    expect(validateMcpServer(stdio({ args: [arg] })).ok).toBe(true);
  });

  it.each([
    ['braced host-env', '${HOME}'],
    ['dollar host-env', '$HOME'],
    ['embedded host-env', '--dir=${HOME}/x'],
  ])('rejects host-env placeholder %s', (_label, arg) => {
    expect(validateArg(arg)).toMatch(/host-env placeholder/);
    expect(validateMcpServer(stdio({ args: [arg] })).ok).toBe(false);
  });

  it.each([
    ['unterminated vault ref', '--x=${vault:bad'],
    ['bare malformed vault', '${vault:'],
  ])('rejects malformed vault ref %s', (_label, arg) => {
    expect(validateArg(arg)).toMatch(/malformed vault reference/);
    expect(validateMcpServer(stdio({ args: [arg] })).ok).toBe(false);
  });

  it('validates every arg in the list (rejects when any one is bad)', () => {
    expect(validateMcpServer(stdio({ args: ['-y', 'pkg', '--token=${vault:T}'] })).ok).toBe(true);
    expect(validateMcpServer(stdio({ args: ['-y', '${HOME}'] })).ok).toBe(false);
  });
});

describe('mcpSchemas — transport/field coherence', () => {
  it('rejects stdio without a command', () => {
    expect(validateMcpServer({ name: 'x', transport: 'stdio', args: [], env: {} }).ok).toBe(false);
  });

  it('rejects http with an invalid url', () => {
    expect(validateMcpServer(http({ url: 'not-a-url' })).ok).toBe(false);
  });

  it('accepts sse with a valid https url', () => {
    expect(validateMcpServer({ name: 'sse_server', transport: 'sse', url: 'https://example.com/sse', headers: {} }).ok).toBe(true);
  });
});

describe('mcpSchemas — length caps', () => {
  it('accepts description at the cap', () => {
    expect(validateMcpServer(stdio({ description: 'a'.repeat(DESCRIPTION_MAX) })).ok).toBe(true);
  });

  it('rejects description over the cap', () => {
    expect(validateMcpServer(stdio({ description: 'a'.repeat(DESCRIPTION_MAX + 1) })).ok).toBe(false);
  });

  it('accepts instruction at the cap', () => {
    expect(validateMcpServer(stdio({ instruction: 'a'.repeat(INSTRUCTION_MAX) })).ok).toBe(true);
  });

  it('rejects instruction over the cap', () => {
    expect(validateMcpServer(stdio({ instruction: 'a'.repeat(INSTRUCTION_MAX + 1) })).ok).toBe(false);
  });
});

describe('mcpSchemas — discovery_uses_secrets', () => {
  it('is optional (validates when omitted)', () => {
    expect(validateMcpServer(stdio()).ok).toBe(true);
    expect(validateMcpServer(http()).ok).toBe(true);
  });

  it('accepts an explicit boolean on every transport', () => {
    expect(validateMcpServer(stdio({ discovery_uses_secrets: true })).ok).toBe(true);
    expect(validateMcpServer(http({ discovery_uses_secrets: true })).ok).toBe(true);
    expect(
      validateMcpServer({
        name: 'sse_server',
        transport: 'sse',
        url: 'https://example.com/sse',
        headers: {},
        discovery_uses_secrets: false,
      }).ok,
    ).toBe(true);
  });

  it('rejects a non-boolean value', () => {
    expect(validateMcpServer(stdio({ discovery_uses_secrets: 'yes' })).ok).toBe(false);
  });
});

describe('mcpSchemas — collectVaultRefs', () => {
  it('returns sorted, de-duplicated vault names from a value map', () => {
    expect(
      collectVaultRefs({ A: '${vault:TOKEN_B}', B: '${vault:TOKEN_A}', C: '${vault:TOKEN_A}', D: 'literal' }),
    ).toEqual(['TOKEN_A', 'TOKEN_B']);
  });

  it('returns empty for no refs', () => {
    expect(collectVaultRefs({ A: 'literal', B: '' })).toEqual([]);
    expect(collectVaultRefs(undefined)).toEqual([]);
  });
});
