import { describe, it, expect } from 'vitest';
import { parseMcpServersJson } from '../mcpImport';
import {
  findPlaceholders,
  isPlaceholder,
  placeholderId,
  placeholderLabel,
  substitutePlaceholders,
} from '../mcpImportPlaceholders';

const BLOB = {
  mcpServers: {
    'fuyao-a-share': {
      type: 'http',
      url: 'https://fuyao.aicubes.cn/mcp/a-share',
      headers: { 'X-api-key': '<your-api-key>' },
    },
    'fuyao-meta': {
      type: 'http',
      url: 'https://fuyao.aicubes.cn/mcp/meta',
      headers: { 'X-api-key': '<your-api-key>' },
    },
    local: {
      command: 'npx',
      args: ['-y', 'thing', '--token', '<token>'],
      env: { API_TOKEN: 'REPLACE_ME', REGION: 'us-east-1', REF: '${vault:EXISTING}' },
    },
  },
};

const parse = () => parseMcpServersJson(JSON.stringify(BLOB)).servers;

// Two servers, one coerced name: `-` and `_` both normalize to `_`, so the
// display name cannot be what tells their credentials apart.
const ALIKE = {
  mcpServers: {
    'acme-api': {
      type: 'http',
      url: 'https://a.example.com/mcp',
      headers: { 'X-Key': '<your-api-key>' },
    },
    acme_api: {
      type: 'http',
      url: 'https://b.example.com/mcp',
      headers: { 'X-Key': '<your-api-key>' },
    },
  },
};

const parseAlike = () => parseMcpServersJson(JSON.stringify(ALIKE)).servers;

describe('isPlaceholder', () => {
  it('reads angle-bracket and hint-word values as stand-ins, and real values as real', () => {
    expect(isPlaceholder('<your-api-key>')).toBe(true);
    expect(isPlaceholder('your-api-key')).toBe(true);
    expect(isPlaceholder('REPLACE_ME')).toBe(true);
    expect(isPlaceholder('xxx')).toBe(true);
    expect(isPlaceholder('sk-live-9f8e7d6c5b4a')).toBe(false);
    expect(isPlaceholder('us-east-1')).toBe(false);
    expect(isPlaceholder('${vault:TOKEN}')).toBe(false);
    expect(isPlaceholder('')).toBe(false);
  });

  it('only reads the loose words as stand-ins when they are the whole value', () => {
    // A real token carrying either of these used to be swapped for a blank
    // input, and the literal was then printed unmasked beside it.
    expect(isPlaceholder('example')).toBe(true);
    expect(isPlaceholder('Bearer example-corp-9f8e7d')).toBe(false);
    expect(isPlaceholder('xxxx')).toBe(true);
    expect(isPlaceholder('sk-live-xxxx9f8e7d')).toBe(false);
  });
});

describe('findPlaceholders', () => {
  it('asks each server for its own value even when two share a spelling', () => {
    const found = findPlaceholders(parse());
    expect(found.map((p) => [p.server, p.literal])).toEqual([
      ['fuyao_a_share', '<your-api-key>'],
      ['fuyao_meta', '<your-api-key>'],
      ['local', 'REPLACE_ME'],
      ['local', '<token>'],
    ]);
    expect(found[0].id).not.toBe(found[1].id);
    expect(found[0].uses).toEqual([
      { server: 'fuyao_a_share', where: 'headers', key: 'X-api-key' },
    ]);
    expect(found[3].uses).toEqual([{ server: 'local', where: 'args', key: '#4' }]);
  });

  it('asks twice when two names coerce to one, and shows the shared label', () => {
    const found = findPlaceholders(parseAlike());
    expect(found).toHaveLength(2);
    expect(found[0].id).not.toBe(found[1].id);
    // Both render the same coerced name, which is exactly why the id is not it.
    expect(found.map((p) => p.server)).toEqual(['acme_api', 'acme_api']);
  });

  it('names each field by where its value goes, never by the literal', () => {
    const found = findPlaceholders(parse());
    expect(placeholderLabel(found[1])).toBe('fuyao_meta · X-api-key');
    expect(placeholderLabel(found[3])).toBe('local · #4');
  });
});

// The backend vaults an argv value only where a secret-looking flag names it
// (`_SECRET_KEY_RE` in `mcp_sanitize.py`), so the credentials step has to ask
// about exactly those, or the import stores the placeholder as the credential
// and the server authenticates with `REPLACE_ME`.
describe('findPlaceholders on argv values a secret flag names', () => {
  const blobOf = (args: string[]) => ({ mcpServers: { tool: { command: 'npx', args } } });
  const found = (args: string[]) =>
    findPlaceholders(parseMcpServersJson(JSON.stringify(blobOf(args))).servers);

  it('reads the value a secret flag names, in either shape', () => {
    expect(found(['--token', 'REPLACE_ME']).map((p) => [p.literal, p.uses[0].where])).toEqual([
      ['REPLACE_ME', 'args'],
    ]);
    expect(found(['--api-key=your-api-key']).map((p) => [p.literal, p.uses[0].key])).toEqual([
      ['your-api-key', '#1'],
    ]);
  });

  it('leaves a value no secret flag names alone', () => {
    // A package name is not a stand-in, and a port is not a credential.
    expect(found(['-y', 'todo-mcp-server'])).toEqual([]);
    expect(found(['--port', 'REPLACE_ME'])).toEqual([]);
  });

  it('still reads the angle spelling anywhere in the argv', () => {
    expect(found(['-y', '<thing>']).map((p) => p.literal)).toEqual(['<thing>']);
  });

  it('leaves a bare positional credential to the user, since nothing would vault it', () => {
    // No flag names a positional, so the backend stores whatever is typed there
    // as plaintext `args`. Asking for it under copy that promises the vault is
    // the one case where importing the literal unchanged is the safer answer.
    expect(found(['-y', 'thing', '<your-api-key>'])).toEqual([]);
    expect(found(['<token>'])).toEqual([]);
    expect(found(['<API_KEY>'])).toEqual([]);
    // A positional naming anything else is still a stand-in worth asking about.
    expect(found(['--dir', '<path>', '<region>']).map((p) => p.literal)).toEqual([
      '<path>',
      '<region>',
    ]);
  });

  it('fills the value half of a `--flag=value` argument and keeps the flag', () => {
    const payload = blobOf(['--api-key=your-api-key']);
    const out = substitutePlaceholders(
      payload,
      { [placeholderId('tool', 'your-api-key')]: 'sk-live' },
      parseMcpServersJson(JSON.stringify(payload)).servers,
    ) as ReturnType<typeof blobOf>;
    expect(out.mcpServers.tool.args).toEqual(['--api-key=sk-live']);
  });
});

describe('substitutePlaceholders', () => {
  it('keeps two servers that share a spelling on their own keys', () => {
    const servers = parse();
    const out = substitutePlaceholders(
      BLOB,
      {
        [placeholderId('fuyao-a-share', '<your-api-key>')]: 'key-a',
        [placeholderId('fuyao-meta', '<your-api-key>')]: 'key-b',
      },
      servers,
    ) as typeof BLOB;
    expect(out.mcpServers['fuyao-a-share'].headers['X-api-key']).toBe('key-a');
    expect(out.mcpServers['fuyao-meta'].headers['X-api-key']).toBe('key-b');
    // The input is not mutated.
    expect(BLOB.mcpServers['fuyao-a-share'].headers['X-api-key']).toBe('<your-api-key>');
  });

  it('sends each alike-coercing server only its own answer', () => {
    const out = substitutePlaceholders(
      ALIKE,
      {
        [placeholderId('acme-api', '<your-api-key>')]: 'key-a',
        [placeholderId('acme_api', '<your-api-key>')]: 'key-b',
      },
      parseAlike(),
    ) as typeof ALIKE;
    expect(out.mcpServers['acme-api'].headers['X-Key']).toBe('key-a');
    expect(out.mcpServers.acme_api.headers['X-Key']).toBe('key-b');
  });

  it('replaces a filled literal wherever it sits and leaves blanks as they were', () => {
    const servers = parse();
    const out = substitutePlaceholders(
      BLOB,
      {
        [placeholderId('fuyao-a-share', '<your-api-key>')]: 'real-key',
        [placeholderId('local', '<token>')]: '',
      },
      servers,
    ) as typeof BLOB;
    expect(out.mcpServers['fuyao-a-share'].headers['X-api-key']).toBe('real-key');
    // Another server's identical spelling is not this server's answer.
    expect(out.mcpServers['fuyao-meta'].headers['X-api-key']).toBe('<your-api-key>');
    expect(out.mcpServers.local.args).toEqual(['-y', 'thing', '--token', '<token>']);
    expect(out.mcpServers.local.env.API_TOKEN).toBe('REPLACE_ME');
  });

  it('substitutes into an argv slot as readily as into a header', () => {
    const servers = parse();
    const out = substitutePlaceholders(
      BLOB,
      { [placeholderId('local', '<token>')]: 'tok' },
      servers,
    ) as typeof BLOB;
    expect(out.mcpServers.local.args).toEqual(['-y', 'thing', '--token', 'tok']);
  });

  it('returns the payload untouched when nothing was filled', () => {
    expect(
      substitutePlaceholders(
        BLOB,
        { [placeholderId('fuyao-a-share', '<your-api-key>')]: '  ' },
        parse(),
      ),
    ).toBe(BLOB);
  });

  it('fills the credential and leaves prose that merely names the literal alone', () => {
    const blob = {
      mcpServers: {
        acme: {
          type: 'http',
          url: 'https://acme.example.com/mcp',
          headers: { Authorization: '<your-api-key>' },
          description: 'Configure <your-api-key>',
        },
      },
    };
    const out = substitutePlaceholders(
      blob,
      { [placeholderId('acme', '<your-api-key>')]: 'sk-live' },
      parseMcpServersJson(JSON.stringify(blob)).servers,
    ) as typeof blob;
    expect(out.mcpServers.acme.headers.Authorization).toBe('sk-live');
    expect(out.mcpServers.acme.description).toBe('Configure <your-api-key>');
  });
});

describe('substitutePlaceholders fills credential containers only', () => {
  it('leaves a literal that is the whole of a description untouched', () => {
    const payload = {
      mcpServers: {
        docs: {
          url: 'https://docs.example.com/mcp',
          description: '<your-api-key>',
          headers: { Authorization: '<your-api-key>' },
          env: { TOKEN: '<your-api-key>' },
          args: ['--key', '<your-api-key>'],
        },
      },
    };
    const servers = parseMcpServersJson(JSON.stringify(payload)).servers;
    const out = substitutePlaceholders(
      payload,
      { [placeholderId('docs', '<your-api-key>')]: 'sk-live' },
      servers,
    ) as { mcpServers: { docs: Record<string, unknown> } };
    const docs = out.mcpServers.docs;
    expect(docs.headers).toEqual({ Authorization: 'sk-live' });
    expect(docs.env).toEqual({ TOKEN: 'sk-live' });
    expect(docs.args).toEqual(['--key', 'sk-live']);
    expect(docs.description).toBe('<your-api-key>');
    expect(docs.url).toBe('https://docs.example.com/mcp');
  });
});

// A vendor hands out the whole header value, scheme included, so the collected
// literal is `Bearer <your-api-key>` and only the bracketed span is the user's
// to supply. Filling the whole value dropped the scheme and the imported server
// authenticated with a bare token.
describe('substitutePlaceholders fills the bracketed span, not the whole value', () => {
  const fill = (payload: object, literal: string, value: string) =>
    substitutePlaceholders(
      payload,
      { [placeholderId('acme', literal)]: value },
      parseMcpServersJson(JSON.stringify(payload)).servers,
    ) as { mcpServers: { acme: Record<string, Record<string, string>> } };

  it('keeps the scheme in front of a header value that wraps the placeholder', () => {
    const payload = {
      mcpServers: {
        acme: {
          type: 'http',
          url: 'https://acme.example.com/mcp',
          headers: { Authorization: 'Bearer <your-api-key>' },
        },
      },
    };
    // The literal collected is the whole value, which is what the user sees.
    expect(
      findPlaceholders(parseMcpServersJson(JSON.stringify(payload)).servers).map((p) => p.literal),
    ).toEqual(['Bearer <your-api-key>']);
    const out = fill(payload, 'Bearer <your-api-key>', 'EXAMPLE-TOKEN');
    expect(out.mcpServers.acme.headers.Authorization).toBe('Bearer EXAMPLE-TOKEN');
  });

  it('does not double the scheme when the user pastes it along with the token', () => {
    const payload = {
      mcpServers: {
        acme: {
          type: 'http',
          url: 'https://acme.example.com/mcp',
          headers: { Authorization: 'Bearer <your-api-key>' },
        },
      },
    };
    const out = fill(payload, 'Bearer <your-api-key>', 'bearer EXAMPLE-TOKEN');
    expect(out.mcpServers.acme.headers.Authorization).toBe('Bearer EXAMPLE-TOKEN');
  });

  it('replaces an env value that is nothing but the span', () => {
    const payload = {
      mcpServers: {
        acme: { command: 'npx', args: ['-y', 'thing'], env: { TOKEN: '<token>' } },
      },
    };
    expect(fill(payload, '<token>', 'EXAMPLE-TOKEN').mcpServers.acme.env.TOKEN).toBe(
      'EXAMPLE-TOKEN',
    );
  });

  it('replaces a literal carrying no span whole, the way it always did', () => {
    const payload = {
      mcpServers: {
        acme: { command: 'npx', args: ['-y', 'thing'], env: { TOKEN: 'REPLACE_ME' } },
      },
    };
    expect(fill(payload, 'REPLACE_ME', 'EXAMPLE-TOKEN').mcpServers.acme.env.TOKEN).toBe(
      'EXAMPLE-TOKEN',
    );
  });

  it('fills every span in a literal that carries two', () => {
    const payload = {
      mcpServers: {
        acme: {
          type: 'http',
          url: 'https://acme.example.com/mcp',
          headers: { 'X-Key': 'id=<your-id> key=<your-api-key>' },
        },
      },
    };
    expect(fill(payload, 'id=<your-id> key=<your-api-key>', 'EXAMPLE-TOKEN').mcpServers.acme
      .headers['X-Key']).toBe('id=EXAMPLE-TOKEN key=EXAMPLE-TOKEN');
  });
});
