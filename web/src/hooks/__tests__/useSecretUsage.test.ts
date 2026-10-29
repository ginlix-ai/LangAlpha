import { expect, it } from 'vitest';
import { catalogServer } from '@/test/factories';
import { secretUsage, serverKeyRefs } from '../useSecretUsage';

it('counts a key a command reads through its args or its address', () => {
  const server = catalogServer({
    name: 'imported',
    args: ['--token=${vault:API_KEY}', '--verbose'],
    url: 'https://example.com/mcp?key=${vault:URL_KEY}',
    env_refs: ['ENV_KEY'],
    header_refs: ['API_KEY'],
  });

  expect(serverKeyRefs(server)).toEqual(['API_KEY', 'ENV_KEY', 'URL_KEY']);
  expect(secretUsage([server]).get('URL_KEY')?.map((s) => s.name)).toEqual(['imported']);
});
