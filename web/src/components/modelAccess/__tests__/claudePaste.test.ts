// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { checkClaudePaste } from '../claudePaste';

describe('checkClaudePaste', () => {
  it.each([
    ['abcDEF123_-xyz#state-456', 'code_state', true],
    ['  abc#def  ', 'code_state', true],
    ['https://console.anthropic.com/oauth/code/callback?code=abc&state=def', 'callback_url', true],
    ['https://platform.claude.com/oauth/code/callback#code=abc&state=def', 'callback_url', true],
    ['code=abc&state=def', 'query', true],
    ['https://claude.ai/oauth/authorize?client_id=x&response_type=code', 'bare_page', false],
    ['https://claude.ai/', 'bare_page', false],
    ['https://example.com/page', 'unknown', false],
    ['abcdefghijklmnopqrstuvwxyz0123', 'code_only', false],
    ['not a code at all', 'unknown', false],
    ['', 'empty', false],
    ['   ', 'empty', false],
  ])('%s → %s', (input, shape, ok) => {
    expect(checkClaudePaste(input)).toEqual({ shape, ok });
  });
});
