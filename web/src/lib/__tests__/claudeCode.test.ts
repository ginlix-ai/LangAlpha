/**
 * A Claude Code refusal says how it is fixed: a token Claude refused is fixed
 * with a new one in Settings, as no token is, and the app words it itself.
 */
import { describe, it, expect } from 'vitest';
import { claudeCodeRefusal, claudeCodeTokenUrl } from '../claudeCode';

describe('claudeCodeRefusal', () => {
  it('links a refused token to its sheet, reads the tokens again, and words it in the app', () => {
    expect(claudeCodeRefusal('token_refused', 'ws-1')).toEqual({
      reread: true,
      link: { url: claudeCodeTokenUrl('ws-1'), label: 'Add setup token', labelKey: 'modelAccess.claudeCode.add' },
      messageKey: 'chat.claudeCode.off.token_refused',
    });
  });

  it('keeps the server words for a reason the app does not word', () => {
    expect(claudeCodeRefusal('signed_out', null).messageKey).toBeNull();
    expect(claudeCodeRefusal('server', null)).toEqual({ reread: false, link: null, messageKey: null });
  });
});
