import { describe, expect, it } from 'vitest';
import { DRIVE_COMMENT_SURFACE, DRIVE_TAB_COMMENT_SURFACE } from '@/pages/Drive/useDriveFileComments';
import { enterFor, footerLayout } from '../composerFooter';

describe('a comment box on the Drive page', () => {
  it('sends on Enter and the filled button, and queues on the quiet one and ⌘/Ctrl+Enter, whatever the owner picked', () => {
    for (const preference of ['queue', 'send'] as const) {
      const layout = footerLayout(true, true, enterFor(DRIVE_COMMENT_SURFACE, preference));
      expect(layout.primary).toBe('send');
      expect(layout.quiet).toEqual({ route: 'queue', shown: true });
      expect(layout.modEnter).toBe('queue');
    }
  });

  it('keeps the owner\'s preference everywhere else', () => {
    expect(enterFor({}, 'queue')).toBe('queue');
    expect(enterFor({}, 'send')).toBe('send');
    expect(enterFor({ pickAnyChat: true }, 'queue')).toBe('queue');
  });

  it("keeps it in a chat's Drive tab too, where the chat beside the file shows the queue", () => {
    expect(enterFor(DRIVE_TAB_COMMENT_SURFACE, 'queue')).toBe('queue');
    expect(enterFor(DRIVE_TAB_COMMENT_SURFACE, 'send')).toBe('send');
    expect(DRIVE_TAB_COMMENT_SURFACE.pickAnyChat).toBe(true);
  });
});
