// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { enterKey, footerLayout } from '../composerFooter';

const key = (k: string, mods: Partial<{ shiftKey: boolean; metaKey: boolean; ctrlKey: boolean }> = {}) => ({
  key: k, shiftKey: false, metaKey: false, ctrlKey: false, ...mods,
});

describe('the comment box footer', () => {
  it('queues on Enter and the filled button, and sends this one alone on ⌘/Ctrl+Enter, while ticked', () => {
    const layout = footerLayout(true, true);
    expect(layout.primary).toBe('queue');
    expect(layout.quiet).toEqual({ route: 'send', shown: true });
    expect(layout.modEnter).toBe('send');
  });

  it('saves a note on every key once unticked, and hides Send now in place', () => {
    const layout = footerLayout(false, true);
    expect(layout.primary).toBe('note');
    expect(layout.quiet).toEqual({ route: 'send', shown: false });
    expect(layout.modEnter).toBe('note');
  });

  it('sends on the filled button where no thread would hold a queue, and saves a note unticked', () => {
    const layout = footerLayout(true, false);
    expect(layout.primary).toBe('send');
    expect(layout.quiet).toBeNull();
    expect(layout.modEnter).toBe('send');
    expect(footerLayout(false, false)).toEqual({ primary: 'note', quiet: null, modEnter: 'note' });
  });

  it('sends on Enter and queues on ⌘/Ctrl+Enter when the owner picked Send now for Enter', () => {
    const layout = footerLayout(true, true, 'send');
    expect(layout.primary).toBe('send');
    expect(layout.quiet).toEqual({ route: 'queue', shown: true });
    expect(layout.modEnter).toBe('queue');
  });

  it('still saves a note unticked, fading Add to queue in place, when Enter sends', () => {
    const layout = footerLayout(false, true, 'send');
    expect(layout.primary).toBe('note');
    expect(layout.quiet).toEqual({ route: 'queue', shown: false });
    expect(layout.modEnter).toBe('note');
  });

  it('always sends where no thread would hold a queue, whichever Enter the owner picked', () => {
    expect(footerLayout(true, false, 'send')).toEqual(footerLayout(true, false, 'queue'));
    expect(footerLayout(false, false, 'send')).toEqual({ primary: 'note', quiet: null, modEnter: 'note' });
  });

  it('leaves Shift+Enter and every other key to the field', () => {
    expect(enterKey(key('Enter', { shiftKey: true }))).toBeNull();
    expect(enterKey(key('Enter', { shiftKey: true, metaKey: true }))).toBeNull();
    expect(enterKey(key('a'))).toBeNull();
    expect(enterKey(key('Enter'))).toBe('plain');
    expect(enterKey(key('Enter', { metaKey: true }))).toBe('mod');
    expect(enterKey(key('Enter', { ctrlKey: true }))).toBe('mod');
  });

  it('leaves Enter a new line with an on-screen keyboard up, whose return has no modifier, for the buttons to send', () => {
    expect(enterKey(key('Enter'), true)).toBeNull();
    expect(enterKey(key('Enter', { metaKey: true }), true)).toBeNull();
    // A hardware keyboard covers nothing: Enter sends as before.
    expect(enterKey(key('Enter'), false)).toBe('plain');
  });
});
