// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { footerLayout, routeForKey } from '../composerFooter';

const key = (k: string, mods: Partial<{ shiftKey: boolean; metaKey: boolean; ctrlKey: boolean }> = {}) => ({
  key: k, shiftKey: false, metaKey: false, ctrlKey: false, ...mods,
});

describe('the comment box footer', () => {
  it('queues on Enter and the filled button, and sends this one alone on ⌘/Ctrl+Enter, while ticked', () => {
    const layout = footerLayout(true, true);
    expect(layout.primary).toBe('queue');
    expect(layout.quiet).toEqual({ route: 'send', shown: true });
    expect(routeForKey(key('Enter'), layout)).toBe('queue');
    expect(routeForKey(key('Enter', { metaKey: true }), layout)).toBe('send');
    expect(routeForKey(key('Enter', { ctrlKey: true }), layout)).toBe('send');
  });

  it('saves a note on every key once unticked, and hides Send now in place', () => {
    const layout = footerLayout(false, true);
    expect(layout.primary).toBe('note');
    expect(layout.quiet).toEqual({ route: 'send', shown: false });
    expect(routeForKey(key('Enter'), layout)).toBe('note');
    expect(routeForKey(key('Enter', { metaKey: true }), layout)).toBe('note');
  });

  it('sends on the filled button where no thread would hold a queue, and saves a note unticked', () => {
    const layout = footerLayout(true, false);
    expect(layout.primary).toBe('send');
    expect(layout.quiet).toBeNull();
    expect(routeForKey(key('Enter'), layout)).toBe('send');
    expect(routeForKey(key('Enter', { metaKey: true }), layout)).toBe('send');
    expect(footerLayout(false, false)).toEqual({ primary: 'note', quiet: null, modEnter: 'note' });
  });

  it('sends on Enter and queues on ⌘/Ctrl+Enter when the owner picked Send now for Enter', () => {
    const layout = footerLayout(true, true, 'send');
    expect(layout.primary).toBe('send');
    expect(layout.quiet).toEqual({ route: 'queue', shown: true });
    expect(routeForKey(key('Enter'), layout)).toBe('send');
    expect(routeForKey(key('Enter', { metaKey: true }), layout)).toBe('queue');
    expect(routeForKey(key('Enter', { ctrlKey: true }), layout)).toBe('queue');
  });

  it('still saves a note unticked, fading Add to queue in place, when Enter sends', () => {
    const layout = footerLayout(false, true, 'send');
    expect(layout.primary).toBe('note');
    expect(layout.quiet).toEqual({ route: 'queue', shown: false });
    expect(routeForKey(key('Enter'), layout)).toBe('note');
    expect(routeForKey(key('Enter', { metaKey: true }), layout)).toBe('note');
  });

  it('always sends where no thread would hold a queue, whichever Enter the owner picked', () => {
    expect(footerLayout(true, false, 'send')).toEqual(footerLayout(true, false, 'queue'));
    expect(footerLayout(false, false, 'send')).toEqual({ primary: 'note', quiet: null, modEnter: 'note' });
  });

  it('leaves Shift+Enter and every other key to the field', () => {
    const layout = footerLayout(true, true);
    expect(routeForKey(key('Enter', { shiftKey: true }), layout)).toBeNull();
    expect(routeForKey(key('Enter', { shiftKey: true, metaKey: true }), layout)).toBeNull();
    expect(routeForKey(key('a'), layout)).toBeNull();
  });
});
