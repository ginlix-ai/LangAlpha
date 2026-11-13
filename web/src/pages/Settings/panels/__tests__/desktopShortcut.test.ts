// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { acceleratorFromPress, formatAccelerator } from '../desktopShortcut';

const press = (code: string, mods: Partial<Record<'metaKey' | 'ctrlKey' | 'altKey' | 'shiftKey', boolean>> = {}) => ({
  code,
  metaKey: false,
  ctrlKey: false,
  altKey: false,
  shiftKey: false,
  ...mods,
});

describe('acceleratorFromPress', () => {
  it('reads Command on a Mac and Control elsewhere as CommandOrControl', () => {
    expect(acceleratorFromPress(press('KeyL', { metaKey: true, altKey: true }), 'darwin')).toBe('CommandOrControl+Alt+L');
    expect(acceleratorFromPress(press('KeyL', { ctrlKey: true, altKey: true }), 'win32')).toBe('CommandOrControl+Alt+L');
    expect(acceleratorFromPress(press('KeyL', { ctrlKey: true }), 'darwin')).toBe('Control+L');
  });

  it('is not a shortcut yet with a modifier alone, a bare key, or Shift alone', () => {
    expect(acceleratorFromPress(press('MetaLeft', { metaKey: true }), 'darwin')).toBeNull();
    expect(acceleratorFromPress(press('KeyL'), 'darwin')).toBeNull();
    expect(acceleratorFromPress(press('KeyL', { shiftKey: true }), 'darwin')).toBeNull();
  });

  it('takes a function key alone, and names the keys Electron names', () => {
    expect(acceleratorFromPress(press('F5'), 'linux')).toBe('F5');
    expect(acceleratorFromPress(press('Space', { altKey: true, shiftKey: true }), 'darwin')).toBe('Alt+Shift+Space');
    expect(acceleratorFromPress(press('ArrowUp', { ctrlKey: true }), 'linux')).toBe('CommandOrControl+Up');
  });
});

describe('formatAccelerator', () => {
  it('writes a shortcut the way the platform does', () => {
    expect(formatAccelerator('Alt+CommandOrControl+L', 'darwin')).toBe('⌥⌘L');
    expect(formatAccelerator('CommandOrControl+Shift+Control+K', 'darwin')).toBe('⌃⇧⌘K');
    expect(formatAccelerator('Alt+CommandOrControl+L', 'win32')).toBe('Alt+Ctrl+L');
    expect(formatAccelerator('', 'darwin')).toBe('');
  });
});
