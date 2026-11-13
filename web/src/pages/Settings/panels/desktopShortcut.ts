/**
 * Keyboard shortcuts as the desktop shell registers them: Electron
 * accelerators (`Alt+CommandOrControl+L`), read from a key press and shown
 * back in the platform's own words.
 */

/** The parts of a key press a shortcut is made of. */
export interface ShortcutPress {
  code: string;
  metaKey: boolean;
  ctrlKey: boolean;
  altKey: boolean;
  shiftKey: boolean;
}

const NAMED_CODES: Readonly<Record<string, string>> = {
  Space: 'Space',
  Enter: 'Enter',
  Tab: 'Tab',
  Backspace: 'Backspace',
  Delete: 'Delete',
  Insert: 'Insert',
  Home: 'Home',
  End: 'End',
  PageUp: 'PageUp',
  PageDown: 'PageDown',
  ArrowUp: 'Up',
  ArrowDown: 'Down',
  ArrowLeft: 'Left',
  ArrowRight: 'Right',
  Backquote: '`',
  Minus: '-',
  Equal: '=',
  BracketLeft: '[',
  BracketRight: ']',
  Backslash: '\\',
  Semicolon: ';',
  Quote: "'",
  Comma: ',',
  Period: '.',
  Slash: '/',
};

/** The accelerator key for a physical key, or null for a modifier or a key Electron cannot bind. */
function keyOf(code: string): string | null {
  const letter = /^Key([A-Z])$/.exec(code);
  if (letter) return letter[1];
  const digit = /^Digit([0-9])$/.exec(code);
  if (digit) return digit[1];
  const numpad = /^Numpad([0-9])$/.exec(code);
  if (numpad) return `num${numpad[1]}`;
  if (/^F([1-9]|1[0-9]|2[0-4])$/.test(code)) return code;
  return NAMED_CODES[code] ?? null;
}

/**
 * The accelerator a key press makes, or null while it is not one yet (a
 * modifier alone) or cannot be (a plain letter, which would take that key
 * from every other app). Command on a Mac and Control elsewhere become
 * `CommandOrControl`, so the shortcut means the same on every platform.
 */
export function acceleratorFromPress(press: ShortcutPress, platform: string): string | null {
  const key = keyOf(press.code);
  if (!key) return null;
  const mac = platform === 'darwin';
  const parts: string[] = [];
  const primary = mac ? press.metaKey : press.ctrlKey;
  if (primary) parts.push('CommandOrControl');
  if (mac && press.ctrlKey) parts.push('Control');
  if (!mac && press.metaKey) parts.push('Super');
  if (press.altKey) parts.push('Alt');
  // Shift alone makes a capital letter, not a shortcut.
  const hasModifier = parts.length > 0;
  if (press.shiftKey) parts.push('Shift');
  if (!hasModifier && !/^F\d+$/.test(key)) return null;
  return [...parts, key].join('+');
}

const MAC_GLYPHS: Readonly<Record<string, string>> = {
  CommandOrControl: '⌘',
  CmdOrCtrl: '⌘',
  Command: '⌘',
  Cmd: '⌘',
  Control: '⌃',
  Ctrl: '⌃',
  Alt: '⌥',
  Option: '⌥',
  Shift: '⇧',
};

const OTHER_NAMES: Readonly<Record<string, string>> = {
  CommandOrControl: 'Ctrl',
  CmdOrCtrl: 'Ctrl',
  Control: 'Ctrl',
  Super: 'Win',
};

/** An accelerator as the platform writes shortcuts: `⌥⌘L` on a Mac, `Ctrl+Alt+L` elsewhere. */
export function formatAccelerator(accelerator: string, platform: string): string {
  if (!accelerator) return '';
  const parts = accelerator.split('+').filter(Boolean);
  if (platform === 'darwin') {
    // Apple's order: Control, Option, Shift, Command, then the key.
    const order = ['⌃', '⌥', '⇧', '⌘'];
    const glyphs = parts.slice(0, -1).map((p) => MAC_GLYPHS[p] ?? p);
    glyphs.sort((a, b) => order.indexOf(a) - order.indexOf(b));
    return [...glyphs, parts[parts.length - 1]].join('');
  }
  return parts.map((p) => OTHER_NAMES[p] ?? p).join('+');
}
