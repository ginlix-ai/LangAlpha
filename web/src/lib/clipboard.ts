/**
 * `navigator.clipboard` is restricted to secure contexts (HTTPS or localhost):
 * it is `undefined` when the app is opened over plain HTTP from another device
 * on the LAN, so a copy button that calls it throws before anything is written.
 * The fallback selects the text and runs `document.execCommand('copy')`, which
 * every browser still honors inside a click on any origin. It also stands in
 * when the async API refuses the write, as it does once the document has lost
 * focus.
 *
 * Resolves once the text is on the clipboard; rejects when neither way took it.
 */
export async function copyText(text: string): Promise<void> {
  // The DOM types declare the clipboard always present; off a secure context it is not.
  const clipboard = navigator.clipboard as Clipboard | undefined;
  if (clipboard) {
    try {
      await clipboard.writeText(text);
      return;
    } catch (err) {
      if (copyBySelection(text)) return;
      throw err;
    }
  }
  if (!copyBySelection(text)) throw new Error('The browser refused to copy');
}

function copyBySelection(text: string): boolean {
  const holder = document.createElement('span');
  holder.textContent = text;
  // In the layout, since a node that is not rendered cannot be selected, but
  // clipped out of view. `pre` keeps the text's line breaks and runs of spaces.
  Object.assign(holder.style, {
    position: 'fixed',
    top: '0',
    clip: 'rect(0, 0, 0, 0)',
    whiteSpace: 'pre',
    userSelect: 'text',
    webkitUserSelect: 'text',
  });
  document.body.append(holder);
  // A focused field copies its own selection instead (Firefox), or loses it
  // (Chromium, WebKit); a click on a button leaves it focused in Safari and in
  // Firefox on a Mac. It steps aside for the copy and takes focus back after.
  const active = document.activeElement;
  const field = active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement ? active : null;
  field?.blur();
  const selection = document.getSelection();
  const kept = selection
    ? Array.from({ length: selection.rangeCount }, (_, i) => selection.getRangeAt(i))
    : [];
  try {
    const range = document.createRange();
    range.selectNodeContents(holder);
    selection?.removeAllRanges();
    selection?.addRange(range);
    return document.execCommand('copy');
  } catch {
    return false;
  } finally {
    // The reader's own selection comes back as it was.
    selection?.removeAllRanges();
    for (const range of kept) selection?.addRange(range);
    holder.remove();
    field?.focus({ preventScroll: true });
  }
}
