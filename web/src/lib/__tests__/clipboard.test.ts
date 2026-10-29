import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { copyText } from '../clipboard';

describe('copyText', () => {
  let copiedSelection: string | null;
  let execResult: boolean;

  beforeEach(() => {
    copiedSelection = null;
    execResult = true;
    Object.defineProperty(document, 'execCommand', {
      configurable: true,
      value: vi.fn((command: string) => {
        if (command === 'copy') copiedSelection = String(document.getSelection());
        return execResult;
      }),
    });
  });

  afterEach(() => {
    Reflect.deleteProperty(navigator, 'clipboard');
    Reflect.deleteProperty(document, 'execCommand');
    document.body.innerHTML = '';
  });

  const withClipboard = (writeText: (text: string) => Promise<void>) => {
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
  };

  it('writes through the async clipboard where there is one', async () => {
    const writeText = vi.fn(() => Promise.resolve());
    withClipboard(writeText);
    await copyText('AAPL');
    expect(writeText).toHaveBeenCalledWith('AAPL');
    expect(document.execCommand).not.toHaveBeenCalled();
  });

  it('copies a selection of the text on an origin with no clipboard (plain HTTP on the LAN)', async () => {
    document.body.innerHTML = '<p>what the reader selected</p>';
    const range = document.createRange();
    range.selectNodeContents(document.querySelector('p')!);
    document.getSelection()!.addRange(range);

    await copyText('line one\n  line two');

    expect(copiedSelection).toBe('line one\n  line two');
    expect(String(document.getSelection())).toBe('what the reader selected');
    expect(document.body.children).toHaveLength(1);
  });

  it('copies past a focused field and gives it focus back', async () => {
    // Firefox copies a focused field's own selection rather than the text's.
    document.body.innerHTML = '<textarea>my draft</textarea>';
    const field = document.querySelector('textarea')!;
    field.focus();
    field.select();
    let focusedAtCopy: Element | null = null;
    vi.mocked(document.execCommand).mockImplementation(() => {
      focusedAtCopy = document.activeElement;
      copiedSelection = String(document.getSelection());
      return true;
    });

    await copyText('AAPL');

    expect(focusedAtCopy).not.toBe(field);
    expect(copiedSelection).toBe('AAPL');
    expect(document.activeElement).toBe(field);
    expect([field.selectionStart, field.selectionEnd]).toEqual([0, 8]);
  });

  it('falls back when the async clipboard refuses the write', async () => {
    withClipboard(() => Promise.reject(new DOMException('Document is not focused.', 'NotAllowedError')));
    await copyText('AAPL');
    expect(copiedSelection).toBe('AAPL');
  });

  it('rejects when neither way takes the text', async () => {
    execResult = false;
    await expect(copyText('AAPL')).rejects.toThrow();
    withClipboard(() => Promise.reject(new DOMException('Document is not focused.', 'NotAllowedError')));
    await expect(copyText('AAPL')).rejects.toThrow('Document is not focused.');
  });
});
