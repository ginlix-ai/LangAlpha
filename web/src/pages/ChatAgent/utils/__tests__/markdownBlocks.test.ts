import { describe, it, expect } from 'vitest';
import { splitMarkdownBlocks } from '../markdownBlocks';

describe('splitMarkdownBlocks', () => {
  it('reproduces the input when joined and cuts at paragraph breaks', () => {
    const doc = '# Title\n\nFirst paragraph.\n\nSecond paragraph.\n';
    const blocks = splitMarkdownBlocks(doc);
    expect(blocks.join('')).toBe(doc);
    expect(blocks).toEqual(['# Title\n\n', 'First paragraph.\n\n', 'Second paragraph.\n']);
  });

  it('keeps a fenced code block whole across blank lines, closed or not', () => {
    const closed = 'Intro\n\n```python\na = 1\n\nb = 2\n```\n\nAfter\n';
    expect(splitMarkdownBlocks(closed)).toEqual(['Intro\n\n', '```python\na = 1\n\nb = 2\n```\n\n', 'After\n']);
    const open = 'Intro\n\n```python\na = 1\n\nb = 2\n';
    expect(splitMarkdownBlocks(open)).toEqual(['Intro\n\n', '```python\na = 1\n\nb = 2\n']);
  });

  it('keeps display math whole across blank lines', () => {
    const doc = 'Solve\n\n$$\nx = 1\n\ny = 2\n$$\n\nDone\n';
    expect(splitMarkdownBlocks(doc)).toEqual(['Solve\n\n', '$$\nx = 1\n\ny = 2\n$$\n\n', 'Done\n']);
  });

  it('keeps loose lists and indented continuations in one block', () => {
    const loose = '- a\n\n- b\n\n1. c\n\nPara\n';
    expect(splitMarkdownBlocks(loose)).toEqual(['- a\n\n- b\n\n1. c\n\n', 'Para\n']);
    const nested = '- item\n\n  continued paragraph\n\n  ```sh\n  ls\n  ```\n\nPara\n';
    expect(splitMarkdownBlocks(nested)).toEqual(['- item\n\n  continued paragraph\n\n  ```sh\n  ls\n  ```\n\n', 'Para\n']);
  });

  it('renders whole when a construct has cross-block meaning', () => {
    for (const doc of [
      'See [spec][1].\n\n[1]: https://example.com\n',
      'Claim[^1].\n\n[^1]: Footnote.\n',
      '<div align="center">\n\n**bold**\n\n</div>\n',
      // Tag names are case-insensitive to the parser, so they are here too.
      '<DETAILS>\n\n<SUMMARY>More</SUMMARY>\n\n</DETAILS>\n',
      // Allowed containers outside CommonMark's list keep their children too.
      '<picture>\n\n<source srcset="a.avif" />\n<img src="a.png" />\n\n</picture>\n',
      '<svg viewBox="0 0 10 10">\n\n<circle r="4" />\n\n</svg>\n',
      '<math>\n\n<mi>x</mi>\n\n</math>\n',
      '<!-- note -->\n\nPara\n',
      // A definition keeps document scope inside a block quote or list item.
      '> [ref]: https://example.com\n\nSee [spec][ref].\n',
      '- [ref]: https://example.com\n\nSee [spec][ref].\n',
      '1. > [^1]: Footnote.\n\nClaim[^1].\n',
    ]) {
      expect(splitMarkdownBlocks(doc)).toEqual([doc]);
    }
  });

  it('ignores cross-block lookalikes inside a fence or display math', () => {
    const fenced = 'Para\n\n```html\n<div>\n[label]: value\n```\n\nAfter\n';
    expect(splitMarkdownBlocks(fenced)).toEqual(['Para\n\n', '```html\n<div>\n[label]: value\n```\n\n', 'After\n']);
    const math = 'Para\n\n$$\n<p>\n$$\n\nAfter\n';
    expect(splitMarkdownBlocks(math)).toEqual(['Para\n\n', '$$\n<p>\n$$\n\n', 'After\n']);
  });

  it('does not treat an inline citation element as block HTML', () => {
    const doc = '<cite-bubble label="a" href="https://x"></cite-bubble> leads.\n\nNext\n';
    expect(splitMarkdownBlocks(doc)).toHaveLength(2);
  });

  it('handles empty input and runs of blank lines', () => {
    expect(splitMarkdownBlocks('')).toEqual(['']);
    expect(splitMarkdownBlocks('\n\n')).toEqual(['\n\n']);
    expect(splitMarkdownBlocks('a\n\n\n\nb')).toEqual(['a\n\n\n\n', 'b']);
  });
});
