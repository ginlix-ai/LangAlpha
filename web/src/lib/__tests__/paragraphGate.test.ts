import { describe, it, expect } from 'vitest';
import { visibleParagraphPrefix } from '../paragraphGate';

describe('visibleParagraphPrefix', () => {
  it('holds a reply that has not reached a blank line yet', () => {
    expect(visibleParagraphPrefix('')).toBe('');
    expect(visibleParagraphPrefix('The first sentence is still')).toBe('');
    expect(visibleParagraphPrefix('one line\nand a second')).toBe('');
  });

  it('cuts at the last blank line and holds the tail', () => {
    expect(visibleParagraphPrefix('first\n\nsecond')).toBe('first\n\n');
    expect(visibleParagraphPrefix('first\n\nsecond\n\nthird')).toBe('first\n\nsecond\n\n');
  });

  it('skips a blank line inside an open fence', () => {
    const text = 'intro\n\n```python\nx = 1\n\ny = 2\n';
    expect(visibleParagraphPrefix(text)).toBe('intro\n\n');
  });

  it('holds everything when the only blank line is inside an open fence', () => {
    expect(visibleParagraphPrefix('~~~\na\n\nb\n')).toBe('');
  });

  it('a mismatched marker does not close the fence', () => {
    // ``` cannot be closed by ~~~, so the blank line after it is still code.
    expect(visibleParagraphPrefix('```\na\n~~~\n\nb\n')).toBe('');
  });

  it('cuts after a fence that closed', () => {
    const text = 'intro\n\n```js\nconst a = 1;\n\nconst b = 2;\n```\n\ntail';
    expect(visibleParagraphPrefix(text)).toBe('intro\n\n```js\nconst a = 1;\n\nconst b = 2;\n```\n\n');
  });

  it('a marker with trailing text does not close the fence', () => {
    // Only whitespace may follow a closing fence, so a nested ```js line is
    // still code and the blank line under it is not a paragraph boundary.
    expect(visibleParagraphPrefix('```\nshow me:\n```js\n\nstill code\n')).toBe('');
    // The real close still lands, and the tail after it is paintable.
    const text = 'intro\n\n```\n```js\n\ncode\n```\n\ntail';
    expect(visibleParagraphPrefix(text)).toBe('intro\n\n```\n```js\n\ncode\n```\n\n');
  });

  it('a backtick opener whose info string holds a backtick is prose', () => {
    // CommonMark: no backtick may appear in a backtick fence's info string, so
    // this line opens nothing and the blank line under it is a real boundary.
    expect(visibleParagraphPrefix('intro\n\n```a`b\n\ntail')).toBe('intro\n\n```a`b\n\n');
    // A tilde fence has no such rule and still opens.
    expect(visibleParagraphPrefix('intro\n\n~~~a`b\n\ntail')).toBe('intro\n\n');
  });

  it('opens the gate once the held tail passes the cap', () => {
    const held = 'x'.repeat(2001);
    expect(visibleParagraphPrefix(`first\n\n${held}`)).toBe(`first\n\n${held}`);
    // A single giant paragraph with no boundary at all still appears.
    const giant = 'y'.repeat(2001);
    expect(visibleParagraphPrefix(giant)).toBe(giant);
    // An unterminated fence is not a reason to sit on a whole answer.
    const fenced = `\`\`\`\n${'z'.repeat(2001)}`;
    expect(visibleParagraphPrefix(fenced)).toBe(fenced);
  });

  it('a blockquote separates its paragraphs with a marker-only line', () => {
    // CommonMark writes the blank line between two quoted paragraphs as `>` or
    // `> `. Reading only a wholly empty line left a reply that opens with a
    // quote with no boundary anywhere in it, withheld until the cap.
    expect(visibleParagraphPrefix('> first\n>\n> second still bei')).toBe('> first\n>\n');
    expect(visibleParagraphPrefix('> first\n> \n> second still bei')).toBe('> first\n> \n');
    expect(visibleParagraphPrefix('> > first\n> >\n> > second still bei')).toBe('> > first\n> >\n');
  });

  it('tracks a fence opened inside a blockquote, and only closes it in there', () => {
    // The fence marker carries the quote prefix too, so without stripping it
    // the block was never open and `>` lines inside it read as boundaries.
    expect(visibleParagraphPrefix('> ```js\n> code\n>\n> more code')).toBe('');
    expect(visibleParagraphPrefix('> ```js\n> code\n> ```\n>\n> after')).toBe('> ```js\n> code\n> ```\n>\n');
    // A quoted fence line inside a top-level block belongs to the code, not to
    // the fence: closing on it would cut the reply in the middle of the block.
    expect(visibleParagraphPrefix('```\n> ```\n\nstill code')).toBe('');
  });

  it('tracks a fence indented under a list marker', () => {
    // `10. ` is four characters, so the step's content starts past the three
    // spaces CommonMark allows before an unindented fence. Missing the fence
    // here released a prefix ending inside the code block, with the opener
    // never closed.
    const text = '10. Pull the filing:\n\n    ```python\n    x = 1\n\n    y = 2';

    expect(visibleParagraphPrefix(text)).toBe('10. Pull the filing:\n\n');
  });

  it('closes a list-indented fence at its own indent', () => {
    const text = '1. Step:\n\n   ```py\n   x = 1\n   ```\n\ntail';

    expect(visibleParagraphPrefix(text)).toBe('1. Step:\n\n   ```py\n   x = 1\n   ```\n\n');
  });

  it('does not let a less indented marker close a fence opened inside a list', () => {
    // Outdenting has left the container the fence was opened in, so this is
    // not its closer. Holding is the safe way to be wrong: the alternative
    // cuts the reply inside the block.
    //
    // Four spaces, because that is where a container is legible without
    // parsing one. Three or fewer is where the two readings collide: the same
    // indent is an ordinary top-level fence and the first column of a `1. `
    // item's content, and the gate has no way to tell them apart.
    const text = '1.  Step:\n\n    ```py\n    x = 1\n```\n\ntail';

    expect(visibleParagraphPrefix(text)).toBe('1.  Step:\n\n');
  });

  it('closes a top-level fence indented under four spaces at the left margin', () => {
    // A fence written one to three spaces in is top-level however it looks,
    // and CommonMark closes it at nought to three. Reading that indent as a
    // container held the whole reply until the cap, which is the stall
    // paragraph mode exists to prevent.
    const text = 'Here:\n\n   ```js\nx = 1\n```\n\ntail';

    expect(visibleParagraphPrefix(text)).toBe('Here:\n\n   ```js\nx = 1\n```\n\n');
  });

  it('reads a marker indented past a top-level fence as code, not its end', () => {
    const text = 'Here:\n\n```js\n    ```\n\nstill code\n```\n\ntail';

    expect(visibleParagraphPrefix(text)).toBe('Here:\n\n```js\n    ```\n\nstill code\n```\n\n');
  });

  it('does not let an outdented marker close a fence the list still holds', () => {
    // CommonMark ends the list here and reads the outdented marker as a NEW
    // top-level opener, so releasing through it hands the renderer a fence
    // that swallows everything after it until the next marker arrives. The
    // parser reports the list and that second block as separate nodes, which
    // is how the gate knows the blank line after the marker is inside one.
    const text = '1. Step:\n\n   ```py\n   x = 1\n```\n\ntail';

    expect(visibleParagraphPrefix(text)).toBe('1. Step:\n\n');
  });

  it('keeps a blank line inside an indented code block out of the reckoning', () => {
    // Four-space code never had a rule of its own; the blank line between its
    // two halves used to be a perfectly good boundary, and cutting there split
    // one block into two.
    const text = 'intro\n\n    x = 1\n\n    y = 2\n\ntail';

    expect(visibleParagraphPrefix(text)).toBe('intro\n\n    x = 1\n\n    y = 2\n\n');
  });

  it('keeps a blank line inside display math out of the reckoning', () => {
    // The renderer reads `$$` as a block the way it reads a fence; cut inside
    // one and KaTeX is handed half an expression.
    const text = 'intro\n\n$$\na = 1\n\nb = 2\n$$\ntail';

    expect(visibleParagraphPrefix(text)).toBe('intro\n\n');
  });

  it('holds an open display-math block to its last boundary above', () => {
    expect(visibleParagraphPrefix('intro\n\n$$\na = 1\n\nb')).toBe('intro\n\n');
  });

  it('reads bracket-delimited display math the way the renderer rewrites it', () => {
    const text = 'intro\n\n\\[\na = 1\n\nb = 2\n\\]\ntail';

    expect(visibleParagraphPrefix(text)).toBe('intro\n\n');
  });

  it('takes the cap as an argument', () => {
    expect(visibleParagraphPrefix('abcdef', 4)).toBe('abcdef');
    expect(visibleParagraphPrefix('abcdef', 10)).toBe('');
  });
});
