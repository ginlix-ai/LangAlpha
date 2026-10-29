// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { repairStreamingTail } from '../streamingTail';

const WJ = '\u2060';
const repair = (block: string) => repairStreamingTail(block).text;

describe('repairStreamingTail', () => {
  it('holds a last line that is still only block markers', () => {
    expect(repair('What moved it:\n-')).toBe('What moved it:\n');
    expect(repair('What moved it:\n- ')).toBe('What moved it:\n');
    expect(repair('What moved it:\n- **')).toBe('What moved it:\n');
    expect(repair('Title\n==')).toBe('Title\n');
    expect(repair('Steps:\n1.')).toBe('Steps:\n');
    expect(repair('1. one\n\n2')).toBe('1. one\n\n');
    expect(repair('## ')).toBe('');
    expect(repair('> - ')).toBe('');
    expect(repair('- [x')).toBe('');
    expect(repair('Intro\n``')).toBe('Intro\n');
  });

  it('shows a link being written as its label', () => {
    expect(repair('per the [Q2 FY27](https://investor.exam')).toBe(`per the ${WJ}Q2 FY27`);
    expect(repair('per the [Q2 FY27')).toBe(`per the ${WJ}Q2 FY27`);
    expect(repair('per the [Q2 FY27]')).toBe(`per the ${WJ}Q2 FY27`);
    expect(repair('per the [')).toBe('per the ');
    expect(repair('see [**bold label')).toBe(`see ${WJ}**bold label**`);
    expect(repair('saved as [out.csv](results/out.csv) here')).toBe('saved as [out.csv](results/out.csv) here');
  });

  it('holds an image or a citation until it closes', () => {
    expect(repair('see ![chart](results/ch')).toBe('see ');
    expect(repair('see !')).toBe('see ');
    expect(repair('notes (')).toBe('notes ');
    expect(repair('notes ([reuters.com](https://www.reu')).toBe('notes ');
    expect(repair('notes ([reuters.com](https://www.reuters.com/x)')).toBe('notes ');
    // A parenthesized link to a file is no citation.
    expect(repair('(see [note](results/no')).toBe(`(see ${WJ}note`);
  });

  it('closes emphasis and code spans the line leaves open', () => {
    expect(repair('**bold')).toBe('**bold**');
    expect(repair('**bold*')).toBe('**bold**');
    expect(repair('**bold ')).toBe('**bold**');
    expect(repair('a *b* and **c')).toBe('a *b* and **c**');
    expect(repair('so *price is ~~not')).toBe('so *price is ~~not~~*');
    expect(repair('这是**重点')).toBe('这是**重点**');
    expect(repair('run `margin_')).toBe('run `margin_`');
    expect(repair('**see `foo')).toBe('**see `foo`**');
    expect(repair('a `')).toBe('a ');
    expect(repair('a ~')).toBe('a ');
  });

  it('leaves alone what only looks like emphasis', () => {
    expect(repair('2*3')).toBe('2*3');
    expect(repair('snake_case_name')).toBe('snake_case_name');
    expect(repair('see https://example.com/a_b*c')).toBe('see https://example.com/a_b*c');
    expect(repair('`a*b` and \\*c')).toBe('`a*b` and \\*c');
    // A finished line keeps what it has: an open `**` there may be literal.
    expect(repair('a **literal\nnext')).toBe('a **literal\nnext');
  });

  it('holds math until it closes', () => {
    expect(repair('the drift is $\\Delta m = m_')).toBe('the drift is ');
    expect(repair('cost \\$5 and $x^2$ then $y')).toBe('cost \\$5 and $x^2$ then ');
    expect(repair('inline \\(a +')).toBe('inline ');
    expect(repair('Sum:\n$$\n\\Delta_{4} = \\sum')).toBe('Sum:\n');
    expect(repair('$$\nx\n$$')).toBe('$$\nx\n$$');
  });

  // A paragraph is usually one line, so a stray dollar held to the line's end
  // stalls the rest of the paragraph.
  it('shows a dollar its line has left open once it has run on', () => {
    const stray = 'EPS of $(0.12) missed while revenue grew 4% on volume and the margin held at 41';
    expect(repair(stray)).toBe(stray);
    expect(repair('EPS of $(0.12) missed')).toBe('EPS of ');
    const formula = `a $${'x_i + '.repeat(12)}y$ here`;
    expect(repair(formula)).toBe(formula);
  });

  it('draws a table from its header row and lands each row whole', () => {
    expect(repair('| a | b |\n')).toBe('| a | b |\n|---|---|');
    expect(repair('Here:\n| a | b | c |\n|:---|--')).toBe('Here:\n| a | b | c |\n|:---|--|---|');
    expect(repair('| a | b |\n|---|---|\n| 1 | 2')).toBe('| a | b |\n|---|---|\n');
    expect(repair('| a | b |\n|---|---|\n| 1 | 2 |\n')).toBe('| a | b |\n|---|---|\n| 1 | 2 |\n');
    expect(repair('Intro\n| a')).toBe('Intro\n');
  });

  it('holds a fence line still being typed, and reads nothing inside a fence', () => {
    expect(repair('Code:\n```pyth')).toBe('Code:\n');
    expect(repair('```py\nx = 1\n``')).toBe('```py\nx = 1\n');
    expect(repair('```py\nx = **1')).toBe('```py\nx = **1');
    expect(repair('```py\nx = 1\n```')).toBe('```py\nx = 1\n```');
  });

  it('holds a tag being typed and reads past a finished one', () => {
    expect(repair('press <kb')).toBe('press ');
    expect(repair('press <')).toBe('press ');
    expect(repair('see <cite-bubble label="a" href="https://a.b/c_d"></cite-bubble> and **b')).toBe(
      'see <cite-bubble label="a" href="https://a.b/c_d"></cite-bubble> and **b**',
    );
  });

  // The reveal fade and the live fence hold offsets into the block as written.
  it('keeps every character it shows at its offset, and only appends after them', () => {
    const doc = [
      '## Margins: **mix** explains it',
      '',
      'What moved it:',
      '- **Yields** cost 150 bp, per the [CFO commentary](https://example.com/q2_cfo) and ([reuters.com](https://www.reuters.com/x)).',
      '- `margin_table` shows $x^2$ and *price ~~is~~ not* it.',
      '',
      '| Driver | Effect |',
      '|:---|---:|',
      '| Mix | +40 bp |',
      '',
      '$$',
      '\\Delta = m_t - m_{t-4}',
      '$$',
      '',
      '```python',
      'print(1)',
      '```',
    ].join('\n');
    for (let end = 0; end <= doc.length; end++) {
      const block = doc.slice(0, end);
      const { text, shown } = repairStreamingTail(block);
      expect(shown, JSON.stringify(block)).toBeLessThanOrEqual(block.length);
      const visible = text.slice(0, shown);
      for (let i = 0; i < shown; i++) {
        const ok = visible[i] === block[i] || (visible[i] === WJ && block[i] === '[');
        expect(ok, `${JSON.stringify(block)} at ${i}`).toBe(true);
      }
    }
  });
});
