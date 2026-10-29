/**
 * Math has to survive the whole pipeline: remark-math finds it, rehype-katex
 * renders it, and rehype-sanitize runs last, so a KaTeX markup change the
 * schema does not allow strips the output without any error.
 */
import { describe, it, expect, vi } from 'vitest';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark', setTheme: () => {} }),
}));

import Markdown from '../Markdown';

function render(content: string): HTMLElement {
  const host = document.createElement('div');
  host.innerHTML = renderToStaticMarkup(<Markdown variant="chat" content={content} />);
  return host;
}

describe('Markdown math', () => {
  it('renders inline and display math with the MathML tree intact', () => {
    const host = render('Area is $x^2$.\n\n$$\n\\int_0^1 x\\,dx\n$$\n');

    expect(host.querySelectorAll('.katex')).toHaveLength(2);
    expect(host.querySelectorAll('.katex-display')).toHaveLength(1);
    const tex = [...host.querySelectorAll('annotation[encoding="application/x-tex"]')].map((a) => a.textContent);
    expect(tex).toEqual(['x^2', '\\int_0^1 x\\,dx']);
  });

  it('reads \\[...\\] and \\(...\\) as math', () => {
    const host = render('Inline \\(a+b\\) and\n\n\\[E = mc^2\\]\n');

    const tex = [...host.querySelectorAll('annotation[encoding="application/x-tex"]')].map((a) => a.textContent);
    expect(tex).toEqual(['a+b', 'E = mc^2']);
  });

  it('gives display math on a line of its own a block, and keeps it inline in a sentence', () => {
    const host = render(
      '$$E = mc^2$$\n\n\\[a^2 + b^2 = c^2\\]\n\nMomentum is\n$$p = mv$$\nfor a point mass.\n\nThe force $$F = ma$$ is inline.\n'
    );

    const display = [...host.querySelectorAll('.katex-display annotation')].map((a) => a.textContent);
    expect(display).toEqual(['E = mc^2', 'a^2 + b^2 = c^2', 'p = mv']);
    const inline = [...host.querySelectorAll('p .katex annotation')].map((a) => a.textContent);
    expect(inline).toEqual(['F = ma']);
  });

  it('adds no empty numbered row for a trailing \\\\ in align', () => {
    const host = render('$$\n\\begin{align}\na &= b \\\\\nc &= d \\\\\n\\end{align}\n$$\n');

    expect(host.querySelectorAll('.katex-mathml mtr')).toHaveLength(2);
  });

  it('keeps a lone currency dollar as text', () => {
    const host = render('It costs $100 today.');

    expect(host.querySelector('.katex')).toBeNull();
    expect(host.textContent).toContain('$100');
  });

  // remark-math pairs any two dollars, so two cashtags in one paragraph would
  // set the words between them as a formula.
  it('keeps a dollar before a ticker as text, next to math in the same paragraph', () => {
    const host = render('$AAPL and $MSFT guided flat, $BRK.B is no peer, $AAPL/$MSFT trades at $68.3B, and the drift is $x^2$ or $\\alpha$.');

    const tex = [...host.querySelectorAll('annotation[encoding="application/x-tex"]')].map((a) => a.textContent);
    expect(tex).toEqual(['x^2', '\\alpha']);
    expect(host.textContent).toContain('$AAPL and $MSFT guided flat, $BRK.B is no peer, $AAPL/$MSFT trades at $68.3B');
  });

  // The dollar after `(` opens a formula of its own, so it does not close
  // the ticker's, and the two dollars after it pair.
  it('keeps a ticker dollar as text before math opened in parentheses or quotes', () => {
    const host = render('Compare $AAPL with ($x$), and $NVDA with "$y$".');

    const tex = [...host.querySelectorAll('annotation[encoding="application/x-tex"]')].map((a) => a.textContent);
    expect(tex).toEqual(['x', 'y']);
    expect(host.textContent).toContain('Compare $AAPL with (');
    expect(host.textContent).toContain('and $NVDA with "');
  });

  it('keeps a dollar standing for the unit as text, and math in parentheses', () => {
    const host = render('Brent averaged 85 $/bbl vs 80 $/bbl, in millions ($), with variance ($\\sigma^2$) flat.');

    const tex = [...host.querySelectorAll('annotation[encoding="application/x-tex"]')].map((a) => a.textContent);
    expect(tex).toEqual(['\\sigma^2']);
    expect(host.textContent).toContain('85 $/bbl vs 80 $/bbl, in millions ($)');
  });

  it('reads capitals a dollar closes as math', () => {
    const host = render('With $N = 5$ draws, $X$, $P(A)$ and $X_t$ hold.');

    const tex = [...host.querySelectorAll('annotation[encoding="application/x-tex"]')].map((a) => a.textContent);
    expect(tex).toEqual(['N = 5', 'X', 'P(A)', 'X_t']);
  });
});
