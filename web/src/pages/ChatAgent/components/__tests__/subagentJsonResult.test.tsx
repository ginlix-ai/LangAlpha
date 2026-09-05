import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ThemeProvider } from '@/contexts/ThemeContext';
import Markdown from '../Markdown';
import StructuredResultBlock from '../messageList/StructuredResultBlock';
import { parseResultPreview, parseStructuredResult } from '../../utils/structuredResult';

/**
 * A schema-constrained subagent's answer: one JSON object whose string values
 * carry prose, currency figures and inline citations. Each of those used to be
 * rewritten inside the code fence, which corrupted the block on screen and made
 * the Copy button return text that no longer parsed.
 */
const PAYLOAD = [
  '```json',
  '{',
  '  "summary": "Acme Corp reported revenue of $90.0 billion, up 18% YoY ([example.com](https://example.com/report)).",',
  '  "risks": [',
  '    "Capex ran to about $116B ([research.example](https://research.example/a))."',
  '  ]',
  '}',
  '```',
].join('\n');

function renderWithTheme(ui: React.ReactElement) {
  return render(<ThemeProvider>{ui}</ThemeProvider>);
}

describe('fenced JSON in a subagent answer', () => {
  it('keeps currency and citation syntax verbatim inside the fence', () => {
    const { container } = renderWithTheme(<Markdown variant="chat" content={PAYLOAD} />);
    const text = container.textContent ?? '';
    expect(text).toContain('$90.0 billion');
    // The KaTeX guard and the citation transform must not reach into code.
    expect(text).not.toContain('\\$');
    expect(text).not.toContain('cite-bubble');
    expect(text).toContain('([example.com](https://example.com/report))');
  });

  it('leaves the block parseable, which is what Copy hands back', () => {
    const { container } = renderWithTheme(<Markdown variant="chat" content={PAYLOAD} />);
    const code = container.querySelector('pre')?.textContent ?? '';
    expect(code).not.toBe('');
    expect(() => JSON.parse(code)).not.toThrow();
  });
});

describe('StructuredResultBlock', () => {
  it('renders each schema field under a humanized label', () => {
    const result = parseStructuredResult(PAYLOAD);
    expect(result).not.toBeNull();
    renderWithTheme(<StructuredResultBlock result={result!} />);
    expect(screen.getByText('Summary')).toBeInTheDocument();
    expect(screen.getByText('Risks')).toBeInTheDocument();
  });

  it('renders inline citations as pills rather than literal tags', () => {
    const result = parseStructuredResult(PAYLOAD)!;
    renderWithTheme(<StructuredResultBlock result={result} />);
    // CitationBubble labels itself "Source: <name>" with the TLD dropped.
    expect(screen.getByLabelText('Source: example')).toBeInTheDocument();
    expect(screen.getByLabelText('Source: research')).toBeInTheDocument();
    expect(document.body.textContent).not.toContain('cite-bubble');
  });

  it('keeps the array field as a list', () => {
    const result = parseStructuredResult(PAYLOAD)!;
    const { container } = renderWithTheme(<StructuredResultBlock result={result} />);
    expect(container.querySelectorAll('li')).toHaveLength(1);
  });

  it('flags a partial result', () => {
    const clipped =
      '{"briefs": {"AAPL": {"summary": "Acme reported $111.2B revenue."}, ' +
      '"MSFT": {"summary": "cut\n... [truncated]';
    const result = parseResultPreview(clipped);
    expect(result?.truncated).toBe(true);
    renderWithTheme(<StructuredResultBlock result={result!} collapsedMaxHeight={320} />);
    expect(screen.getByTestId('structured-result-truncated')).toBeInTheDocument();
  });

  it('offers expansion only once the fields overflow the bound', () => {
    const result = parseStructuredResult(PAYLOAD)!;
    // jsdom lays nothing out, so the overflow probe needs a height to read.
    const height = vi
      .spyOn(Element.prototype, 'scrollHeight', 'get')
      .mockReturnValue(999);
    renderWithTheme(<StructuredResultBlock result={result} collapsedMaxHeight={320} />);
    expect(screen.getByRole('button', { name: /show all/i })).toBeInTheDocument();
    height.mockRestore();
  });

  it('leaves a result that fits unbounded and unflagged', () => {
    const result = parseStructuredResult(PAYLOAD)!;
    // Heights read as 0 here, which is the "fits" case.
    renderWithTheme(<StructuredResultBlock result={result} collapsedMaxHeight={320} />);
    expect(screen.queryByRole('button', { name: /show all/i })).toBeNull();
    expect(screen.queryByTestId('structured-result-truncated')).toBeNull();
  });

  it('dumps the fields past the breadth bound instead of mounting each one', () => {
    const wide = Object.fromEntries(
      Array.from({ length: 62 }, (_, i) => [`field_${i}`, `value ${i}`])
    );
    const result = parseStructuredResult(JSON.stringify(wide))!;
    renderWithTheme(<StructuredResultBlock result={result} />);
    expect(screen.getByText('Field 49')).toBeInTheDocument();
    expect(screen.queryByText('Field 50')).toBeNull();
    const overflow = screen.getByTestId('structured-result-overflow');
    expect(overflow.textContent).toContain('+12 more');
    expect(overflow.querySelector('pre')).not.toBeNull();
  });

  it('bounds a wide array the same way', () => {
    const result = parseStructuredResult(
      JSON.stringify({ items: Array.from({ length: 55 }, (_, i) => `item ${i}`) })
    )!;
    const { container } = renderWithTheme(<StructuredResultBlock result={result} />);
    expect(container.querySelectorAll('li')).toHaveLength(50);
    expect(screen.getByTestId('structured-result-overflow').textContent).toContain(
      '+5 more'
    );
  });

  it('hides the original object behind a disclosure', () => {
    const result = parseStructuredResult(PAYLOAD)!;
    const { container } = renderWithTheme(<StructuredResultBlock result={result} />);
    expect(container.querySelector('pre')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: /show raw json/i }));
    expect(container.querySelector('pre')?.textContent ?? '').toContain('"summary"');
  });
});
