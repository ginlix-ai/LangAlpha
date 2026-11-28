import { describe, expect, it } from 'vitest';
import { codeSpots } from '../codeSpots';

/** Lines as the code viewer draws them: each numbered, its number never part of its words. */
function drawn(lines: string[]): HTMLElement {
  const root = document.createElement('pre');
  lines.forEach((text, i) => {
    const line = document.createElement('span');
    line.setAttribute('data-line', String(i + 1));
    const number = document.createElement('span');
    number.className = 'linenumber';
    number.textContent = String(i + 1);
    line.append(number, `${text}\n`);
    root.appendChild(line);
  });
  document.body.appendChild(root);
  return root;
}

function lineOfFound(found: ReturnType<ReturnType<typeof codeSpots>['find']>): string | null {
  return found && 'range' in found ? (found.range.startContainer.parentElement?.getAttribute('data-line') ?? null) : null;
}

describe('codeSpots finds selected words again', () => {
  const SAME = ['x = total()', 'print(x)', 'x = total()', 'print(x)', 'x = total()'];

  it('by the lines they were on, among places the words around cannot tell apart', () => {
    const spots = codeSpots(drawn(SAME));
    expect(lineOfFound(spots.find({ kind: 'text', quote: 'total', lines: '3' }))).toBe('3');
    expect(lineOfFound(spots.find({ kind: 'text', quote: 'total', lines: '5' }))).toBe('5');
  });

  it('by the words around before the lines', () => {
    const spots = codeSpots(drawn(['a = total(1)', 'b = total(2)', 'c = total(3)']));
    expect(lineOfFound(spots.find({ kind: 'text', quote: 'total', lines: '1', prefix: 'c = ' }))).toBe('3');
  });

  it('reads the file afresh for a new pass of lookups, and not before', () => {
    const root = drawn(['first line']);
    const spots = codeSpots(root);
    spots.fresh?.();
    expect(spots.find({ kind: 'text', quote: 'first', lines: '1' })).not.toBeNull();
    root.querySelector('[data-line]')!.lastChild!.textContent = 'renamed line\n';
    expect(spots.find({ kind: 'text', quote: 'renamed', lines: '1' })).toBeNull();
    spots.fresh?.();
    expect(lineOfFound(spots.find({ kind: 'text', quote: 'renamed', lines: '1' }))).toBe('1');
  });
});
