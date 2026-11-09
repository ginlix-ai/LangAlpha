/**
 * A token's detail line in a usage table (`UsageDetail`): which part gives
 * way when the line is short of room.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { UsageDetail } from '../PlanUsage';

vi.mock('@/hooks/useClaudeCodePlan', () => ({ usePlanClock: () => ({ now: 0, locale: 'en-US' }) }));

const TEXT = 'Uses its own token';
// Each character 10px wide; the line 200px, the tail its full 300px.
const LINE = 200;
const TAIL = 300;

describe('UsageDetail', () => {
  beforeEach(() => {
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockImplementation(
      () => ({ font: '', letterSpacing: '0px', measureText: (s: string) => ({ width: s.length * 10 }) }) as unknown as CanvasRenderingContext2D,
    );
    vi.spyOn(Element.prototype, 'getBoundingClientRect').mockImplementation(function (this: Element) {
      return { width: this.hasAttribute('data-usage-line') ? LINE : TAIL } as DOMRect;
    });
  });
  afterEach(() => vi.restoreAllMocks());

  it('keeps whose token it is whole and lets the pending note give way', () => {
    render(<UsageDetail text={TEXT} plan={null} tail="pending" />);
    const text = screen.getByText(TEXT, { selector: 'span:not([hidden])' });
    expect(text).not.toHaveAttribute('title');
    expect(text).toHaveClass('flex-none');
    expect(text.nextElementSibling).toHaveClass('min-w-0', 'overflow-hidden');
  });

  it('cuts the text where it alone outgrows the line', () => {
    render(<UsageDetail text={`${TEXT} · user@example.com`} plan={null} tail="pending" />);
    const text = screen.getByTitle(`${TEXT} · user@example.com`);
    expect(text.textContent).toMatch(/…$/);
    expect(text.textContent!.length * 10).toBeLessThanOrEqual(LINE);
  });
});
