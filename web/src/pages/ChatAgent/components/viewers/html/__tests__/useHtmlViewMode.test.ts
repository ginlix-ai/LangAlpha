import { describe, it, expect } from 'vitest';
import { act, renderHook } from '@testing-library/react';

import { useHtmlViewMode } from '../useHtmlViewMode';

type Request = { anchor: string | null; seq: number | null };

function renderMode(initial: Request) {
  return renderHook(({ anchor, seq }: Request) => useHtmlViewMode(anchor, seq), {
    initialProps: initial,
  });
}

describe('useHtmlViewMode', () => {
  it('shows the page until the reader asks for the source', () => {
    const { result } = renderMode({ anchor: null, seq: null });
    expect(result.current.source).toBe(false);
    act(() => result.current.setSource(true));
    expect(result.current.source).toBe(true);
  });

  // A section exists only in the rendered page, so a reference clicked while
  // reading markup has to bring the page back or it lands nowhere.
  it('returns to the page when a reference asks for a section', () => {
    const { result, rerender } = renderMode({ anchor: null, seq: null });
    act(() => result.current.setSource(true));
    rerender({ anchor: 'risks', seq: 1 });
    expect(result.current.source).toBe(false);
  });

  it('returns to the page when the same section is asked for again', () => {
    const { result, rerender } = renderMode({ anchor: 'risks', seq: 1 });
    act(() => result.current.setSource(true));
    rerender({ anchor: 'risks', seq: 2 });
    expect(result.current.source).toBe(false);
  });

  it('leaves the source up while the request it already met stays in effect', () => {
    const { result, rerender } = renderMode({ anchor: 'risks', seq: 1 });
    act(() => result.current.setSource(true));
    rerender({ anchor: 'risks', seq: 1 });
    expect(result.current.source).toBe(true);
  });

  it('keeps the source when another file opens with no section asked for', () => {
    const { result, rerender } = renderMode({ anchor: 'risks', seq: 1 });
    act(() => result.current.setSource(true));
    rerender({ anchor: null, seq: null });
    expect(result.current.source).toBe(true);
  });
});
