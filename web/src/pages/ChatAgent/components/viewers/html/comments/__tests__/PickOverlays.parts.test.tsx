/**
 * The outlines of a draft's parts leave with the box over them: kept where
 * they were and marked gone, so they fade on the exit beat, rather than
 * unmounting the frame the box starts its fade. A fresh show is new outlines.
 */
import { render } from '@testing-library/react';
import type React from 'react';
import { describe, expect, it } from 'vitest';
import { PickOverlays } from '../PickOverlays';

const PART = { x: 40, y: 60, width: 120, height: 80 };

function overlays(parts: React.ComponentProps<typeof PickOverlays>['parts']) {
  return (
    <PickOverlays
      mode
      boxOpen={parts !== null}
      hover={null}
      areaDrag={null}
      picked={null}
      area={null}
      parts={parts}
      scrolling={false}
      selection={null}
      size={{ width: 800, height: 600 }}
      drafting={parts !== null}
      onComment={() => {}}
    />
  );
}

describe('the parts\' outlines', () => {
  it('stay where they were, marked gone, once the parts go', () => {
    const { container, rerender } = render(overlays({ rects: [PART], tone: 'picked' }));
    const shown = container.querySelector<HTMLElement>('.page-comments-part')!;
    expect(shown.hasAttribute('data-gone')).toBe(false);

    rerender(overlays(null));
    const gone = container.querySelector<HTMLElement>('.page-comments-part')!;
    expect(gone).toBe(shown);
    expect(gone.hasAttribute('data-gone')).toBe(true);
    expect(gone.style.transform).toBe(shown.style.transform);

    // Shown again, they are new outlines that fade in where they are.
    rerender(overlays({ rects: [PART], tone: 'picked' }));
    const again = container.querySelector<HTMLElement>('.page-comments-part')!;
    expect(again).not.toBe(shown);
    expect(again.hasAttribute('data-gone')).toBe(false);
  });
});
