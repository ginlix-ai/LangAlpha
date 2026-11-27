import { useRef } from 'react';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { act, render } from '@testing-library/react';
import { scrollMemory, useScrollMemory } from '../scrollMemory';

/** A port drawn only once `ready`, as a shell behind a loading state draws its column. */
function Port({ ready, memoryKey }: { ready: boolean; memoryKey: string }) {
  const ref = useRef<HTMLDivElement>(null);
  useScrollMemory(ref, memoryKey);
  return ready ? <div ref={ref} data-testid="port" /> : <p>loading</p>;
}

/** jsdom keeps no scroll offset, so give the port one it can write and read back. */
function scrollable(el: HTMLElement) {
  let top = 0;
  Object.defineProperty(el, 'scrollTop', { configurable: true, get: () => top, set: (v: number) => { top = v; } });
  return el;
}

afterEach(() => scrollMemory.clear());

describe('useScrollMemory', () => {
  it('saves the offset of a port that mounts after the hook first ran', () => {
    const view = render(<Port ready={false} memoryKey="route:/dashboard" />);
    view.rerender(<Port ready memoryKey="route:/dashboard" />);
    const port = scrollable(view.getByTestId('port'));
    act(() => {
      port.scrollTop = 420;
      port.dispatchEvent(new Event('scroll'));
    });
    expect(scrollMemory.get('route:/dashboard')).toBe(420);
  });

  it('restores the saved offset into a port that mounts late', () => {
    scrollMemory.set('route:/dashboard', 300);
    const view = render(<Port ready={false} memoryKey="route:/dashboard" />);
    // The restore writes in the commit that mounts the port, before a test
    // could reach the element, so watch the writes on every element.
    const written: number[] = [];
    const setter = vi.spyOn(Element.prototype, 'scrollTop', 'set').mockImplementation((v: number) => { written.push(v); });
    try {
      view.rerender(<Port ready memoryKey="route:/dashboard" />);
      expect(written).toContain(300);
    } finally {
      setter.mockRestore();
    }
  });

  it('keeps the saved offset while a short page holds the restore clamped', () => {
    scrollMemory.set('route:/news', 300);
    // Every port here holds at most 50px of scroll, as a page still loading does.
    const tops = new WeakMap<Element, number>();
    const getter = vi.spyOn(Element.prototype, 'scrollTop', 'get').mockImplementation(function (this: Element) {
      return tops.get(this) ?? 0;
    });
    const setter = vi.spyOn(Element.prototype, 'scrollTop', 'set').mockImplementation(function (this: Element, v: number) {
      tops.set(this, Math.max(0, Math.min(v, 50)));
    });
    try {
      const view = render(<Port ready={false} memoryKey="route:/news" />);
      view.rerender(<Port ready memoryKey="route:/news" />);
      const port = view.getByTestId('port');
      expect(port.scrollTop).toBe(50);
      port.dispatchEvent(new Event('scroll'));
      expect(scrollMemory.get('route:/news')).toBe(300);

      // The reader's own scroll is saved as ever, and ends the restore, so
      // coming back to the offset it last wrote is saved too.
      port.scrollTop = 20;
      port.dispatchEvent(new Event('scroll'));
      expect(scrollMemory.get('route:/news')).toBe(20);
      port.scrollTop = 50;
      port.dispatchEvent(new Event('scroll'));
      expect(scrollMemory.get('route:/news')).toBe(50);
    } finally {
      getter.mockRestore();
      setter.mockRestore();
    }
  });

  it('stops saving under a key once the port goes', () => {
    const view = render(<Port ready memoryKey="route:/a" />);
    const port = scrollable(view.getByTestId('port'));
    view.rerender(<Port ready={false} memoryKey="route:/a" />);
    port.scrollTop = 90;
    port.dispatchEvent(new Event('scroll'));
    expect(scrollMemory.get('route:/a')).toBeUndefined();
  });
});
