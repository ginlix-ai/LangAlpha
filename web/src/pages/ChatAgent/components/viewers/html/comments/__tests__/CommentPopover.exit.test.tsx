/**
 * A box that leaves plays its exit where it stood. The layer stops naming its
 * pin and the band it kept clear the moment it is no longer current; a box
 * that kept the pin but not the band went back beside its pin for its exit,
 * leaping across the page from under the words it was written about.
 */
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { render, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { DURATION } from '@/lib/motion';
import { CommentPopover, type CommentPopoverProps } from '../CommentPopover';

type Fade = { from: number; to: number; duration?: number; set: (v: number) => void; finish: () => void };
const framer = vi.hoisted(() => ({ fades: [] as Fade[] }));
vi.mock('@/lib/framer', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  animate: (from: number, to: number, opts: { duration?: number; onUpdate: (v: number) => void }) => {
    let finish = () => {};
    const done = new Promise<void>((resolve) => { finish = resolve; });
    framer.fades.push({ from, to, duration: opts.duration, set: opts.onUpdate, finish });
    return { stop: () => {}, then: (go: () => void) => done.then(go) };
  },
}));

function boundary(): HTMLElement {
  const el = document.createElement('div');
  Object.defineProperty(el, 'clientWidth', { value: 900 });
  Object.defineProperty(el, 'clientHeight', { value: 700 });
  document.body.append(el);
  return el;
}

function box(props: Partial<CommentPopoverProps> & Pick<CommentPopoverProps, 'boundary'>) {
  return (
    <CommentPopover
      open
      point={{ x: 300, y: 120 }}
      clear={{ top: 100, bottom: 140 }}
      narrow={false}
      width={320}
      label="Comment"
      focus="field"
      arrival={false}
      replaced={false}
      onDismiss={() => {}}
      onEscape={() => {}}
      onGone={() => {}}
      {...props}
    >
      <textarea aria-label="words" />
    </CommentPopover>
  );
}

const anchorStyle = (container: HTMLElement) => {
  const anchor = container.querySelector<HTMLElement>('.page-comments-anchor')!;
  return { top: anchor.style.top, height: anchor.style.height, left: anchor.style.left };
};

describe('a box leaving', () => {
  it('stays placed against the band it kept clear', () => {
    const edge = boundary();
    const { container, rerender } = render(box({ boundary: edge }));
    const open = anchorStyle(container);
    // Placed against the band: the anchor spans it.
    expect(open).toEqual({ top: '100px', height: '40px', left: '300px' });

    // No longer current: the layer names neither its pin nor its band.
    rerender(box({ boundary: edge, open: false, point: null, clear: null }));
    expect(anchorStyle(container)).toEqual(open);
  });

  it('fades where it stands on the clock its pin and marks leave on, never shrinking toward a corner', async () => {
    const edge = boundary();
    framer.fades.length = 0;
    const onGone = vi.fn();
    const { rerender } = render(box({ boundary: edge, onGone }));
    expect(framer.fades).toHaveLength(0);
    const el = document.querySelector<HTMLElement>('.page-comment-box')!;
    rerender(box({ boundary: edge, onGone, open: false, point: null, clear: null }));
    // Held mounted while it fades, and let go once the fade has played.
    expect(el.isConnected).toBe(true);
    expect(framer.fades).toHaveLength(1);
    expect(framer.fades[0]).toMatchObject({ from: 1, to: 0, duration: DURATION.exit });
    framer.fades[0].set(0.4);
    expect(el.style.opacity).toBe('0.4');
    expect(onGone).not.toHaveBeenCalled();
    framer.fades[0].finish();
    await waitFor(() => expect(onGone).toHaveBeenCalledTimes(1));

    // No exit of the stylesheet's own: no pop out, and no transition easing each frame again.
    const css = readFileSync(resolve(__dirname, '../CommentLayer.css'), 'utf8').replace(/\/\*[\s\S]*?\*\//g, '');
    const closed = /\.page-comment-box\.pop-in\[data-state='closed'\]\s*\{([^}]*)\}/.exec(css)?.[1] ?? '';
    expect(closed).toMatch(/animation:\s*none/);
    expect(closed).toMatch(/transition:\s*none/);
  });
});
