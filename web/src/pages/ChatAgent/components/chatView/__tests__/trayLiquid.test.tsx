/**
 * The liquid owns when a fold's run is over: it draws one shape per child of
 * the row it measured, the Status button among them, and says it settled when
 * the shape set off last lands, whatever the tray's contents count.
 */
import { describe, it, expect, vi } from 'vitest';
import { useEffect, type ComponentProps } from 'react';
import { render } from '@testing-library/react';

vi.mock('@/lib/framer', async (importOriginal) => {
  const framer = await importOriginal<typeof import('@/lib/framer')>();
  // Each shape lands as soon as it mounts, so the test reads which one reports it.
  function Shape({ onAnimationComplete, transition, ...rest }: ComponentProps<typeof framer.motion.span>) {
    useEffect(() => {
      onAnimationComplete?.({});
    }, [onAnimationComplete]);
    const { initial: _initial, animate: _animate, ...attrs } = rest;
    return <span data-delay={(transition as { delay: number }).delay} {...(attrs as ComponentProps<'span'>)} />;
  }
  return { ...framer, motion: { ...framer.motion, span: Shape } };
});

import { TrayLiquid } from '../trayLiquid';

function boxed(rect: { left: number; top: number; width: number; height: number }): HTMLElement {
  const el = document.createElement('span');
  el.getBoundingClientRect = () => DOMRect.fromRect({ x: rect.left, y: rect.top, width: rect.width, height: rect.height });
  return el;
}

describe('TrayLiquid', () => {
  it('settles once, from the shape set off last, for every child of the row', () => {
    const tray = boxed({ left: 0, top: 0, width: 400, height: 28 });
    const chips = document.createElement('div');
    // Two chips and the Status button; the dock sits under the first chip.
    chips.append(
      boxed({ left: 16, top: 4, width: 80, height: 20 }),
      boxed({ left: 102, top: 4, width: 120, height: 20 }),
      boxed({ left: 228, top: 4, width: 26, height: 20 }),
    );
    const dock = boxed({ left: 20, top: 60, width: 40, height: 24 });
    const onSettled = vi.fn();
    const { container } = render(
      <TrayLiquid
        phase="folding"
        trayRef={{ current: tray }}
        chipsRef={{ current: chips }}
        dockRef={{ current: dock }}
        onSettled={onSettled}
      />,
    );
    const delays = [...container.querySelectorAll('[data-delay]')].map((el) => Number(el.getAttribute('data-delay')));
    expect(delays).toHaveLength(3);
    expect(onSettled).toHaveBeenCalledTimes(1);
    expect(onSettled).toHaveBeenCalledWith('folding');
    // The Status button, furthest from the dock, is the one that waits longest.
    expect(Math.max(...delays)).toBe(delays[2]);
  });

  it('draws nothing and says nothing when there is nothing to measure', () => {
    const onSettled = vi.fn();
    const { container } = render(
      <TrayLiquid phase="unfolding" trayRef={{ current: null }} chipsRef={{ current: null }} dockRef={{ current: null }} onSettled={onSettled} />,
    );
    expect(container.querySelector('[data-delay]')).toBeNull();
    // The fold's own fallback ends this run (`LIQUID_FALLBACK_MS`).
    expect(onSettled).not.toHaveBeenCalled();
  });
});
