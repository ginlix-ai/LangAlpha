import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { SceneClock, useSceneTime } from '../engine/introScenes/clock';

vi.mock('@/lib/framer', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/framer')>()),
  useReducedMotion: () => false,
}));

let frames: FrameRequestCallback[] = [];

beforeEach(() => {
  frames = [];
  vi.spyOn(window, 'requestAnimationFrame').mockImplementation((cb) => {
    frames.push(cb);
    return frames.length;
  });
  vi.spyOn(window, 'cancelAnimationFrame').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

function Probe({ seen }: { seen: Set<number> }) {
  seen.add(useSceneTime());
  return null;
}

describe('SceneClock', () => {
  it('draws on every second 60Hz vsync, even when the pair lands a hair short of 33.3ms', () => {
    const now = vi.spyOn(performance, 'now').mockReturnValue(0);
    const seen = new Set<number>();
    render(
      <SceneClock length={10000}>
        <Probe seen={seen} />
      </SceneClock>
    );
    now.mockRestore();

    for (const stamp of [16.6, 33.2, 49.9, 66.4, 83.1, 99.7, 116.3, 132.9]) {
      const tick = frames.shift()!;
      act(() => tick(stamp));
    }

    // Without the slack, 49.9 and 99.7 miss by a fraction of a millisecond and
    // the scene waits a third vsync each time: 16.6, 66.4, 116.3.
    expect([...seen].filter((t) => t > 0)).toEqual([16.6, 49.9, 83.1, 116.3]);
  });
});
