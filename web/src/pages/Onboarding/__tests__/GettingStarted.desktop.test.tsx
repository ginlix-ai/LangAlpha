import { describe, it, expect, vi, beforeEach, afterEach, afterAll } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { GETTING_STARTED_TASKS } from '../registry';
import type { GettingStartedTaskState } from '../OnboardingProvider';

const h = vi.hoisted(() => ({ assign: vi.fn(), completeTask: vi.fn<() => Promise<void>>() }));

vi.mock('@/lib/desktop', () => ({ isDesktopShell: () => true }));

vi.mock('../OnboardingProvider', () => {
  const tasks: GettingStartedTaskState[] = GETTING_STARTED_TASKS.map((def) => ({
    def,
    done: false,
  }));
  return {
    useOnboarding: () => ({
      gettingStarted: {
        visible: true,
        tasks,
        doneCount: 0,
        dismiss: () => {},
        completeTask: h.completeTask,
      },
    }),
  };
});

import { GettingStartedLauncher } from '../engine/GettingStarted';

/**
 * The portal is a full page load, which jsdom will not perform. `assign` is an
 * own non-configurable property of Location, so the whole object is swapped.
 */
const realLocation = window.location;
Object.defineProperty(window, 'location', {
  configurable: true,
  value: {
    href: realLocation.href,
    origin: realLocation.origin,
    assign: h.assign,
  },
});
afterAll(() => {
  Object.defineProperty(window, 'location', {
    configurable: true,
    value: realLocation,
  });
});

function pickChatApps() {
  render(
    <MemoryRouter initialEntries={['/settings']}>
      <GettingStartedLauncher variant="row" />
    </MemoryRouter>
  );
  fireEvent.click(screen.getByRole('button', { name: /Connect a chat app/ }));
}

describe('GettingStartedLauncher in the desktop app', () => {
  beforeEach(() => {
    localStorage.clear();
    vi.useFakeTimers();
    h.assign.mockClear();
    h.completeTask.mockClear();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('takes this window to the chat apps page once the task is stamped, never a popup', async () => {
    let stamped!: () => void;
    h.completeTask.mockReturnValueOnce(new Promise((resolve) => (stamped = resolve)));
    const open = vi.spyOn(window, 'open');
    pickChatApps();
    expect(open).not.toHaveBeenCalled();
    expect(h.completeTask).toHaveBeenCalledWith('integrations');
    // Nothing leaves before the stamp settles: the page would cut it off.
    await vi.advanceTimersByTimeAsync(0);
    expect(h.assign).not.toHaveBeenCalled();

    stamped();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.assign).toHaveBeenCalledTimes(1);
    expect(h.assign).toHaveBeenCalledWith(expect.stringMatching(/\/integrations$/));

    // The fallback timer finds the page already leaving.
    await vi.advanceTimersByTimeAsync(2000);
    expect(h.assign).toHaveBeenCalledTimes(1);
  });

  it('leaves anyway when the stamp never settles', async () => {
    h.completeTask.mockReturnValueOnce(new Promise(() => {}));
    pickChatApps();
    await vi.advanceTimersByTimeAsync(1499);
    expect(h.assign).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(h.assign).toHaveBeenCalledTimes(1);
  });
});
