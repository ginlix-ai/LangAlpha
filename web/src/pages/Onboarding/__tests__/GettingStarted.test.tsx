import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router';
import { GETTING_STARTED_TASKS } from '../registry';
import type { GettingStartedTaskState } from '../OnboardingProvider';

const dismiss = vi.fn();
const completeTask = vi.fn();
let mockTasks: GettingStartedTaskState[] = [];
vi.mock('../OnboardingProvider', () => ({
  useOnboarding: () => ({
    gettingStarted: {
      visible: true,
      tasks: mockTasks,
      doneCount: mockTasks.filter((t) => t.done).length,
      dismiss,
      completeTask,
    },
  }),
}));

import { GettingStartedLauncher } from '../engine/GettingStarted';

const TOTAL = GETTING_STARTED_TASKS.length;

function LocationProbe() {
  return <span data-testid="loc">{useLocation().pathname}</span>;
}

function renderLauncher(variant: 'row' | 'rail' = 'row', onExpand = vi.fn()) {
  return render(
    <MemoryRouter initialEntries={['/settings']}>
      {variant === 'row' ? (
        <GettingStartedLauncher variant="row" />
      ) : (
        <GettingStartedLauncher variant="rail" onExpand={onExpand} />
      )}
      <Routes>
        <Route path="*" element={<LocationProbe />} />
      </Routes>
    </MemoryRouter>
  );
}

const header = () => screen.getByRole('button', { name: `Get started, 1 of ${TOTAL} done` });

describe('GettingStartedLauncher', () => {
  beforeEach(() => {
    localStorage.clear();
    dismiss.mockClear();
    completeTask.mockClear();
    mockTasks = GETTING_STARTED_TASKS.map((def, i) => ({ def, done: i === 0 }));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('sits in the sidebar unfolded, marks the done tasks, and describes only the next one', () => {
    renderLauncher();
    expect(header()).toHaveAttribute('aria-expanded', 'true');
    expect(header()).toHaveTextContent(`1/${TOTAL}`);
    expect(screen.getByRole('button', { name: /Explore the dashboard/ })).toBeDisabled();
    const next = screen.getByRole('button', { name: /Check live markets/ });
    expect(next).toHaveTextContent('Streaming charts for any symbol.');
    expect(screen.queryByText(/Risk, markets, style, and how the agent works with you/)).toBeNull();
  });

  it('folds away from its header and stays folded', () => {
    const { unmount } = renderLauncher();
    fireEvent.click(header());
    expect(header()).toHaveAttribute('aria-expanded', 'false');
    expect(screen.queryByRole('button', { name: /Check live markets/ })).toBeNull();
    unmount();
    renderLauncher();
    expect(header()).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(header());
    expect(screen.getByRole('button', { name: /Check live markets/ })).toBeInTheDocument();
  });

  it('the rail ring opens the sidebar on the list, focused on the next task', () => {
    localStorage.setItem('onboarding.guideOpen', '0');
    const onExpand = vi.fn();
    const { unmount } = renderLauncher('rail', onExpand);
    fireEvent.click(header());
    expect(onExpand).toHaveBeenCalledTimes(1);
    // The sidebar swaps the rail for the panel, which draws the section.
    unmount();
    renderLauncher();
    expect(header()).toHaveAttribute('aria-expanded', 'true');
    expect(screen.getByRole('button', { name: /Check live markets/ })).toHaveFocus();
  });

  it('clicking a pending task navigates to its target', () => {
    renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: /Run your first analysis/ }));
    expect(screen.getByTestId('loc').textContent).toBe('/chat');
  });

  it('the profile task opens the welcome flow', () => {
    renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: /Set up your investing profile/ }));
    expect(screen.getByTestId('loc').textContent).toBe('/welcome');
    expect(completeTask).not.toHaveBeenCalled();
  });

  it('in the browser an external task stamps it done and opens a new tab, no in-app navigation', () => {
    // No 'noopener' feature string — that would make open() return null even
    // on success (reading as "blocked"). The opener is severed on the handle.
    const handle = { opener: window } as unknown as Window;
    const open = vi.spyOn(window, 'open').mockReturnValue(handle);
    renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: /Connect a chat app/ }));
    expect(completeTask).toHaveBeenCalledWith('integrations');
    expect(open).toHaveBeenCalledWith(expect.stringMatching(/\/integrations$/), '_blank');
    expect(handle.opener).toBeNull();
    expect(screen.getByTestId('loc').textContent).toBe('/settings');
  });

  it('a blocked popup leaves the external task pending so the user can retry', () => {
    vi.spyOn(window, 'open').mockReturnValue(null);
    renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: /Connect a chat app/ }));
    expect(completeTask).not.toHaveBeenCalled();
  });

  it('"Hide guide" dismisses it for good', () => {
    renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: 'Hide guide' }));
    expect(dismiss).toHaveBeenCalledTimes(1);
  });
});
