import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom';
import { GETTING_STARTED_TASKS } from '../registry';
import type { GettingStartedTaskState } from '../OnboardingProvider';

const dismiss = vi.fn();
const completeTask = vi.fn();
let mockTasks: GettingStartedTaskState[] = [];
let mockVisible = true;
vi.mock('../OnboardingProvider', () => ({
  useOnboarding: () => ({
    gettingStarted: {
      visible: mockVisible,
      tasks: mockTasks,
      doneCount: mockTasks.filter((t) => t.done).length,
      dismiss,
      completeTask,
    },
  }),
}));

const navigateToPersonalization = vi.fn(async () => {});
vi.mock('@/pages/Dashboard/hooks/useOnboarding', () => ({
  useOnboarding: () => ({ navigateToPersonalization }),
}));

import { GettingStartedCard } from '../engine/GettingStartedCard';

function LocationProbe() {
  return <span data-testid="loc">{useLocation().pathname}</span>;
}

function renderCard() {
  return render(
    <MemoryRouter initialEntries={['/settings']}>
      <GettingStartedCard />
      <Routes>
        <Route path="*" element={<LocationProbe />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('GettingStartedCard', () => {
  beforeEach(() => {
    dismiss.mockClear();
    completeTask.mockClear();
    navigateToPersonalization.mockClear();
    mockVisible = true;
    mockTasks = GETTING_STARTED_TASKS.map((def, i) => ({ def, done: i === 0 }));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('renders nothing when not visible', () => {
    mockVisible = false;
    const { container } = renderCard();
    expect(container.querySelector('aside')).toBeNull();
  });

  it('shows progress and strikes through completed tasks', () => {
    renderCard();
    expect(screen.getByText(`1 / ${GETTING_STARTED_TASKS.length}`)).toBeInTheDocument();
    const bar = screen.getByRole('progressbar');
    expect(bar).toHaveAttribute('aria-valuenow', '1');
    // done task is disabled and struck through; pending ones show descriptions
    const doneBtn = screen.getByRole('button', {
      name: /Explore the dashboard/,
    });
    expect(doneBtn).toBeDisabled();
    expect(screen.getByText('Explore the dashboard')).toHaveClass('line-through');
    expect(screen.getByText(/tell the agent the tickers you watch/)).toBeInTheDocument();
  });

  it('clicking a pending task navigates to its target', () => {
    renderCard();
    fireEvent.click(screen.getByRole('button', { name: /Run your first analysis/ }));
    expect(screen.getByTestId('loc').textContent).toBe('/chat');
  });

  it('an interview task explains the flow first, then confirm opens it — not a bare route', () => {
    mockTasks = GETTING_STARTED_TASKS.map((def) => ({ def, done: false }));
    renderCard();
    fireEvent.click(screen.getByRole('button', { name: /Share your investing preferences/ }));
    // explainer dialog, nothing launched yet
    expect(screen.getByText(/agent asks a few questions about your portfolio/)).toBeInTheDocument();
    expect(navigateToPersonalization).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Start the chat' }));
    expect(navigateToPersonalization).toHaveBeenCalledTimes(1);
    // a plain navigate to /chat/t/__default__ would bounce back to /chat
    expect(screen.getByTestId('loc').textContent).toBe('/settings');
  });

  it('declining the interview dialog launches nothing', () => {
    mockTasks = GETTING_STARTED_TASKS.map((def) => ({ def, done: false }));
    renderCard();
    fireEvent.click(screen.getByRole('button', { name: /Share your watchlist/ }));
    fireEvent.click(screen.getByRole('button', { name: 'Not now' }));
    expect(navigateToPersonalization).not.toHaveBeenCalled();
    expect(screen.queryByText(/agent asks a few questions/)).toBeNull();
  });

  it('clicking an external task stamps it done and opens a new tab, no in-app navigation', () => {
    // No 'noopener' feature string — that would make open() return null even
    // on success (reading as "blocked"). The opener is severed on the handle.
    const handle = { opener: window } as unknown as Window;
    const open = vi.spyOn(window, 'open').mockReturnValue(handle);
    renderCard();
    fireEvent.click(screen.getByRole('button', { name: /Use LangAlpha on other platforms/ }));
    expect(completeTask).toHaveBeenCalledWith('integrations');
    expect(open).toHaveBeenCalledWith(expect.stringMatching(/\/integrations$/), '_blank');
    expect(handle.opener).toBeNull();
    expect(screen.getByTestId('loc').textContent).toBe('/settings');
  });

  it('a blocked popup leaves the external task pending so the user can retry', () => {
    vi.spyOn(window, 'open').mockReturnValue(null);
    renderCard();
    fireEvent.click(screen.getByRole('button', { name: /Use LangAlpha on other platforms/ }));
    expect(completeTask).not.toHaveBeenCalled();
  });

  it('the X dismisses the card', () => {
    renderCard();
    fireEvent.click(screen.getByRole('button', { name: 'Hide guide' }));
    expect(dismiss).toHaveBeenCalledTimes(1);
  });
});
