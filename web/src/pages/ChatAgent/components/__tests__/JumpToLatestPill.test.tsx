import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import JumpToLatestPill from '../JumpToLatestPill';

// Compact icon-circle affordance: no visible label — the accessible name lives
// in aria-label; the only visible text is the new-message count.
describe('JumpToLatestPill', () => {
  it('renders nothing when not visible', () => {
    const { container } = render(<JumpToLatestPill visible={false} hasNew={false} onJump={() => {}} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders icon-only (no visible text) when visible without new messages', () => {
    render(<JumpToLatestPill visible hasNew={false} onJump={() => {}} />);
    expect(screen.getByRole('button').textContent).toBe('');
  });

  it('shows the new-message count when hasNew', () => {
    render(<JumpToLatestPill visible hasNew newCount={3} onJump={() => {}} />);
    expect(screen.getByRole('button')).toHaveTextContent('3');
  });

  it('stays icon-only when hasNew but count is 0', () => {
    render(<JumpToLatestPill visible hasNew newCount={0} onJump={() => {}} />);
    expect(screen.getByRole('button').textContent).toBe('');
  });

  it('calls onJump when clicked', () => {
    const onJump = vi.fn();
    render(<JumpToLatestPill visible hasNew={false} onJump={onJump} />);
    fireEvent.click(screen.getByRole('button'));
    expect(onJump).toHaveBeenCalledTimes(1);
  });

  it('exposes an accessible label', () => {
    render(<JumpToLatestPill visible hasNew={false} onJump={() => {}} />);
    expect(screen.getByRole('button')).toHaveAttribute('aria-label');
  });
});
