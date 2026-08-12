import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import MarketWatchChip from '../MarketWatchChip';

// i18n in tests returns the defaultValue (with {{symbols}} interpolation), so
// assert on the stable symbol substring rather than exact translated copy.
describe('MarketWatchChip', () => {
  it('renders nothing when symbols is undefined', () => {
    const { container } = render(<MarketWatchChip />);
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing when symbols is null', () => {
    const { container } = render(<MarketWatchChip symbols={null} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders nothing when symbols is an empty list', () => {
    const { container } = render(<MarketWatchChip symbols={[]} />);
    expect(container.firstChild).toBeNull();
  });

  it('renders the watched symbols joined by ", " when non-empty', () => {
    render(<MarketWatchChip symbols={['NVDA', 'TSLA']} />);
    expect(screen.getByText(/NVDA, TSLA/)).toBeInTheDocument();
  });

  it('exposes a title tooltip', () => {
    const { container } = render(<MarketWatchChip symbols={['NVDA']} />);
    const chip = container.firstChild as HTMLElement;
    expect(chip).toHaveAttribute('title');
  });

  it('renders a single symbol without a trailing separator', () => {
    render(<MarketWatchChip symbols={['AAPL']} />);
    const node = screen.getByText(/AAPL/);
    expect(node.textContent).not.toMatch(/AAPL,/);
  });

  it('is not a button when onClick is omitted', () => {
    render(<MarketWatchChip symbols={['NVDA', 'TSLA']} />);
    expect(screen.queryByRole('button')).not.toBeInTheDocument();
  });

  it('renders a button and fires the handler on click when onClick is provided', () => {
    const onClick = vi.fn();
    render(<MarketWatchChip symbols={['NVDA', 'TSLA']} onClick={onClick} />);
    const button = screen.getByRole('button');
    expect(button).toHaveAttribute('type', 'button');
    // The symbols still render inside the clickable chip.
    expect(screen.getByText(/NVDA, TSLA/)).toBeInTheDocument();
    fireEvent.click(button);
    expect(onClick).toHaveBeenCalledTimes(1);
  });
});
