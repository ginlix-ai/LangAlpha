import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { BrandMark } from '../BrandMark';

describe('BrandMark', () => {
  it('shows the monogram when we have no art for the vendor', () => {
    const { container } = render(<BrandMark name="notion" />);
    expect(container.querySelector('img')).toBeNull();
    expect(screen.getByText('N')).toBeTruthy();
  });

  it('draws the kind glyph, not a monogram, for a thing that is not a brand', () => {
    const { container } = render(<BrandMark name="pdf-report" kind="skill" />);
    expect(container.querySelector('svg')).toBeTruthy();
    expect(screen.queryByText('P')).toBeNull();
  });

  it('lands on the kind glyph when a declared logo fails to load', () => {
    const { container } = render(
      <BrandMark name="yfinance" kind="plugin" art={{ src: '/icon/yfinance' }} />,
    );
    fireEvent.error(container.querySelector('img')!);
    expect(container.querySelector('img')).toBeNull();
    expect(container.querySelector('svg')).toBeTruthy();
    expect(screen.queryByText('Y')).toBeNull();
  });

  it('draws the art when there is some', () => {
    const { container } = render(
      <BrandMark name="robinhood" art={{ src: '/icon/robinhood' }} />,
    );
    expect(container.querySelector('img')?.getAttribute('src')).toBe('/icon/robinhood');
  });

  it('falls back to the monogram when the art fails to load', () => {
    // A vendor with no usable mark 404s, which is the common case rather than
    // an error — the row must land on the identity it would have had anyway.
    const { container } = render(
      <BrandMark name="robinhood" art={{ src: '/icon/robinhood' }} />,
    );
    fireEvent.error(container.querySelector('img')!);
    expect(container.querySelector('img')).toBeNull();
    expect(screen.getByText('R')).toBeTruthy();
  });

  it('re-arms when the art changes, rather than inheriting the last failure', () => {
    const { container, rerender } = render(
      <BrandMark name="robinhood" art={{ src: '/icon/old' }} />,
    );
    fireEvent.error(container.querySelector('img')!);
    rerender(<BrandMark name="robinhood" art={{ src: '/icon/new' }} />);
    expect(container.querySelector('img')?.getAttribute('src')).toBe('/icon/new');
  });

  it('beds a transparent glyph on white so it survives a dark surface', () => {
    const { container } = render(
      <BrandMark name="ollama" art={{ src: '/ollama.png', padded: true }} />,
    );
    expect(container.querySelector('img')?.className).toContain('bg-white');
  });

  it('is decorative in both states, so the row keeps its own accessible name', () => {
    const withArt = render(<BrandMark name="ibkr" art={{ src: '/icon/ibkr' }} />);
    expect(withArt.container.querySelector('img')?.getAttribute('alt')).toBe('');
    expect(withArt.container.querySelector('[aria-hidden="true"]')).toBeTruthy();

    const without = render(<BrandMark name="ibkr" />);
    expect(without.container.querySelector('[aria-hidden="true"]')).toBeTruthy();
  });
});
