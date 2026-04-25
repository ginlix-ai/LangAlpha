import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import NetworkBanner from '../NetworkBanner';

describe('NetworkBanner', () => {
  let originalOnLine: PropertyDescriptor | undefined;

  beforeEach(() => {
    originalOnLine = Object.getOwnPropertyDescriptor(globalThis.navigator, 'onLine');
    Object.defineProperty(globalThis.navigator, 'onLine', { configurable: true, get: () => true });
  });
  afterEach(() => {
    if (originalOnLine) Object.defineProperty(globalThis.navigator, 'onLine', originalOnLine);
  });

  it('renders nothing when online', () => {
    const { container } = render(<NetworkBanner />);
    expect(container).toBeEmptyDOMElement();
  });

  it('renders the offline message after the offline event', () => {
    render(<NetworkBanner />);
    act(() => {
      window.dispatchEvent(new Event('offline'));
    });
    expect(screen.getByRole('status')).toBeInTheDocument();
    expect(screen.getByText(/network offline/i)).toBeInTheDocument();
  });
});
