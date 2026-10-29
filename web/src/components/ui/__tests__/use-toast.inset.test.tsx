import { describe, expect, it } from 'vitest';
import { render } from '@testing-library/react';
import { useRef } from 'react';
import { useToastInset } from '../use-toast';

function Floating({ top }: { top: number }) {
  const ref = useRef<HTMLDivElement>(null);
  useToastInset(ref);
  return (
    <div
      ref={(el) => {
        ref.current = el;
        if (el) el.getBoundingClientRect = () => ({ top }) as DOMRect;
      }}
    />
  );
}

const lift = () => document.documentElement.style.getPropertyValue('--toast-lift');

describe('useToastInset', () => {
  it('lifts by the tallest of several floating elements and releases each on its own', () => {
    const h = window.innerHeight;
    const a = render(<Floating top={h - 40} />);
    expect(lift()).toBe('40px');
    const b = render(<Floating top={h - 90} />);
    expect(lift()).toBe('90px');
    b.unmount();
    expect(lift()).toBe('40px');
    a.unmount();
    expect(lift()).toBe('');
  });
});
