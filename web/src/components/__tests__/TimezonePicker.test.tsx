import { describe, expect, it, vi } from 'vitest';
import { fireEvent, render, screen } from '@testing-library/react';
import TimezonePicker from '../TimezonePicker';

describe('TimezonePicker', () => {
  it.each([
    ['a stored value', 'Asia/Calcutta', 'Asia/Kolkata'],
    ['the home zone', 'Asia/Kolkata', 'Asia/Calcutta'],
  ])('lists a zone once when %s carries its retired name', (_case, value, home) => {
    render(<TimezonePicker value={value} onChange={vi.fn()} home={{ zone: home, label: 'Yours' }} aria-label="Zone" />);
    fireEvent.click(screen.getByRole('button', { name: /Zone/ }));
    const india = screen.getAllByRole('option').filter((o) => o.textContent?.includes('India'));
    expect(india).toHaveLength(1);
    expect(india[0]).toHaveAttribute('aria-selected', 'true');
  });
});
