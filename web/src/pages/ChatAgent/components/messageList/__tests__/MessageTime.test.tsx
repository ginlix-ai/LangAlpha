import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import { MessageTime } from '../MessageTime';

describe('MessageTime', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 9, 21, 12, 0));
  });
  afterEach(() => vi.useRealTimers());

  it.each([null, undefined, '', 'not a date'])('renders nothing for %p', (value) => {
    const { container } = render(<MessageTime value={value} />);
    expect(container.firstChild).toBeNull();
  });

  it('reads an ISO string from replayed history and stamps dateTime', () => {
    const iso = new Date(2026, 9, 21, 7, 5).toISOString();
    const { container } = render(<MessageTime value={iso} />);
    const time = container.querySelector('time');
    expect(time?.getAttribute('datetime')).toBe(iso);
    // Some ICU builds put a narrow no-break space before the day period.
    expect(time?.textContent?.replace(/\s/g, ' ')).toBe('7:05 AM');
  });
});
