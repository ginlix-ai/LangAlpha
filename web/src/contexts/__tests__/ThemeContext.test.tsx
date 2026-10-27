import { beforeEach, describe, expect, it } from 'vitest';
import { act, render } from '@testing-library/react';
import { ThemeProvider } from '../ThemeContext';

function fromOtherTab(newValue: string | null) {
  act(() => {
    window.dispatchEvent(new StorageEvent('storage', { key: 'theme', newValue }));
  });
}

beforeEach(() => {
  localStorage.clear();
});

describe('a theme picked in another tab of this browser', () => {
  it('is shown here too, and removed there means the default', () => {
    localStorage.setItem('theme', 'dark');
    render(<ThemeProvider>{null}</ThemeProvider>);
    expect(document.documentElement.dataset.theme).toBe('dark');

    fromOtherTab('light');
    expect(document.documentElement.dataset.theme).toBe('light');
    expect(localStorage.getItem('theme')).toBe('light');

    fromOtherTab(null);
    expect(localStorage.getItem('theme')).toBe('auto');
  });
});
