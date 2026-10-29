import { afterEach, describe, expect, it } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import i18n from '@/i18n';
import { compactNumber } from '@/lib/format';
import { useLocale } from '../useLocale';

afterEach(async () => {
  await act(() => i18n.changeLanguage('en-US'));
});

// Compiled like the app, so the figure is cached on its inputs: it can only
// follow a switch if the language arrives as one of them.
function Volume({ n }: { n: number }) {
  const locale = useLocale();
  return <span>{compactNumber(n, locale)}</span>;
}

describe('useLocale', () => {
  it('reformats a mounted figure when the language switches', async () => {
    render(<Volume n={1_500_000} />);
    expect(screen.getByText('1.5M')).toBeTruthy();
    await act(() => i18n.changeLanguage('zh-CN'));
    expect(screen.getByText('150万')).toBeTruthy();
  });
});
