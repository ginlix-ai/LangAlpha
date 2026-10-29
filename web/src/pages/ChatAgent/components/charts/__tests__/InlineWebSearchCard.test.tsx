import { act, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';

import i18n from '@/i18n';
import { InlineWebSearchCard } from '../InlineWebSearchCard';

const artifact = {
  type: 'web_search',
  query: 'acme quarterly guidance',
  results: Array.from({ length: 6 }, (_, i) => ({
    title: `Result ${i + 1}`,
    url: `https://example${i + 1}.com/page`,
  })),
};

afterEach(async () => {
  await act(() => i18n.changeLanguage('en-US'));
});

describe('InlineWebSearchCard', () => {
  it('counts its results, and the ones it folds away, in the reader\'s language', async () => {
    render(<InlineWebSearchCard artifact={artifact} />);

    expect(screen.getByText('6 results')).toBeInTheDocument();
    expect(screen.getByText('+2 more results')).toBeInTheDocument();
    expect(screen.getByText('Result 4')).toBeInTheDocument();
    expect(screen.queryByText('Result 5')).not.toBeInTheDocument();

    await act(() => i18n.changeLanguage('zh-CN'));
    expect(screen.getByText('6 个结果')).toBeInTheDocument();
    expect(screen.getByText('还有 2 个结果')).toBeInTheDocument();
  });
});
