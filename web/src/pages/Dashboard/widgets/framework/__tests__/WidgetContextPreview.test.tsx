import { afterEach, describe, expect, it } from 'vitest';
import { act, render, screen } from '@testing-library/react';
import i18n from '@/i18n';
import { WidgetContextPreview } from '../WidgetContextPreview';

afterEach(async () => {
  await act(() => i18n.changeLanguage('en-US'));
});

describe('WidgetContextPreview', () => {
  it("shows a replayed snapshot's timestamps in the app language, not the browser's", async () => {
    await act(() => i18n.changeLanguage('zh-CN'));
    const publishedAt = '2026-09-30T12:00:00Z';
    render(
      <WidgetContextPreview
        snapshot={{ widget_type: 'news.feed', widget_id: 'w1', label: 'News', data: { publishedAt } }}
        onClose={() => {}}
      />,
    );
    expect(screen.getByTitle(publishedAt).textContent).toMatch(/^2026\/9\/30 /);
  });
});
