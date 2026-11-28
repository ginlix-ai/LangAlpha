import { act, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it } from 'vitest';
import i18n from '@/i18n';
import CrosshairTooltip from '../CrosshairTooltip';

// 2026-03-31 00:00 as chart seconds: venue wall clock written as UTC.
const BAR = { time: Date.UTC(2026, 2, 31) / 1000, open: 10, high: 12, low: 9, close: 11, volume: 1500 };

describe('CrosshairTooltip', () => {
  afterEach(async () => {
    await act(() => i18n.changeLanguage('en-US'));
  });

  it("reads in the user's language, on the bar's own date", async () => {
    await act(() => i18n.changeLanguage('zh-CN'));
    render(<CrosshairTooltip visible x={0} y={0} data={BAR} />);
    expect(screen.getByText('2026年3月31日')).toBeInTheDocument();
    for (const label of ['开', '高', '低', '收', '量']) expect(screen.getByText(label)).toBeInTheDocument();
  });
});
