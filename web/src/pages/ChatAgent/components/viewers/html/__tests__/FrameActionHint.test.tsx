import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';

import FrameActionHint from '../FrameActionHint';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: { count?: number }) => (opts?.count != null ? `${key}:${opts.count}` : key),
  }),
}));

describe('FrameActionHint', () => {
  beforeEach(() => vi.useFakeTimers({ toFake: ['performance', 'setInterval', 'clearInterval'] }));
  afterEach(() => vi.useRealTimers());

  it('counts down to the click that will count, says so, then goes', () => {
    render(<FrameActionHint retryAt={performance.now() + 3000} />);
    expect(screen.getByText('filePanel:widgetClickAgainIn:3')).toBeTruthy();
    expect(screen.getByRole('status').textContent).toBe('filePanel:widgetClickRefused');
    act(() => { vi.advanceTimersByTime(3000); });
    expect(screen.getByText('filePanel:widgetClickAgain')).toBeTruthy();
    act(() => { vi.advanceTimersByTime(3000); });
    expect(screen.queryByRole('status')).toBeNull();
  });

  it('shows nothing without a refusal', () => {
    const { container } = render(<FrameActionHint retryAt={null} />);
    expect(container.firstChild).toBeNull();
  });
});
