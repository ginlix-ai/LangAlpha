/**
 * The screen as a machine comes up: its setup log fades out under the picture
 * fading in, rather than leaving the screen empty between the two.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, screen } from '@testing-library/react';

import { DURATION } from '@/lib/motion';

import { MachineArt } from '../MachineArt';

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

const log = () => screen.queryByText('langalpha setup', { exact: false });

describe('the setup log', () => {
  it('stays to fade out as the machine comes on, then leaves', () => {
    const { rerender } = render(<MachineArt art="macbook" state="prep" label="Mac" />);
    expect(log()).toBeInTheDocument();

    rerender(<MachineArt art="macbook" state="on" label="Mac" />);
    expect(log()?.closest('.ma-screen')).toHaveStyle({ opacity: '0' });

    act(() => vi.advanceTimersByTime(DURATION.fold * 1000));
    expect(log()).toBeNull();
  });

  it('shows again on the next start', () => {
    const { rerender } = render(<MachineArt art="macbook" state="off" label="Mac" />);
    expect(log()).toBeNull();

    rerender(<MachineArt art="macbook" state="prep" label="Mac" />);
    expect(log()?.closest('.ma-screen')).toHaveStyle({ opacity: '1' });
  });
});
