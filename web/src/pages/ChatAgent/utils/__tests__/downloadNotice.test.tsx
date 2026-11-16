import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act, render, renderHook, screen } from '@testing-library/react';
import type { ReactElement } from 'react';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn(() => ({ dismiss: vi.fn(), update: vi.fn() })) }));

import i18n from '@/i18n';
import { toast } from '@/components/ui/use-toast';
import type { ReadFrom } from '../api/readFrom';
import { trackPending, useDownloadState, withDownloadNotice } from '../downloadNotice';

describe('trackPending', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('joins a repeat save, then holds the key as started before it goes idle', async () => {
    let finish!: (handedOff: boolean) => void;
    const run = vi.fn(() => new Promise<boolean>((resolve) => { finish = resolve; }));
    const { result } = renderHook(() => useDownloadState('ws:big.bin'));
    expect(result.current).toBe('idle');

    let first!: Promise<boolean>;
    let second!: Promise<boolean>;
    act(() => {
      first = trackPending('ws:big.bin', run);
      second = trackPending('ws:big.bin', run);
    });
    expect(run).toHaveBeenCalledTimes(1);
    expect(second).toBe(first);
    expect(result.current).toBe('preparing');

    await act(async () => {
      finish(true);
      await first;
    });
    expect(result.current).toBe('started');

    // A click inside the hold is the same click again.
    await act(async () => {
      await trackPending('ws:big.bin', run);
    });
    expect(run).toHaveBeenCalledTimes(1);

    act(() => {
      vi.runAllTimers();
    });
    expect(result.current).toBe('idle');
  });

  it('goes straight back to idle when nothing was handed to the browser', async () => {
    const { result } = renderHook(() => useDownloadState('ws:gone.bin'));
    await act(async () => {
      await trackPending('ws:gone.bin', async () => false);
    });
    expect(result.current).toBe('idle');
  });
});

describe('withDownloadNotice', () => {
  beforeEach(async () => {
    await i18n.changeLanguage('en-US');
    vi.mocked(toast).mockClear();
  });

  async function confirmation(from: ReadFrom) {
    await withDownloadNotice(`ws:${Math.random()}`, 'report.pdf', async () => from);
    const { description } = vi.mocked(toast).mock.calls.at(-1)![0] as { description: ReactElement };
    render(description);
  }

  it('says a download came from the backup while the computer is away', async () => {
    await confirmation({ kind: 'backup', unreachable: 'not_connected' });
    expect(screen.getByText(/report\.pdf, from its backup\. Can't reach this workspace's computer/)).toBeInTheDocument();
  });

  it('confirms a live download plainly', async () => {
    await confirmation({ kind: 'live' });
    expect(screen.getByText(/Download started: report\.pdf\. Check your browser's downloads/)).toBeInTheDocument();
  });
});
