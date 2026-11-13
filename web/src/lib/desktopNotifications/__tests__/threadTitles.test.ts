/**
 * The Dock's attention list reads thread titles from the query cache, so a
 * rename, which moves no lifecycle state, must still list it again.
 */
import { QueryClient } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import { subscribeThreadTitles } from '../notifications';

describe('subscribeThreadTitles', () => {
  it('fires when thread data lands, a rename patch included, and for nothing else', () => {
    const qc = new QueryClient();
    const onChange = vi.fn();
    const off = subscribeThreadTitles(qc, onChange);
    qc.setQueryData(['threads', 'recent', 30], { threads: [{ thread_id: 't1', title: 'Renamed' }] });
    expect(onChange).toHaveBeenCalledTimes(1);
    qc.setQueryData(['market', 'status'], { open: true });
    expect(onChange).toHaveBeenCalledTimes(1);
    off();
    qc.setQueryData(['threads', 'detail', 't1'], { title: 'Again' });
    expect(onChange).toHaveBeenCalledTimes(1);
  });
});
