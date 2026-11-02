import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { watchStreamSilence, type StreamSilenceWatch } from '../streamSilence';

const BEAT_MS = 15_000;
const SILENCE_MS = BEAT_MS * 3;

describe('watchStreamSilence', () => {
  const hidden = { value: false };
  const onSilent = vi.fn();
  let watch: StreamSilenceWatch;

  const setHidden = (value: boolean) => {
    hidden.value = value;
    document.dispatchEvent(new Event('visibilitychange'));
  };

  beforeEach(() => {
    vi.useFakeTimers();
    onSilent.mockClear();
    hidden.value = false;
    Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden.value });
    watch = watchStreamSilence(BEAT_MS, onSilent);
  });

  afterEach(() => {
    watch.stop();
    vi.useRealTimers();
  });

  it('calls a stream dead once its keepalives are overdue, and only once', () => {
    vi.advanceTimersByTime(SILENCE_MS - 1);
    expect(onSilent).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onSilent).toHaveBeenCalledExactlyOnceWith(false);
    vi.advanceTimersByTime(SILENCE_MS * 2);
    expect(onSilent).toHaveBeenCalledTimes(1);
  });

  it('leaves alone a stream whose keepalives keep arriving', () => {
    for (let beat = 0; beat < 20; beat += 1) {
      vi.advanceTimersByTime(15_000);
      watch.heard();
    }
    expect(onSilent).not.toHaveBeenCalled();
  });

  // Regression: on iOS an app switch fires only visibilitychange, and a dead
  // socket's read stays pending, so the turn sat on "Working" past its end.
  it('gives a stream quiet through an app switch a moment on return, not the rest of the bound', () => {
    setHidden(true);
    vi.advanceTimersByTime(20_000);
    setHidden(false);
    vi.advanceTimersByTime(2_999);
    expect(onSilent).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onSilent).toHaveBeenCalledExactlyOnceWith(true);
  });

  it('lets a stream back from a short absence reach its next keepalive', () => {
    setHidden(true);
    vi.advanceTimersByTime(BEAT_MS - 1);
    setHidden(false);
    vi.advanceTimersByTime(3_000);
    expect(onSilent).not.toHaveBeenCalled();
    watch.heard();
    vi.advanceTimersByTime(SILENCE_MS - 1);
    expect(onSilent).not.toHaveBeenCalled();
  });

  it('takes bytes held for the return as the stream being alive', () => {
    setHidden(true);
    vi.advanceTimersByTime(60_000);
    setHidden(false);
    vi.advanceTimersByTime(500);
    watch.heard();
    vi.advanceTimersByTime(SILENCE_MS - 1);
    expect(onSilent).not.toHaveBeenCalled();
  });

  it('does not cut a stream a desktop tab switch left streaming', () => {
    setHidden(true);
    for (let beat = 0; beat < 8; beat += 1) {
      vi.advanceTimersByTime(15_000);
      watch.heard();
    }
    setHidden(false);
    vi.advanceTimersByTime(3_000);
    expect(onSilent).not.toHaveBeenCalled();
  });

  it('does nothing while the page stays hidden', () => {
    setHidden(true);
    vi.advanceTimersByTime(SILENCE_MS * 10);
    expect(onSilent).not.toHaveBeenCalled();
  });

  it('gives a page that woke from sleep the same moment, with no visibility change', () => {
    // The timer is frozen with the page, so it fires late rather than on time.
    vi.setSystemTime(Date.now() + SILENCE_MS + 60_000);
    vi.advanceTimersByTime(SILENCE_MS);
    expect(onSilent).not.toHaveBeenCalled();
    vi.advanceTimersByTime(3_000);
    expect(onSilent).toHaveBeenCalledExactlyOnceWith(true);
  });

  it('calls nothing after it is stopped', () => {
    watch.stop();
    vi.advanceTimersByTime(SILENCE_MS * 2);
    setHidden(true);
    setHidden(false);
    vi.advanceTimersByTime(SILENCE_MS);
    expect(onSilent).not.toHaveBeenCalled();
  });
});
