/**
 * The welcome flow's screenshot drop zone takes images from the picker, a drop
 * or a paste, prepares them in parallel and keeps at most five. A file that is
 * not an image is refused by name, and a remove hands back the list without
 * that shot. Preparing needs a canvas and `createImageBitmap`, which jsdom
 * lacks, so the component tests stand in for `prepareScreenshot`; the type
 * check that runs before any decoding is tested on the real function.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import * as real from '../welcome/screenshots';
import type { Screenshot } from '../welcome/screenshots';

vi.mock('../welcome/screenshots', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../welcome/screenshots')>()),
  prepareScreenshot: vi.fn(),
}));

import { ScreenshotDrop } from '../welcome/steps/ScreenshotDrop';

const prepare = vi.mocked(real.prepareScreenshot);

function shot(name: string): Screenshot {
  return { id: `id-${name}`, name, dataUrl: `data:image/jpeg;base64,${name}`, bytes: 10 };
}
/** A stand-in for the flow's state: `onChange` applies a setter the way React
 *  would, and `current` is the list it leaves. */
function flowState(initial: Screenshot[] = []) {
  const state = { current: initial };
  const onChange = vi.fn((next: Screenshot[] | ((prev: Screenshot[]) => Screenshot[])) => {
    state.current = typeof next === 'function' ? next(state.current) : next;
  });
  return { state, onChange };
}
const png = (name: string) => new File(['x'], name, { type: 'image/png' });

beforeEach(() => {
  prepare.mockReset();
  prepare.mockImplementation(async (file: File) => shot(file.name));
});

describe('prepareScreenshot', () => {
  it('refuses a file that is not an image', async () => {
    const actual = await vi.importActual<typeof import('../welcome/screenshots')>(
      '../welcome/screenshots',
    );
    await expect(
      actual.prepareScreenshot(new File(['x'], 'a.heic', { type: 'image/heic' })),
    ).rejects.toMatchObject({ reason: 'not-image' });
    await expect(
      actual.prepareScreenshot(new File(['x'], 'a.pdf', { type: 'application/pdf' })),
    ).rejects.toMatchObject({ reason: 'not-image' });
  });
});

describe('ScreenshotDrop', () => {
  it('keeps the first five of seven files and says so', async () => {
    const { state, onChange } = flowState();
    const { container } = renderWithProviders(<ScreenshotDrop shots={[]} onChange={onChange} />);
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    const files = Array.from({ length: 7 }, (_, i) => png(`s${i}.png`));
    fireEvent.change(input, { target: { files } });

    await waitFor(() => expect(onChange).toHaveBeenCalledTimes(1));
    expect(state.current.map((s) => s.name)).toEqual(['s0.png', 's1.png', 's2.png', 's3.png', 's4.png']);
    expect(prepare).toHaveBeenCalledTimes(5);
    expect(screen.getByRole('status')).toHaveTextContent('Up to 5 screenshots; the rest were left out.');
  });

  it('names a file that is not an image and keeps the rest', async () => {
    prepare.mockImplementation(async (file: File) => {
      if (file.name === 'bad.heic') throw new real.ScreenshotError('not-image');
      return shot(file.name);
    });
    const { state, onChange } = flowState();
    const { container } = renderWithProviders(<ScreenshotDrop shots={[]} onChange={onChange} />);
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [png('ok.png'), png('bad.heic')] } });

    await waitFor(() => expect(onChange).toHaveBeenCalled());
    expect(state.current.map((s) => s.name)).toEqual(['ok.png']);
    expect(screen.getByRole('status')).toHaveTextContent("bad.heic isn't a PNG, JPEG, WebP or GIF image.");
  });

  it('a file still being read when the step is left joins the list all the same', async () => {
    let finish: (value: Screenshot) => void = () => {};
    prepare.mockImplementation(() => new Promise((resolve) => (finish = resolve)));
    const { state, onChange } = flowState();
    const { container, unmount } = renderWithProviders(<ScreenshotDrop shots={[]} onChange={onChange} />);
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [png('late.png')] } });

    // Continue, while it is still being read.
    unmount();
    await act(async () => finish(shot('late.png')));
    expect(state.current.map((s) => s.name)).toEqual(['late.png']);
  });

  it('hands the flow each add as it starts, settled once its files are in the list', async () => {
    let finish: (value: Screenshot) => void = () => {};
    prepare.mockImplementation(() => new Promise((resolve) => (finish = resolve)));
    const { state, onChange } = flowState();
    const onPreparing = vi.fn<(work: Promise<void>) => void>();
    const { container, unmount } = renderWithProviders(
      <ScreenshotDrop shots={[]} onChange={onChange} onPreparing={onPreparing} />,
    );
    const input = container.querySelector('input[type="file"]') as HTMLInputElement;
    fireEvent.change(input, { target: { files: [png('late.png')] } });
    expect(onPreparing).toHaveBeenCalledTimes(1);

    // Start, a few steps on, waits on it and then reads the list.
    unmount();
    finish(shot('late.png'));
    await act(async () => {
      await onPreparing.mock.calls[0][0];
      expect(state.current.map((s) => s.name)).toEqual(['late.png']);
    });
  });

  it('removes a shot without touching the others', () => {
    const shots = [shot('a.png'), shot('b.png')];
    const { state, onChange } = flowState(shots);
    renderWithProviders(<ScreenshotDrop shots={shots} onChange={onChange} />);
    const removes = screen.getAllByRole('button', { name: /^Remove / });
    expect(removes).toHaveLength(2);
    fireEvent.click(removes[0]);
    expect(state.current).toEqual([shots[1]]);
  });

  it('adds an image pasted outside a text field', async () => {
    const { state, onChange } = flowState();
    renderWithProviders(<ScreenshotDrop shots={[]} onChange={onChange} />);
    fireEvent.paste(document.body, { clipboardData: { files: [png('clip.png')] } });
    await waitFor(() => expect(onChange).toHaveBeenCalled());
    expect(state.current[0].name).toBe('clip.png');
  });

  it('refuses a file let go outside the zone instead of letting the page open it', () => {
    renderWithProviders(<ScreenshotDrop shots={[]} onChange={vi.fn()} />);
    const over = { types: ['Files'], dropEffect: 'copy' };
    expect(fireEvent.dragOver(document.body, { dataTransfer: over })).toBe(false);
    expect(over.dropEffect).toBe('none');
    expect(
      fireEvent.drop(document.body, { dataTransfer: { types: ['Files'], files: [png('x.png')] } }),
    ).toBe(false);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('still takes a file dropped on the zone', async () => {
    const { state, onChange } = flowState();
    const { container } = renderWithProviders(<ScreenshotDrop shots={[]} onChange={onChange} />);
    const zone = container.querySelector('button') as HTMLButtonElement;
    const over = { types: ['Files'], dropEffect: 'copy' };
    expect(fireEvent.dragOver(zone, { dataTransfer: over })).toBe(false);
    expect(over.dropEffect).toBe('copy');
    fireEvent.drop(zone, { dataTransfer: { types: ['Files'], files: [png('dropped.png')] } });
    await waitFor(() => expect(onChange).toHaveBeenCalled());
    expect(state.current[0].name).toBe('dropped.png');
  });

  it('leaves the window alone while it is kept unseen', () => {
    renderWithProviders(<ScreenshotDrop shots={[]} onChange={vi.fn()} active={false} />);
    expect(fireEvent.dragOver(document.body, { dataTransfer: { types: ['Files'] } })).toBe(true);
  });

  it('ignores a paste into a text field', () => {
    const onChange = vi.fn();
    renderWithProviders(
      <>
        <textarea data-testid="ta" />
        <ScreenshotDrop shots={[]} onChange={onChange} />
      </>,
    );
    fireEvent.paste(screen.getByTestId('ta'), { clipboardData: { files: [png('clip.png')] } });
    expect(prepare).not.toHaveBeenCalled();
  });
});
