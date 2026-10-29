/**
 * The drop overlay shows while a drag is over the composer and goes when it
 * leaves, even when an element the pointer rested on was removed mid-drag and
 * so never fired its own dragleave.
 */
import type React from 'react';
import { describe, expect, it } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { useFileAttachments } from '../chat-input.useFileAttachments';

const drag = (target: Node) => ({ preventDefault() {}, target }) as unknown as React.DragEvent;

describe('the composer drag', () => {
  it('ends when it leaves after an element under it was removed', () => {
    const { result } = renderHook(() => useFileAttachments());
    const zone = document.createElement('div');
    const stop = document.createElement('button');
    zone.append(stop);
    document.body.append(zone);

    act(() => result.current.onDragEnter(drag(zone)));
    act(() => result.current.onDragEnter(drag(stop)));
    act(() => result.current.onDragLeave(drag(zone)));
    expect(result.current.isDragging).toBe(true);

    // The turn ends under the pointer and its Stop goes.
    stop.remove();
    act(() => result.current.onDragEnter(drag(zone)));
    act(() => result.current.onDragLeave(drag(zone)));
    expect(result.current.isDragging).toBe(false);
    zone.remove();
  });
});
