// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { readStop } from '../stopBubble';
import type { SSEEvent } from '../../types';

const close = (stoppedBy?: string): SSEEvent => ({ finish_reason: 'stopped', ...(stoppedBy ? { stopped_by: stoppedBy } : {}) }) as SSEEvent;

describe('readStop', () => {
  it('tells a lost computer and a suspension apart from the user', () => {
    expect(readStop(close())).toBe(true);
    expect(readStop(close('suspension'))).toBe('suspension');
    expect(readStop(close('computer_lost'))).toBe('computer_lost');
    expect(readStop(close('someone_else'))).toBe(true);
  });
});
