import { describe, expect, it } from 'vitest';
import { resolveScrollContent } from '../scrollDom';

describe('resolveScrollContent', () => {
  it('finds the marked transcript behind a placeholder drawn at its width', () => {
    const viewport = document.createElement('div');
    viewport.innerHTML =
      '<div data-transcript-floor><div><div class="max-w-3xl" id="skeleton"></div></div>' +
      '<div><div data-transcript-content class="max-w-3xl" id="transcript"></div></div></div>';

    expect(resolveScrollContent(viewport).id).toBe('transcript');
  });

  it('falls back to the transcript width class on a surface with no mark', () => {
    const viewport = document.createElement('div');
    viewport.innerHTML = '<div><div class="max-w-3xl" id="transcript"></div></div>';

    expect(resolveScrollContent(viewport).id).toBe('transcript');
  });
});
