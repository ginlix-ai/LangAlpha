import { describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import DocumentErrorBoundary from '../DocumentErrorBoundary';

function Broken(): never {
  throw new Error('unreadable');
}

describe('DocumentErrorBoundary', () => {
  it('says when its fallback stands in for the viewer, and when that fallback goes', () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const failed = vi.fn();
    const { unmount, getByText } = render(
      <DocumentErrorBoundary fallback={<p>Could not open</p>} onFailedChange={failed}>
        <Broken />
      </DocumentErrorBoundary>,
    );
    getByText('Could not open');
    expect(failed.mock.calls).toEqual([[true]]);
    unmount();
    expect(failed.mock.calls).toEqual([[true], [false]]);
  });
});
