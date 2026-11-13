/**
 * A history card for a file its turn never received: history marks it
 * `missing`, with no bytes behind it, so the card says so and opens nothing.
 */
import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import { AttachmentCard } from '../attachments';

describe('AttachmentCard: a file the turn never received', () => {
  it.each([
    ['a file', { name: 'q3.pdf', type: 'application/pdf', size: 0, missing: true }],
    ['an image', { name: 'chart.png', type: 'image/png', size: 0, missing: true, url: '/files/chart.png' }],
  ])('shows %s as not received, with no size and nothing to open', (_kind, attachment) => {
    render(<AttachmentCard attachment={attachment} />);
    expect(screen.getByRole('img', { name: `${attachment.name}, not received` })).toBeTruthy();
    expect(screen.getByText('Not received')).toBeTruthy();
    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.queryByText(/\d+\s*B\b/)).toBeNull();
  });
});
