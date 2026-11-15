/**
 * The close button floats over the header's top right, so a title long
 * enough to reach it wraps short of it, whichever density sets the padding.
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { ModalShell } from '../ModalShell';

describe('ModalShell header', () => {
  it.each(['form', 'reading'] as const)('keeps the title clear of the close button in %s density', (density) => {
    renderWithProviders(
      <ModalShell labelId="t" title="Remove the setup token from a computer with a long name?" onClose={() => {}} density={density}>
        <p>body</p>
      </ModalShell>,
    );
    expect(screen.getByRole('heading', { name: /Remove the setup token/ }).parentElement).toHaveClass('pr-12');
  });

  it('describes the dialog by its subtitle, and by nothing without one', () => {
    const { unmount } = renderWithProviders(
      <ModalShell labelId="t" title="Change spec" subtitle="Unpushed git commits are not kept." onClose={() => {}}>
        <p>body</p>
      </ModalShell>,
    );
    expect(screen.getByRole('dialog', { name: 'Change spec' })).toHaveAccessibleDescription(
      'Unpushed git commits are not kept.',
    );
    unmount();
    renderWithProviders(
      <ModalShell labelId="t" title="Change spec" onClose={() => {}}>
        <p>body</p>
      </ModalShell>,
    );
    expect(screen.getByRole('dialog', { name: 'Change spec' })).not.toHaveAttribute('aria-describedby');
  });
});
