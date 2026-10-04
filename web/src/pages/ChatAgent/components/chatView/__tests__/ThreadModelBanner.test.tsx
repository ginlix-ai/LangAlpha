/**
 * The default question the banner opens is about one model. A composer pick
 * made while it is open moves the thread to another, so the question must go
 * with it rather than confirm the earlier model as the default.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { ThreadModelBanner } from '../ThreadModelBanner';

const mocks = vi.hoisted(() => ({
  mutateAsync: vi.fn(),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({
    preferences: { model_preference: { preferred_flash_model: 'model-default' } },
    isLoading: false,
    isLoaded: true,
  }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    systemDefaults: { default_model: 'deploy-default', flash_model: 'deploy-flash' },
    metadata: {
      'model-a': { display_name: 'Model A' },
      'model-b': { display_name: 'Model B' },
      'model-default': { display_name: 'Model Default' },
    },
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: mocks.mutateAsync }),
}));

describe('ThreadModelBanner', () => {
  beforeEach(() => {
    mocks.mutateAsync.mockReset();
  });

  it('drops an open default question when the thread moves to another model', async () => {
    const onDismiss = vi.fn();
    const { rerender } = renderWithProviders(
      <ThreadModelBanner model="model-a" defaultModel="model-default" mode="fast" onDismiss={onDismiss} />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Make Model A my default' }));
    await waitFor(() => expect(screen.getByText('Make Model A your default model?')).toBeTruthy());

    rerender(<ThreadModelBanner model="model-b" defaultModel="model-default" mode="fast" onDismiss={onDismiss} />);

    expect(screen.queryByText('Make Model A your default model?')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Make default' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Make Model B my default' })).toBeTruthy();
    expect(mocks.mutateAsync).not.toHaveBeenCalled();
  });

  it('keeps focus with the question that replaces its button, and returns it on cancel', async () => {
    renderWithProviders(
      <ThreadModelBanner model="model-a" defaultModel="model-default" mode="fast" onDismiss={vi.fn()} />,
    );

    fireEvent.click(screen.getByRole('button', { name: 'Make Model A my default' }));
    await waitFor(() => expect(document.activeElement).toBe(screen.getByRole('radio', { checked: true })));

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Make Model A my default' }));
  });
});
