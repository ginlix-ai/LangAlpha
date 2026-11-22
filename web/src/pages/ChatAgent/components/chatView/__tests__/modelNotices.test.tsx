/**
 * The default question an offer opens is about one model. A composer pick
 * made while it is open moves the thread to another, so the question must go
 * with it rather than confirm the earlier model as the default. The
 * subagents offer writes the thread's side as the default, and says so when
 * that write is refused.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { DefaultModelOfferNotice, SubagentsOfferNotice } from '../modelNotices';

const mocks = vi.hoisted(() => ({
  mutateAsync: vi.fn(),
  toast: vi.fn(),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({
    preferences: { model_preference: { preferred_model: 'model-default' } },
    isLoading: false,
    isLoaded: true,
  }),
}));

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    systemDefaults: { default_model: 'deploy-default', background_model: 'deploy-background' },
    metadata: {
      'model-a': { display_name: 'Model A' },
      'model-b': { display_name: 'Model B' },
      'model-default': { display_name: 'Model Default' },
    },
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutateAsync: mocks.mutateAsync, isPending: false }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: mocks.toast,
}));

const offer = (model: string) => <DefaultModelOfferNotice model={model} />;

describe('DefaultModelOfferNotice', () => {
  beforeEach(() => {
    mocks.mutateAsync.mockReset();
  });

  it('drops an open default question when the thread moves to another model', async () => {
    const { rerender } = renderWithProviders(offer('model-a'));

    fireEvent.click(screen.getByRole('button', { name: 'Make Model A my default' }));
    await waitFor(() => expect(screen.getByText('Make Model A your default model?')).toBeTruthy());

    rerender(offer('model-b'));

    expect(screen.queryByText('Make Model A your default model?')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Make default' })).toBeNull();
    expect(screen.getByRole('button', { name: 'Make Model B my default' })).toBeTruthy();
    expect(mocks.mutateAsync).not.toHaveBeenCalled();
  });

  it('keeps focus with the question that replaces its button, and returns it on cancel', async () => {
    renderWithProviders(offer('model-a'));

    fireEvent.click(screen.getByRole('button', { name: 'Make Model A my default' }));
    await waitFor(() => expect(document.activeElement).toBe(screen.getByRole('radio', { checked: true })));

    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    expect(document.activeElement).toBe(screen.getByRole('button', { name: 'Make Model A my default' }));
  });
});

describe('SubagentsOfferNotice', () => {
  beforeEach(() => {
    mocks.mutateAsync.mockReset().mockResolvedValue({});
    mocks.toast.mockReset();
  });

  it("makes the thread's side the default", () => {
    renderWithProviders(<SubagentsOfferNotice allowed={false} />);
    fireEvent.click(screen.getByRole('button', { name: 'Turn off by default' }));
    expect(mocks.mutateAsync).toHaveBeenCalledWith({ other_preference: { subagents_default: false } });
  });

  it('says so when the default could not be saved', async () => {
    mocks.mutateAsync.mockRejectedValue(new Error('500'));
    renderWithProviders(<SubagentsOfferNotice allowed />);
    await act(async () => { fireEvent.click(screen.getByRole('button', { name: 'Turn on by default' })); });
    expect(mocks.toast).toHaveBeenCalledWith(expect.objectContaining({ variant: 'destructive' }));
  });
});
