import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import React from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RobinhoodConnectCard } from '../RobinhoodConnectCard';
import * as api from '../../../utils/api';

vi.mock('@/components/ui/use-toast', () => ({ toast: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  getRobinhoodStatus: vi.fn(),
  initiateRobinhood: vi.fn(),
  disconnectRobinhood: vi.fn(),
  formatApiErrorDetail: (e: unknown) => String(e),
}));

const mockApi = api as unknown as {
  getRobinhoodStatus: ReturnType<typeof vi.fn>;
  initiateRobinhood: ReturnType<typeof vi.fn>;
  disconnectRobinhood: ReturnType<typeof vi.fn>;
};

function renderCard() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <RobinhoodConnectCard workspaceId="ws-1" />
    </QueryClientProvider>
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe('RobinhoodConnectCard', () => {
  it('shows Connect and the trading-gated note when not connected', async () => {
    mockApi.getRobinhoodStatus.mockResolvedValue({
      connected: false, expires_at: null, trading_enabled: false, server_name: 'robinhood',
    });
    renderCard();

    expect(await screen.findByRole('button', { name: /connect/i })).toBeInTheDocument();
    expect(screen.getByText(/trading gated/i)).toBeInTheDocument();
    expect(screen.getByText(/Trade execution stays disabled/i)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /disconnect/i })).not.toBeInTheDocument();
  });

  it('shows the connected badge and Disconnect when connected', async () => {
    mockApi.getRobinhoodStatus.mockResolvedValue({
      connected: true, expires_at: '2026-07-01T00:00:00Z', trading_enabled: false, server_name: 'robinhood',
    });
    renderCard();

    expect(await screen.findByRole('button', { name: /disconnect/i })).toBeInTheDocument();
    expect(screen.getByText(/^connected$/i)).toBeInTheDocument();
    // Trading stays gated even when connected.
    expect(screen.getByText(/trading gated/i)).toBeInTheDocument();
  });

  it('opens the authorize popup on Connect', async () => {
    mockApi.getRobinhoodStatus.mockResolvedValue({
      connected: false, expires_at: null, trading_enabled: false, server_name: 'robinhood',
    });
    mockApi.initiateRobinhood.mockResolvedValue({ authorize_url: 'https://auth.example/go' });
    const popup = { location: { href: '' }, close: vi.fn() };
    const openSpy = vi.spyOn(window, 'open').mockReturnValue(popup as unknown as Window);

    renderCard();
    const btn = await screen.findByRole('button', { name: /connect/i });
    await waitFor(() => expect(btn).toBeEnabled()); // status query resolved
    fireEvent.click(btn);

    await waitFor(() => expect(mockApi.initiateRobinhood).toHaveBeenCalledWith('ws-1'));
    await waitFor(() => expect(popup.location.href).toBe('https://auth.example/go'));
    expect(openSpy).toHaveBeenCalled();
    openSpy.mockRestore();
  });
});
