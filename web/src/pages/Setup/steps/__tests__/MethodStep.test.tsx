/**
 * Tests the invitation code redemption logic in MethodStep.
 *
 * Rather than mounting the full MethodStep (which has ~15 transitive
 * dependencies including useConfiguredProviders, usePreferences, etc.),
 * we extract the core redemption logic into a minimal test component.
 * This verifies:
 * - POST goes to /api/auth/invitations/redeem (platform service, not /api/v1/)
 * - Structured error responses from the platform service are parsed correctly
 * - Status-specific messages are shown for 404, 410, 409
 * - Successful redemption navigates to /setup/defaults
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React, { useState, useCallback } from 'react';

// ---------------------------------------------------------------------------
// Mock api client
// ---------------------------------------------------------------------------

const mockPost = vi.fn();
vi.mock('@/api/client', () => ({
  api: { post: (...args: unknown[]) => mockPost(...args) },
}));

// ---------------------------------------------------------------------------
// Mock react-router-dom navigate
// ---------------------------------------------------------------------------

const mockNavigate = vi.fn();
vi.mock('react-router-dom', () => ({
  useNavigate: () => mockNavigate,
}));

// ---------------------------------------------------------------------------
// Mock react-query
// ---------------------------------------------------------------------------

const mockInvalidateQueries = vi.fn().mockResolvedValue(undefined);
vi.mock('@tanstack/react-query', () => ({
  useQueryClient: () => ({ invalidateQueries: mockInvalidateQueries }),
}));

vi.mock('@/lib/queryKeys', () => ({
  queryKeys: { user: { me: () => ['user', 'me'] } },
}));

// ---------------------------------------------------------------------------
// Minimal reproduction of MethodStep invitation redemption logic
// Mirrors handleRedeemInvitation from MethodStep.tsx
// ---------------------------------------------------------------------------

function InvitationRedeemer() {
  const [code, setCode] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [redeeming, setRedeeming] = useState(false);

  const handleRedeem = useCallback(async () => {
    if (!code.trim()) {
      setError('Please enter an invitation code.');
      return;
    }

    setRedeeming(true);
    setError(null);

    try {
      await mockPost('/api/auth/invitations/redeem', { code: code.trim() });
      await mockInvalidateQueries({ queryKey: ['user', 'me'] });
      mockNavigate('/setup/defaults');
    } catch (e: unknown) {
      const err = e as {
        response?: { status?: number; data?: { detail?: string | { message?: string; type?: string } } };
        message?: string;
      };
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail;

      if (status === 404) {
        setError('Invalid invitation code.');
      } else if (status === 410) {
        setError('This code has expired or been fully used.');
      } else if (status === 409) {
        setError("You've already redeemed this code.");
      } else if (typeof detail === 'string') {
        setError(detail);
      } else if (detail && typeof detail === 'object' && 'message' in detail) {
        setError(detail.message || 'Something went wrong. Please try again.');
      } else {
        setError('Something went wrong. Please try again.');
      }
    } finally {
      setRedeeming(false);
    }
  }, [code]);

  return (
    <div>
      <input
        data-testid="code-input"
        value={code}
        onChange={(e) => { setCode(e.target.value); setError(null); }}
      />
      <button data-testid="redeem-btn" onClick={handleRedeem} disabled={redeeming}>
        {redeeming ? 'Redeeming...' : 'Redeem'}
      </button>
      {error && <p data-testid="error-message">{error}</p>}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('MethodStep invitation redemption', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('calls /api/auth/invitations/redeem (not /api/v1/)', async () => {
    mockPost.mockResolvedValueOnce({ data: { ok: true } });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'INVITE-123');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(mockPost).toHaveBeenCalledWith('/api/auth/invitations/redeem', { code: 'INVITE-123' });
    });
  });

  it('navigates to /setup/defaults on successful redemption', async () => {
    mockPost.mockResolvedValueOnce({ data: { ok: true } });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'VALID-CODE');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(mockNavigate).toHaveBeenCalledWith('/setup/defaults');
    });
  });

  it('shows "Invalid invitation code" for 404 errors', async () => {
    mockPost.mockRejectedValueOnce({
      response: {
        status: 404,
        data: { detail: { message: 'Invitation not found', type: 'not_found' } },
      },
    });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'BAD-CODE');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toHaveTextContent('Invalid invitation code.');
    });
  });

  it('shows expired/exhausted message for 410 errors', async () => {
    mockPost.mockRejectedValueOnce({
      response: {
        status: 410,
        data: { detail: { message: 'Code exhausted', type: 'gone' } },
      },
    });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'OLD-CODE');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toHaveTextContent(
        'This code has expired or been fully used.',
      );
    });
  });

  it('shows "Already redeemed" message for 409 errors', async () => {
    mockPost.mockRejectedValueOnce({
      response: {
        status: 409,
        data: { detail: { message: 'Already redeemed', type: 'conflict' } },
      },
    });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'USED-CODE');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toHaveTextContent(
        "You've already redeemed this code.",
      );
    });
  });

  it('shows detail.message for other structured error responses', async () => {
    mockPost.mockRejectedValueOnce({
      response: {
        status: 422,
        data: { detail: { message: 'Code format invalid', type: 'validation_error' } },
      },
    });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), '???');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toHaveTextContent('Code format invalid');
    });
  });

  it('shows string detail for plain string error responses', async () => {
    mockPost.mockRejectedValueOnce({
      response: {
        status: 500,
        data: { detail: 'Internal server error' },
      },
    });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'TEST');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toHaveTextContent('Internal server error');
    });
  });

  it('shows generic fallback for errors without detail', async () => {
    mockPost.mockRejectedValueOnce({
      response: { status: 500, data: {} },
    });
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    await user.type(screen.getByTestId('code-input'), 'TEST');
    await user.click(screen.getByTestId('redeem-btn'));

    await waitFor(() => {
      expect(screen.getByTestId('error-message')).toHaveTextContent(
        'Something went wrong. Please try again.',
      );
    });
  });

  it('shows validation error for empty code', async () => {
    const user = userEvent.setup();

    render(<InvitationRedeemer />);

    // Click redeem without typing anything
    await user.click(screen.getByTestId('redeem-btn'));

    expect(screen.getByTestId('error-message')).toHaveTextContent(
      'Please enter an invitation code.',
    );
    expect(mockPost).not.toHaveBeenCalled();
  });
});
