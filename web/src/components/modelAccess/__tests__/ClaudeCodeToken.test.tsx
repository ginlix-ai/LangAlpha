/**
 * The setup-token sheet: the token stays masked and out of the cache, a save
 * waits on Claude's check, each refusal reads in our words, and a saved token
 * is what the row and the picker read next.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, fireEvent, screen, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

const clock = vi.hoisted(() => ({ now: 0 }));

vi.mock('@/api/model', () => ({
  saveClaudeCodeToken: vi.fn(),
  removeClaudeCodeToken: vi.fn(),
  startClaudeCodeSignIn: vi.fn(),
  submitClaudeCodeSignInCode: vi.fn(),
}));
vi.mock('@/hooks/useNow', () => ({ useNow: () => clock.now }));

import { removeClaudeCodeToken, saveClaudeCodeToken, startClaudeCodeSignIn, submitClaudeCodeSignInCode } from '@/api/model';
import { ClaudeCodeTokenSheet, RemoveClaudeCodeTokenDialog } from '../ClaudeCodeToken';

const save = saveClaudeCodeToken as Mock;
const remove = removeClaudeCodeToken as Mock;
const startSignIn = startClaudeCodeSignIn as Mock;
const sendCode = submitClaudeCodeSignInCode as Mock;
const TOKEN = 'sk-ant-oat01-placeholder';
const refusal = (code: string, message = 'server words', retryable = true) =>
  Object.assign(new Error('Request failed'), { response: { status: 400, data: { detail: message, error: { code, message, retryable } } } });

function setup() {
  const onSaved = vi.fn();
  const view = renderWithProviders(<ClaudeCodeTokenSheet onSaved={onSaved} onCancel={() => {}} />);
  const field = screen.getByLabelText('Setup token') as HTMLInputElement;
  const submit = () => fireEvent.click(screen.getByRole('button', { name: 'Save token' }));
  return { ...view, onSaved, field, submit };
}

describe('ClaudeCodeTokenSheet', () => {
  beforeEach(() => vi.clearAllMocks());

  it('says the token is its own credential, and links the install guide', () => {
    setup();
    expect(screen.getByText(/never your Claude Code sign-in/)).toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Install guide/ })).toHaveAttribute('href', 'https://code.claude.com/docs/en/setup');
    expect(screen.getByText('claude setup-token')).toBeInTheDocument();
  });

  it('masks the field and keeps the browser off it', () => {
    const { field } = setup();
    expect(field).toHaveAttribute('type', 'password');
    expect(field).toHaveAttribute('autocomplete', 'off');
    expect(field).toHaveAttribute('spellcheck', 'false');
  });

  it('saves nothing while the field is empty', () => {
    setup();
    expect(screen.getByRole('button', { name: 'Save token' })).toBeDisabled();
  });

  it('waits on the check, then writes the status it returns and closes', async () => {
    let resolve!: (v: unknown) => void;
    save.mockReturnValue(new Promise((r) => { resolve = r; }));
    const { field, submit, onSaved, queryClient } = setup();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    // A copy of a wrapped token carries the line break; the save does not.
    fireEvent.change(field, { target: { value: `${TOKEN.slice(0, 10)}\n${TOKEN.slice(10)}` } });
    submit();
    expect(save).toHaveBeenCalledWith(TOKEN);
    expect(screen.getByRole('button', { name: /Checking with Claude…/ })).toBeDisabled();
    expect(field).toBeDisabled();
    const status = { state: 'signed_in', email: null, expires_at: '2027-10-29T12:00:00Z' };
    await act(async () => { resolve(status); });
    expect(onSaved).toHaveBeenCalledTimes(1);
    expect(queryClient.getQueryData(queryKeys.features.claudeCodeStatus())).toEqual(status);
    // The answer is the status, so nothing reads it again.
    expect(invalidate).not.toHaveBeenCalled();
    expect(JSON.stringify(queryClient.getQueryCache().getAll().map((q) => q.state.data))).not.toContain(TOKEN);
  });

  it.each([
    ['token_missing', 'Paste your setup token first.'],
    ['token_rejected', /Claude refused this token\. It may be incomplete, expired or revoked/],
    ['check_unavailable', /Couldn't reach Claude to check the token, so nothing was saved/],
    ['check_in_progress', /A token is already being checked/],
    ['unavailable', /Claude Code isn't on for your account/],
  ])('words a %s refusal itself and stays open', async (code, text) => {
    save.mockRejectedValue(refusal(code));
    const { field, submit, onSaved } = setup();
    fireEvent.change(field, { target: { value: TOKEN } });
    submit();
    expect(await screen.findByRole('alert')).toHaveTextContent(text);
    expect(screen.queryByText('server words')).toBeNull();
    expect(onSaved).not.toHaveBeenCalled();
    expect(field).not.toBeDisabled();
    // A refused token is cleared; one the check never reached stays to retry.
    expect(field.value).toBe(code === 'token_rejected' ? '' : TOKEN);
  });

  it("shows the server's words for a refusal it does not know", async () => {
    save.mockRejectedValue(refusal('something_new', 'The server says why.'));
    const { field, submit } = setup();
    fireEvent.change(field, { target: { value: TOKEN } });
    submit();
    expect(await screen.findByRole('alert')).toHaveTextContent('The server says why.');
  });

  it('clears the refusal as the user edits the field', async () => {
    save.mockRejectedValue(refusal('check_unavailable'));
    const { field, submit } = setup();
    fireEvent.change(field, { target: { value: TOKEN } });
    submit();
    await screen.findByRole('alert');
    fireEvent.change(field, { target: { value: TOKEN + 'x' } });
    expect(screen.queryByRole('alert')).toBeNull();
  });
});

describe('RemoveClaudeCodeTokenDialog', () => {
  beforeEach(() => vi.clearAllMocks());

  it('removes the token and rereads the status', async () => {
    remove.mockResolvedValue(undefined);
    const onClose = vi.fn();
    const { queryClient } = renderWithProviders(<RemoveClaudeCodeTokenDialog onClose={onClose} />);
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    fireEvent.click(screen.getByRole('button', { name: 'Remove token' }));
    await waitFor(() => expect(onClose).toHaveBeenCalled());
    expect(invalidate).toHaveBeenCalledWith({ queryKey: queryKeys.features.claudeCodeStatus() });
  });

  it('stays open with the reason when the removal fails', async () => {
    remove.mockRejectedValue(new Error('boom'));
    const onClose = vi.fn();
    renderWithProviders(<RemoveClaudeCodeTokenDialog onClose={onClose} />);
    fireEvent.click(screen.getByRole('button', { name: 'Remove token' }));
    expect(await screen.findByRole('alert')).toHaveTextContent("Couldn't remove the token. Try again.");
    expect(onClose).not.toHaveBeenCalled();
  });
});

describe('ClaudeCodeTokenSheet: Sign in with Claude', () => {
  const AUTHORIZE = 'https://claude.ai/oauth/authorize?state=placeholder';
  const CODE = 'placeholder-code#placeholder-state';
  let opened: Mock;

  beforeEach(() => {
    vi.clearAllMocks();
    clock.now = Date.parse('2026-10-29T12:00:00Z');
    opened = vi.spyOn(window, 'open') as unknown as Mock;
    startSignIn.mockResolvedValue({ authorize_url: AUTHORIZE, expires_at: '2026-10-29T12:10:00Z' });
  });

  const startButton = () => screen.getByRole('button', { name: 'Sign in with Claude' });
  async function toCodeStep() {
    const view = setup();
    fireEvent.click(startButton());
    const field = await screen.findByLabelText('Code from Claude') as HTMLInputElement;
    const connect = () => fireEvent.click(screen.getByRole('button', { name: 'Connect' }));
    return { ...view, codeField: field, connect };
  }

  it('sits above the paste path, which keeps its own heading', () => {
    setup();
    const heading = screen.getByRole('heading', { name: 'Already have a setup token? Paste it' });
    expect(startButton().compareDocumentPosition(heading) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getByText(/makes a setup token for itself/)).toBeInTheDocument();
  });

  // The user's own click opens Claude, which no popup blocker stops; a
  // window opened after the request returns could be.
  it('offers Claude in a new tab by link, then asks for the masked code with the time left', async () => {
    const { codeField } = await toCodeStep();
    expect(opened).not.toHaveBeenCalled();
    const link = screen.getByRole('link', { name: /^Open claude\.ai$/ });
    expect(link).toHaveAttribute('href', AUTHORIZE);
    expect(link).toHaveAttribute('target', '_blank');
    fireEvent.click(link);
    expect(screen.getByRole('link', { name: /Open claude\.ai again/ })).toHaveAttribute('href', AUTHORIZE);
    expect(codeField).toHaveAttribute('type', 'password');
    expect(codeField).toHaveAttribute('autocomplete', 'off');
    expect(codeField).toHaveAttribute('spellcheck', 'false');
    expect(screen.getByText('Sign-in link expires in 10:00')).toBeInTheDocument();
  });

  it('stays at the start with the reason when the sign-in cannot begin', async () => {
    startSignIn.mockRejectedValue(refusal('redis_unavailable', 'server words', true));
    setup();
    fireEvent.click(startButton());
    expect(await screen.findByRole('alert')).toHaveTextContent("Signing in isn't available right now.");
    expect(startButton()).toBeEnabled();
    expect(opened).not.toHaveBeenCalled();
  });

  it('waits on the exchange and the check, then writes the status and closes', async () => {
    let resolve!: (v: unknown) => void;
    sendCode.mockReturnValue(new Promise((r) => { resolve = r; }));
    const { codeField, connect, onSaved, queryClient } = await toCodeStep();
    const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
    fireEvent.change(codeField, { target: { value: CODE } });
    connect();
    expect(sendCode).toHaveBeenCalledWith(CODE);
    expect(screen.getByRole('button', { name: /Checking with Claude…/ })).toBeDisabled();
    expect(codeField).toBeDisabled();
    const status = { state: 'signed_in', email: 'user@example.com', expires_at: '2027-10-29T12:00:00Z' };
    await act(async () => { resolve(status); });
    expect(onSaved).toHaveBeenCalledTimes(1);
    expect(queryClient.getQueryData(queryKeys.features.claudeCodeStatus())).toEqual(status);
    expect(invalidate).not.toHaveBeenCalled();
    expect(codeField.value).toBe('');
  });

  it.each([
    ['code_missing', true, 'Paste the code from Claude first.'],
    ['invalid_callback_input', true, /doesn't look like the code from Claude's page/],
    ['check_unavailable', true, "Couldn't reach Claude to check the token. Send the code again."],
    ['check_in_progress', true, /A token is already being checked/],
    ['state_mismatch', false, /belongs to a different sign-in/],
    ['no_pending_flow', false, /expired or was already used/],
    ['code_rejected', false, /Codes work once and expire quickly/],
    ['check_unavailable', false, /the code can't be used again/],
    ['token_rejected', false, /may not include Claude Code/],
    ['unexpected_grant', false, /didn't expect/],
    ['unavailable', false, /isn't on for your account/],
  ])('a %s refusal (retryable %s) is worded here', async (code, retryable, text) => {
    sendCode.mockRejectedValue(refusal(code, 'server words', retryable));
    const { codeField, connect, onSaved } = await toCodeStep();
    fireEvent.change(codeField, { target: { value: CODE } });
    connect();
    expect(await screen.findByRole('alert')).toHaveTextContent(text);
    expect(screen.queryByText('server words')).toBeNull();
    expect(onSaved).not.toHaveBeenCalled();
    if (retryable) {
      // The same code can still get through: the field keeps it.
      expect(codeField).toBeEnabled();
      expect(codeField.value).toBe(CODE);
      expect(screen.queryByRole('button', { name: 'Sign in with Claude' })).toBeNull();
    } else {
      // The code is spent: back to the start, and a new sign-in starts empty.
      expect(screen.queryByLabelText('Code from Claude')).toBeNull();
      fireEvent.click(startButton());
      expect((await screen.findByLabelText('Code from Claude') as HTMLInputElement).value).toBe('');
    }
  });

  it("shows the server's words for a refusal it does not know", async () => {
    sendCode.mockRejectedValue(refusal('something_new', 'The server says why.', false));
    const { codeField, connect } = await toCodeStep();
    fireEvent.change(codeField, { target: { value: CODE } });
    connect();
    expect(await screen.findByRole('alert')).toHaveTextContent('The server says why.');
    expect(startButton()).toBeInTheDocument();
  });

  it('starts over once the sign-in runs out of time', async () => {
    const { codeField, rerender } = await toCodeStep();
    fireEvent.change(codeField, { target: { value: CODE } });
    clock.now = Date.parse('2026-10-29T12:10:01Z');
    rerender(<ClaudeCodeTokenSheet onSaved={() => {}} onCancel={() => {}} />);
    expect(screen.getByRole('alert')).toHaveTextContent(/expired or was already used/);
    expect(screen.queryByLabelText('Code from Claude')).toBeNull();
    expect(startButton()).toBeInTheDocument();
  });
});
