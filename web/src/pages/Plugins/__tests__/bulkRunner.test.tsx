import { describe, it, expect, vi, beforeEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { ReactNode } from 'react';
import { ApiError } from '@/api/client';

const toast = vi.hoisted(() => vi.fn());
vi.mock('@/components/ui/use-toast', () => ({ toast }));

import { renderHookWithProviders } from '@/test/utils';
import { useBulkRunner, type BulkSelection, type BulkTarget } from '../components/useBulkSelection';

/**
 * A bulk "Switch on" over plugins whose connectors need a sign-in used to end
 * on "0 succeeded · 3 failed": no names, no way forward. The result is titled
 * by what happened, names what was refused, and carries the sign-in itself.
 */

function refused(server: string): ApiError {
  return new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
    data: {
      detail: {
        code: 'mcp_sign_in_required',
        message: 'Sign in to the server to switch it on.',
        servers: [{ server, url: `https://${server}.example/mcp` }],
      },
    },
    status: 409,
    statusText: '',
    headers: {},
  });
}

const selection = (): BulkSelection => ({
  selecting: true,
  selected: new Set(),
  start: vi.fn(),
  exit: vi.fn(),
  toggle: vi.fn(),
  setMany: vi.fn(),
});

const target = (name: string, run: () => Promise<unknown>): BulkTarget => ({
  key: `plugin:${name}`,
  name,
  run,
});

interface ToastArgs {
  title: ReactNode;
  description?: ReactNode;
  variant?: string;
  action?: ReactNode;
  duration?: number;
}

async function runAll(targets: BulkTarget[], signIn?: (servers: unknown) => void) {
  const { result } = renderHookWithProviders(() => useBulkRunner(selection(), () => {}));
  await act(() => result.current.run(targets, { verb: 'enable', noun: 'plugin', signIn }));
  const [args] = toast.mock.calls.at(-1) as [ToastArgs];
  render(
    <div data-testid="toast">
      <div>{args.title}</div>
      <div>{args.description}</div>
      {args.action}
    </div>,
  );
  return { ...args, text: screen.getByTestId('toast') };
}

beforeEach(() => toast.mockClear());

describe('useBulkRunner result', () => {
  it('names the rows a sign-in held back and carries the first sign-in', async () => {
    const signIn = vi.fn();
    const { variant, text, duration } = await runAll(
      [
        target('acme', () => Promise.reject(refused('acme-mcp'))),
        target('wind', () => Promise.reject(refused('wind-mcp'))),
        target('ledger', () => Promise.resolve()),
      ],
      signIn,
    );

    expect(text).toHaveTextContent('Switched on 1 plugin');
    expect(text).toHaveTextContent('acme and wind need a sign-in first.');
    // A next step, not a fault, and one message rather than two.
    expect(variant).toBeUndefined();
    expect(duration).toBeUndefined();
    expect(toast).toHaveBeenCalledTimes(1);
    expect(signIn).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: 'Sign in' }));
    expect(signIn).toHaveBeenCalledWith([
      { server: 'acme-mcp', url: 'https://acme-mcp.example/mcp' },
    ]);
  });

  it('says what could not be done when nothing changed', async () => {
    const { text } = await runAll(
      [target('acme', () => Promise.reject(refused('acme-mcp')))],
      vi.fn(),
    );
    expect(text).toHaveTextContent("Couldn't switch on 1 plugin");
    expect(text).toHaveTextContent('acme needs a sign-in first.');
  });

  it('names any other failure with the first reason the server gave', async () => {
    const gone = new ApiError('Request failed with status code 404', 'ERR_BAD_REQUEST', {
      data: { detail: 'Plugin not found' },
      status: 404,
      statusText: '',
      headers: {},
    });
    const signIn = vi.fn();
    const { variant, text, action } = await runAll(
      [target('zed', () => Promise.reject(gone)), target('yak', () => Promise.reject(gone))],
      signIn,
    );

    expect(text).toHaveTextContent("Couldn't switch on 2 plugins");
    expect(text).toHaveTextContent('zed and yak: Plugin not found');
    expect(variant).toBe('destructive');
    expect(action).toBeUndefined();
  });

  it('stays neutral when some rows changed and one failed, and names that row', async () => {
    const gone = new ApiError('Request failed with status code 404', 'ERR_BAD_REQUEST', {
      data: { detail: 'Plugin not found' },
      status: 404,
      statusText: '',
      headers: {},
    });
    const { variant, text } = await runAll([
      target('ledger', () => Promise.resolve()),
      target('acme', () => Promise.resolve()),
      target('zed', () => Promise.reject(gone)),
    ]);

    expect(text).toHaveTextContent('Switched on 2 plugins');
    expect(text).toHaveTextContent("Couldn't change zed: Plugin not found");
    expect(variant).toBeUndefined();
  });

  it('says what the rows that went took with them, from what each run returned, before a failure', async () => {
    const gone = new ApiError('Request failed with status code 404', 'ERR_BAD_REQUEST', {
      data: { detail: 'Plugin not found' },
      status: 404,
      statusText: '',
      headers: {},
    });
    const outcome = vi.fn((results: unknown[]) => [
      `Removed ${(results as number[]).reduce((a, b) => a + b, 0)} subagents.`,
    ]);
    const { result } = renderHookWithProviders(() => useBulkRunner(selection(), () => {}));
    await act(() =>
      result.current.run(
        [
          target('ledger', () => Promise.resolve(2)),
          target('acme', () => Promise.resolve(3)),
          target('zed', () => Promise.reject(gone)),
        ],
        { verb: 'uninstall', noun: 'plugin', outcome },
      ),
    );
    const [args] = toast.mock.calls.at(-1) as [ToastArgs];
    render(
      <div data-testid="toast">
        <div>{args.title}</div>
        <div>{args.description}</div>
      </div>,
    );

    expect(outcome).toHaveBeenCalledTimes(1);
    expect([...outcome.mock.calls[0][0]].sort()).toEqual([2, 3]);
    expect(screen.getByTestId('toast').textContent).toBe(
      "Uninstalled 2 pluginsRemoved 5 subagents.Couldn't change zed: Plugin not found",
    );
  });
});
