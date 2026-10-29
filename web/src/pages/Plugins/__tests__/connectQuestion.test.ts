import { expect, it, vi } from 'vitest';
import { runAuthResets } from '@/lib/authResets';
import type { ResolvedConnect } from '../hooks/useMcpOauthActions';
import { readConnectQuestion, setConnectQuestion } from '../connectQuestion';

it('takes an open question back when the account changes', () => {
  const answer = vi.fn();
  const cancel = vi.fn();
  setConnectQuestion({
    owner: Symbol('surface'),
    request: { name: 'vendor', url: 'https://vendor.example.com/mcp' } as ResolvedConnect,
    starting: false,
    answer,
    cancel,
  });

  runAuthResets();

  expect(cancel).toHaveBeenCalledOnce();
  expect(answer).not.toHaveBeenCalled();
  expect(readConnectQuestion()).toBeNull();
});
