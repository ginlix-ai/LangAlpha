import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mode = vi.hoisted(() => ({ platform: true, authEnv: true }));
vi.mock('@/config/hostMode', () => ({
  get isPlatformMode() { return mode.platform; },
}));
vi.mock('@/config/localUser', () => ({
  LOCAL_USER_ID: 'local-user',
  get hasAuthEnv() { return mode.authEnv; },
}));

import { setStorageUser, userLocalStorage } from '../userStorage';

const KEY = 'workspace_thread_id_ws-1';

beforeEach(() => {
  localStorage.clear();
  mode.platform = true;
  mode.authEnv = true;
});
afterEach(() => setStorageUser(null));

describe('userLocalStorage', () => {
  it('keeps each user their own value, so one who comes back finds it', () => {
    setStorageUser('user-a');
    userLocalStorage.setItem(KEY, 'thread-a');

    setStorageUser('user-b');
    expect(userLocalStorage.getItem(KEY)).toBeNull();
    userLocalStorage.setItem(KEY, 'thread-b');

    setStorageUser('user-a');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-a');
  });

  it('adopts a value stored before scoping once, for the first user to read it', () => {
    localStorage.setItem(KEY, 'thread-old');

    setStorageUser('user-a');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-old');
    expect(localStorage.getItem(KEY)).toBeNull();

    setStorageUser('user-b');
    expect(userLocalStorage.getItem(KEY)).toBeNull();
    setStorageUser('user-a');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-old');
  });

  it('keeps a value stored before scoping when a full store refuses its copy', () => {
    localStorage.setItem(KEY, 'thread-old');
    setStorageUser('user-a');
    const full = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('full', 'QuotaExceededError');
    });
    expect(userLocalStorage.getItem(KEY)).toBe('thread-old');
    full.mockRestore();

    expect(localStorage.getItem(KEY)).toBe('thread-old');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-old');
  });

  it('keeps a value stored before scoping when a full store refuses a new one', () => {
    localStorage.setItem(KEY, 'thread-old');
    setStorageUser('user-a');
    const full = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('full', 'QuotaExceededError');
    });
    userLocalStorage.setItem(KEY, 'thread-new');
    full.mockRestore();

    expect(userLocalStorage.getItem(KEY)).toBe('thread-old');
  });

  it('keeps nothing while nobody is signed in, so the next user inherits nothing', () => {
    setStorageUser(null);
    localStorage.setItem(KEY, 'thread-old');
    userLocalStorage.setItem(KEY, 'thread-x');
    expect(userLocalStorage.getItem(KEY)).toBeNull();

    setStorageUser('user-a');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-old');
  });

  it('keys by the local user when the mode has no sign-in', () => {
    mode.platform = false;
    userLocalStorage.setItem(KEY, 'thread-local');
    expect(localStorage.getItem(KEY)).toBeNull();
    // A platform session published in this mode is not who the app presents.
    setStorageUser('user-a');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-local');
  });

  it('keys by the local user on a platform build with no auth client', () => {
    // AuthProvider presents the local user there, and no session is ever published.
    mode.authEnv = false;
    setStorageUser(null);
    userLocalStorage.setItem(KEY, 'thread-local');
    expect(userLocalStorage.getItem(KEY)).toBe('thread-local');
  });

  it('removes a prefix for the current user and unscoped leftovers only', () => {
    setStorageUser('user-b');
    userLocalStorage.setItem('marketview_thread_id_ws-1_AAPL', 'b');
    setStorageUser('user-a');
    userLocalStorage.setItem('marketview_thread_id_ws-1_AAPL', 'a1');
    userLocalStorage.setItem('marketview_thread_id_ws-1_MSFT', 'a2');
    userLocalStorage.setItem('marketview_thread_id_ws-2_AAPL', 'a3');
    localStorage.setItem('marketview_thread_id_ws-1_NVDA', 'old');

    userLocalStorage.removeByPrefix('marketview_thread_id_ws-1_');

    expect(userLocalStorage.getItem('marketview_thread_id_ws-1_AAPL')).toBeNull();
    expect(userLocalStorage.getItem('marketview_thread_id_ws-1_MSFT')).toBeNull();
    expect(userLocalStorage.getItem('marketview_thread_id_ws-1_NVDA')).toBeNull();
    expect(userLocalStorage.getItem('marketview_thread_id_ws-2_AAPL')).toBe('a3');
    setStorageUser('user-b');
    expect(userLocalStorage.getItem('marketview_thread_id_ws-1_AAPL')).toBe('b');
  });
});
