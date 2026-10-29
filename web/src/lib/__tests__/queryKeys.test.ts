// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { queryKeys } from '../queryKeys';

describe('queryKeys', () => {
  // --- user ---
  describe('user', () => {
    it('has a stable "all" base key', () => {
      expect(queryKeys.user.all).toEqual(['user']);
    });

    it('me() extends the user base key', () => {
      expect(queryKeys.user.me()).toEqual(['user', 'me']);
    });

    it('preferences() extends the user base key', () => {
      expect(queryKeys.user.preferences()).toEqual(['user', 'preferences']);
    });

    it('apiKeys() extends the user base key', () => {
      expect(queryKeys.user.apiKeys()).toEqual(['user', 'api-keys']);
    });
  });

  // --- models ---
  describe('models', () => {
    it('has a stable "all" base key', () => {
      expect(queryKeys.models.all).toEqual(['models']);
    });
  });

  // --- oauth ---
  describe('oauth', () => {
    it('status(key) extends the oauth base key', () => {
      expect(queryKeys.oauth.status('codex')).toEqual(['oauth', 'codex']);
      expect(queryKeys.oauth.status('claude')).toEqual(['oauth', 'claude']);
    });
  });

  // --- workspaces ---
  describe('workspaces', () => {
    it('lists() extends the workspaces base key', () => {
      expect(queryKeys.workspaces.lists()).toEqual(['workspaces', 'list']);
    });

    it('list(params) appends params to lists()', () => {
      const params = { limit: 10, offset: 0 };
      expect(queryKeys.workspaces.list(params)).toEqual(['workspaces', 'list', params]);
    });

    it('detail(id) contains the workspace id', () => {
      expect(queryKeys.workspaces.detail('ws-123')).toEqual(['workspaces', 'detail', 'ws-123']);
    });

    it('home() extends the workspaces base key', () => {
      expect(queryKeys.workspaces.home()).toEqual(['workspaces', 'home']);
    });
  });

  // --- threads ---
  describe('threads', () => {
    it('byWorkspace(wsId) contains the workspace id', () => {
      expect(queryKeys.threads.byWorkspace('ws-1')).toEqual(['threads', 'workspace', 'ws-1']);
    });

    it('detail(threadId) contains the thread id', () => {
      expect(queryKeys.threads.detail('t-42')).toEqual(['threads', 'detail', 't-42']);
    });
  });

  // --- workspaceFiles ---
  describe('workspaceFiles', () => {
    it('byWs(wsId, opts) contains workspace id and options', () => {
      const opts = { path: 'results' };
      expect(queryKeys.workspaceFiles.byWs('ws-5', opts)).toEqual(['workspaceFiles', 'ws-5', opts]);
    });
  });

  // --- hierarchy / prefix invalidation ---
  describe('hierarchy', () => {
    it('user sub-keys all start with the user.all prefix', () => {
      const prefix = queryKeys.user.all;
      expect(queryKeys.user.me().slice(0, prefix.length)).toEqual(prefix);
      expect(queryKeys.user.preferences().slice(0, prefix.length)).toEqual(prefix);
      expect(queryKeys.user.apiKeys().slice(0, prefix.length)).toEqual(prefix);
    });

    it('workspaces sub-keys all start with the workspaces.all prefix', () => {
      const prefix = queryKeys.workspaces.all;
      expect(queryKeys.workspaces.lists().slice(0, prefix.length)).toEqual(prefix);
      expect(queryKeys.workspaces.list({}).slice(0, prefix.length)).toEqual(prefix);
      expect(queryKeys.workspaces.detail('x').slice(0, prefix.length)).toEqual(prefix);
      expect(queryKeys.workspaces.home().slice(0, prefix.length)).toEqual(prefix);
    });
  });
});
