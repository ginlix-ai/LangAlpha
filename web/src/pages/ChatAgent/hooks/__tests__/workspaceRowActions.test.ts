import { QueryClient } from '@tanstack/react-query';
import { expect, it, vi } from 'vitest';

import { queryKeys } from '@/lib/queryKeys';
import type { SharedWorkspaceList } from '@/types/sharedWorkspaces';
import { updateWorkspace } from '../../utils/api';
import {
  invalidateNewWorkspace, invalidateWorkspaceMembership, removeCachedWorkspace, renameWorkspaceRow,
} from '../workspaceRowActions';

vi.mock('../../utils/api', () => ({ updateWorkspace: vi.fn() }));

it('invalidates workspace and computer projections after membership changes', () => {
  const client = new QueryClient();
  const workspaces = queryKeys.workspaces.lists();
  const computers = queryKeys.computers.lists();
  // Shares the `computers` prefix, and costs a `du` on the machine: a
  // membership change must not re-read it.
  const storage = queryKeys.computers.storage('c1');
  client.setQueryData(workspaces, { workspaces: [] });
  client.setQueryData(computers, { computers: [] });
  client.setQueryData(storage, { live: true, workspaces: [], other_bytes: 0 });

  invalidateWorkspaceMembership(client);

  expect(client.getQueryState(workspaces)?.isInvalidated).toBe(true);
  expect(client.getQueryState(computers)?.isInvalidated).toBe(true);
  expect(client.getQueryState(storage)?.isInvalidated).toBe(false);
  client.clear();
});

it('also refreshes the MCP switches after a workspace is created or duplicated', () => {
  // A new workspace is seeded switched off for servers new workspaces start
  // without, and a duplicate copies its source's switches, built-ins included.
  // The Plugins scope badges read both lists.
  const client = new QueryClient();
  const catalog = queryKeys.mcp.catalog();
  const builtins = queryKeys.mcp.builtins();
  const tools = queryKeys.mcp.workspace('ws-1');
  client.setQueryData(catalog, { servers: [], max_servers: 50 });
  client.setQueryData(builtins, { servers: [] });
  client.setQueryData(tools, { servers: [] });

  invalidateNewWorkspace(client);

  expect(client.getQueryState(catalog)?.isInvalidated).toBe(true);
  expect(client.getQueryState(builtins)?.isInvalidated).toBe(true);
  // Another workspace's own view: nothing there changed.
  expect(client.getQueryState(tools)?.isInvalidated).toBe(false);
  client.clear();
});

it('takes a deleted workspace out of every cached list, totals included', () => {
  // The gallery's lists (one per order), the sidebar's page, and the reorder
  // list, whose entry is a bare array.
  const client = new QueryClient();
  const rows = [{ workspace_id: 'ws-1' }, { workspace_id: 'ws-2' }, { workspace_id: 'ws-3' }];
  const recent = queryKeys.workspaces.list({ view: 'all', sortBy: 'activity', includeHome: true });
  const byName = queryKeys.workspaces.list({ view: 'all', sortBy: 'name', includeHome: true });
  const sidebar = queryKeys.workspaces.list({ limit: 20, includeHome: true, sortBy: 'custom', offset: 0 });
  const elsewhere = queryKeys.workspaces.list({ limit: 1, offset: 0, sortBy: 'custom', includeHome: false });
  const reorder = queryKeys.workspaces.list({ view: 'reorder-all', sortBy: 'custom' });
  client.setQueryData(recent, { workspaces: rows, total: 3 });
  client.setQueryData(byName, { workspaces: [...rows].reverse(), total: 3 });
  client.setQueryData(sidebar, { workspaces: rows, total: 40 });
  client.setQueryData(elsewhere, { workspaces: [rows[0]], total: 3 });
  client.setQueryData(reorder, rows);

  removeCachedWorkspace(client, 'ws-2');

  expect(client.getQueryData(recent)).toEqual({ workspaces: [rows[0], rows[2]], total: 2 });
  expect(client.getQueryData(byName)).toEqual({ workspaces: [rows[2], rows[0]], total: 2 });
  expect(client.getQueryData(sidebar)).toEqual({ workspaces: [rows[0], rows[2]], total: 39 });
  // A page the row was not on is left as it was: its total is the refetch's to settle.
  expect(client.getQueryData(elsewhere)).toEqual({ workspaces: [rows[0]], total: 3 });
  expect(client.getQueryData(reorder)).toBe(rows);
  client.clear();
});

it("renames a shared workspace in the member's shared list too, and puts it back on a refusal", async () => {
  // A shared workspace's sidebar row, page header and gallery card read the
  // member's shared list, not the workspace lists.
  const client = new QueryClient();
  const list = queryKeys.workspaces.list({ limit: 20 });
  const shared = queryKeys.sharedWorkspaces.list();
  const entry = (name: string) => ({ workspace: { workspace_id: 'ws-s', name }, shared: { role: 'manager', can_manage: true } });
  const sharedOf = (name: string) => ({ workspaces: [entry(name)], can_create: true }) as unknown as SharedWorkspaceList;
  client.setQueryData(list, { workspaces: [{ workspace_id: 'ws-1', name: 'Mine' }] });
  client.setQueryData(shared, sharedOf('Desk'));

  let answer!: () => void;
  vi.mocked(updateWorkspace).mockImplementationOnce(() => new Promise((resolve) => { answer = () => resolve({}); }));
  const renamed = renameWorkspaceRow(client, 'ws-s', 'Semis Desk');
  expect(client.getQueryData(shared)).toEqual(sharedOf('Semis Desk'));
  answer();
  expect(await renamed).toBe(true);
  expect(client.getQueryState(shared)?.isInvalidated).toBe(true);

  client.setQueryData(shared, sharedOf('Semis Desk'));
  vi.mocked(updateWorkspace).mockRejectedValueOnce(new Error('403'));
  expect(await renameWorkspaceRow(client, 'ws-s', 'Lee desk')).toBe(false);
  expect(client.getQueryData(shared)).toEqual(sharedOf('Semis Desk'));
  // Read again: a member refused for losing the manager role stops being offered it.
  expect(client.getQueryState(shared)?.isInvalidated).toBe(true);
  client.clear();
});

it("leaves the shared list alone when a person renames their own workspace", async () => {
  const client = new QueryClient();
  const shared = queryKeys.sharedWorkspaces.list();
  const sharedList = { workspaces: [{ workspace: { workspace_id: 'ws-s', name: 'Desk' } }], can_create: true };
  client.setQueryData(shared, sharedList);
  vi.mocked(updateWorkspace).mockResolvedValueOnce({});
  expect(await renameWorkspaceRow(client, 'ws-1', 'Mine two')).toBe(true);
  expect(client.getQueryData(shared)).toBe(sharedList);
  expect(client.getQueryState(shared)?.isInvalidated).toBe(false);
  client.clear();
});
