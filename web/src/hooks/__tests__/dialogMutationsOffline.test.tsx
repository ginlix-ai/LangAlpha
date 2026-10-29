import { afterEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, renderHook, waitFor } from '@testing-library/react';
import type { ReactNode } from 'react';
import { QueryClient, QueryClientProvider, onlineManager } from '@tanstack/react-query';
import {
  useAddWorkspaceMcpServer,
  useCreateMcpCatalogServer,
  useImportMcpCatalogServers,
  useImportWorkspaceMcpServers,
  useUpdateMcpCatalogServer,
  useUpdateWorkspaceMcpServer,
} from '../useMcpServers';
import { useUploadSkill, useUploadWorkspaceSkill } from '../useSkills';

vi.mock('../../pages/ChatAgent/utils/api', () => ({
  addWorkspaceMcpServer: vi.fn(),
  updateWorkspaceMcpServer: vi.fn(),
  importWorkspaceMcpServers: vi.fn(),
  createMcpCatalogServer: vi.fn(),
  updateMcpCatalogServer: vi.fn(),
  importMcpCatalogServers: vi.fn(),
  uploadSkill: vi.fn(),
  uploadWorkspaceSkill: vi.fn(),
}));

import {
  addWorkspaceMcpServer,
  createMcpCatalogServer,
  importMcpCatalogServers,
  importWorkspaceMcpServers,
  updateMcpCatalogServer,
  updateWorkspaceMcpServer,
  uploadSkill,
  uploadWorkspaceSkill,
} from '../../pages/ChatAgent/utils/api';

const WS = 'ws-1';
const SERVER = { name: 's1', transport: 'stdio', command: 'node' } as const;
const ZIP = new File(['PK'], 'skill.zip', { type: 'application/zip' });

type Wrapper = ({ children }: { children: ReactNode }) => ReactNode;

/** Mount the hook, and hand back a send that fires it once. */
function sender<V>(useHook: () => { mutateAsync: (vars: V) => Promise<unknown> }, vars: V) {
  return (wrapper: Wrapper) => {
    const { result } = renderHook(useHook, { wrapper });
    return () => result.current.mutateAsync(vars);
  };
}

/**
 * The server form, the import modal and the skill upload each hold every
 * dismissal route until their request settles. A mutation paused for the
 * connection never settles, so the dialog could not be closed until it came
 * back; each of these has to send anyway and fail.
 */
const cases: [string, unknown, ReturnType<typeof sender>][] = [
  ['workspace server add', addWorkspaceMcpServer, sender(() => useAddWorkspaceMcpServer(WS), SERVER)],
  ['workspace server update', updateWorkspaceMcpServer, sender(() => useUpdateWorkspaceMcpServer(WS), { name: 's1', body: SERVER })],
  ['workspace import', importWorkspaceMcpServers, sender(() => useImportWorkspaceMcpServers(WS), { mcpServers: {} })],
  ['catalog server create', createMcpCatalogServer, sender(() => useCreateMcpCatalogServer(), SERVER)],
  ['catalog server update', updateMcpCatalogServer, sender(() => useUpdateMcpCatalogServer(), { name: 's1', body: SERVER })],
  ['catalog import', importMcpCatalogServers, sender(() => useImportMcpCatalogServers(), { mcpServers: {} })],
  ['skill upload', uploadSkill, sender(() => useUploadSkill(), { file: ZIP })],
  ['workspace skill upload', uploadWorkspaceSkill, sender(() => useUploadWorkspaceSkill(WS), { file: ZIP })],
];

describe('mutations a dialog stays open for', () => {
  afterEach(() => {
    onlineManager.setOnline(true);
  });

  it.each(cases)('%s fails at once while offline instead of waiting for the link', async (_label, api, mutate) => {
    onlineManager.setOnline(false);
    (api as Mock).mockRejectedValue(new Error('Network Error'));
    const client = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
    const wrapper: Wrapper = ({ children }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;

    const send = mutate(wrapper);
    let failure: string | undefined;
    act(() => {
      send().catch((err: Error) => {
        failure = err.message;
      });
    });

    await waitFor(() => expect(failure).toBe('Network Error'));
  });
});
