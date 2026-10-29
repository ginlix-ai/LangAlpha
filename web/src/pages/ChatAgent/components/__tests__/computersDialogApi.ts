import { vi } from 'vitest';

/**
 * `../../utils/api` as the Computers dialog suites mock it, with the real
 * error readers, so a refusal is worded by the same code as in the app:
 * `vi.mock('../../utils/api', () => import('./computersDialogApi').then((m) => m.computersApi()))`.
 * Its own module, since the factory runs while the dialog's imports resolve.
 */
export async function computersApi() {
  const errors = await vi.importActual<typeof import('@/api/errors')>('@/api/errors');
  return {
    getComputers: vi.fn(),
    getComputerProviders: vi.fn(),
    getWorkspaces: vi.fn(),
    getComputerStorage: vi.fn(),
    createComputer: vi.fn(),
    renameComputer: vi.fn(),
    startComputer: vi.fn(),
    stopComputer: vi.fn(),
    deleteComputer: vi.fn(),
    setComputerAlwaysOn: vi.fn(),
    setComputerFilesBackup: vi.fn(),
    reinstallComputer: vi.fn(),
    reconnectComputer: vi.fn(),
    createSshPairing: vi.fn(),
    getSshPairing: vi.fn(),
    connectSshPairing: vi.fn(),
    probeSsh: vi.fn(),
    getSshKey: vi.fn(),
    apiErrorObject: errors.apiErrorObject,
    apiErrorReason: errors.apiErrorReason,
    getWorkspaceQuota: vi.fn(async () => ({ performance: null, max: null, always_on: null })),
    streamComputerEvents: vi.fn(async () => {}),
    streamWorkspaceEvents: vi.fn(async () => {}),
    apiErrorStatus: errors.apiErrorStatus,
    apiErrorDetailMessage: errors.apiErrorDetailMessage,
    formatApiErrorDetail: (err: unknown) => (err as { message?: string })?.message ?? 'Request failed',
  };
}
