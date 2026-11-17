/** Adding this Mac by remote control, in the New computer form and the desktop app's first run. */
import type { ComputerCreate } from '@/types/api';

import { ownMachineCreate } from './addComputer';
import { useRemoteControlSetup } from './remoteControlSetupState';

/** This Mac's remote-control setup, and the body that adds it once it is ready. */
export function useAddThisMac(active: boolean) {
  const setup = useRemoteControlSetup(active);
  const body = (options?: { name?: string; takeOver?: string | null }): ComputerCreate | null =>
    setup.create ? ownMachineCreate(setup.create, options) : null;
  return { setup, body };
}
