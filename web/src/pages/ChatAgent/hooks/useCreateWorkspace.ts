/**
 * Create a workspace and refresh the lists that show it. The gallery and the
 * sidebar both open the create modal, and both hand it this.
 */
import { useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';

import { queryKeys } from '@/lib/queryKeys';
import type { Workspace } from '@/types/api';

import { createWorkspace } from '../utils/api';
import { invalidateNewWorkspace } from './workspaceRowActions';

export interface NewWorkspace {
  name: string;
  description: string;
  /** The computer to put it on; omitted means the primary. */
  computerId?: string;
}

export function useCreateWorkspace(): (data: NewWorkspace) => Promise<Workspace> {
  const queryClient = useQueryClient();
  return useCallback(
    async ({ name, description, computerId }: NewWorkspace) => {
      let created: Workspace;
      try {
        created = await createWorkspace(name, description, {}, computerId);
      } catch (err) {
        // A refusal for a computer still being prepared means the list the
        // form chose from is behind, so the computers re-read.
        void queryClient.invalidateQueries({ queryKey: queryKeys.computers.lists() });
        throw err;
      }
      // A new folder also lands on a machine, so the computer rows move too.
      invalidateNewWorkspace(queryClient);
      return created;
    },
    [queryClient],
  );
}
