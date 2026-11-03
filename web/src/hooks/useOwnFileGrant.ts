import { useState } from 'react';
import { useQuery, type UseQueryResult } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import { retryUnlessClientError } from '@/api/errors';
import { createFileGrant } from '@/pages/ChatAgent/utils/api/shareLinks';
import type { FileGrant } from '@/types/api';

let ownGrants = 0;
// Read in a plain function: the compiler treats a module binding read in
// render as a constant.
const nextOwnGrant = () => ++ownGrants;

/**
 * A grant only this mount uses, so nothing it loads is answered from the
 * browser's cache: the serve route lets a response be reused for a minute, and
 * every path under a new grant is a new address. For a load whose report has
 * to describe the files as they are now. Never renewed, as what uses it is
 * done long before it expires.
 */
export function useOwnFileGrant(workspaceId: string | null | undefined): UseQueryResult<FileGrant, Error> {
  const [mount] = useState(nextOwnGrant);
  return useQuery<FileGrant, Error>({
    queryKey: queryKeys.fileGrants.own(workspaceId ?? '', mount),
    queryFn: () => createFileGrant(workspaceId!),
    enabled: !!workspaceId,
    staleTime: Infinity,
    gcTime: 0,
    retry: retryUnlessClientError,
  });
}
