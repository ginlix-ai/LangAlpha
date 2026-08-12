/**
 * Render-nothing mount point for the user thread-events feed. Lives inside
 * the authenticated shell so the connection exists exactly while a signed-in
 * app surface does (the feed module itself is a globalThis singleton — this
 * component only ties its lifetime to the shell).
 */
import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { startThreadLifecycleFeed, stopThreadLifecycleFeed } from './feedClient';

export function ThreadLifecycleFeed(): null {
  const queryClient = useQueryClient();
  useEffect(() => {
    startThreadLifecycleFeed(queryClient);
    return () => stopThreadLifecycleFeed();
  }, [queryClient]);
  return null;
}
