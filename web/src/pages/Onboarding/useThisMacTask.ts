import { useThisMacAvailability } from '@/pages/ChatAgent/hooks/useRemoteControl';

/**
 * The remote-control task's two facts, as `useThisMacAvailability` reads
 * them: whether this Mac is offered as a computer here, and whether a
 * computer already is it, false until the list says so.
 */
export function useThisMacTask(wanted: boolean): { offered: boolean; added: boolean } {
  const { offered, added } = useThisMacAvailability({ enabled: wanted, poll: false });
  return { offered, added: added === true };
}
