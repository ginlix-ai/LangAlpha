import { useComputers } from '../../hooks/useComputers';
import { DiskWarning } from '../DiskWarning';

/** The disk warning in a chat, for the machine this workspace lives on. */
export function ChatDiskWarning({ computerId }: { computerId: string }) {
  const { data } = useComputers();
  const computer = data?.computers?.find((c) => c.computer_id === computerId);
  if (!computer) return null;
  return <DiskWarning computer={computer} />;
}
