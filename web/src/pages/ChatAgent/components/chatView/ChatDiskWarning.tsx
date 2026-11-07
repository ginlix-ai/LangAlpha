import { useComputers } from '../../hooks/useComputers';
import { DiskWarning } from '../DiskWarning';

import { STACK_ROW_CLASS } from './composerStack';

/** The disk warning above the chat input, for the machine this workspace lives on. */
export function ChatDiskWarning({ computerId }: { computerId: string }) {
  const { data } = useComputers();
  const computer = data?.computers?.find((c) => c.computer_id === computerId);
  if (!computer) return null;
  return <DiskWarning computer={computer} className={STACK_ROW_CLASS} />;
}
