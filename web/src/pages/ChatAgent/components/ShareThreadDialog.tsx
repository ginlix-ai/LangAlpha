import { useState } from 'react';
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';
import { ThreadSharePanel } from './ThreadSharePanel';

export interface ShareThreadTarget {
  threadId: string;
  workspaceId: string;
}

interface ShareThreadDialogProps {
  target: ShareThreadTarget | null;
  onClose: () => void;
}

/** The chat header's share controls, opened for any thread from the sidebar. */
export default function ShareThreadDialog({ target, onClose }: ShareThreadDialogProps) {
  // Latched, so the panel stays in place while the dialog animates out.
  const [shown, setShown] = useState(target);
  if (target && target !== shown) setShown(target);

  return (
    <Dialog open={!!target} onOpenChange={(open) => { if (!open) onClose(); }}>
      <DialogContent className="max-w-sm p-0 gap-0">
        {shown && (
          <div className="pr-8">
            <ThreadSharePanel
              key={shown.threadId}
              threadId={shown.threadId}
              workspaceId={shown.workspaceId}
              Heading={DialogTitle}
            />
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
