import React from 'react';
import { useTranslation } from 'react-i18next';
import { Dialog, DialogContent, DialogTitle } from '@/components/ui/dialog';
import ToolCallDetailView, { type ToolCallProcessRecord } from '../../ChatAgent/components/ToolCallDetailView';
import PreviewViewer from '../../ChatAgent/components/viewers/PreviewViewer';
import type { PreviewData } from '../../ChatAgent/hooks/utils/types';
import { getDisplayName, getToolIcon } from '../../ChatAgent/components/toolDisplayConfig';
import { Zap } from 'lucide-react';

export type DialogPayload =
  | { type: 'toolcall'; toolCallProcess: ToolCallProcessRecord }
  | { type: 'preview'; preview: PreviewData };

interface MarketDetailDialogProps {
  payload: DialogPayload | null;
  onClose: () => void;
}

export default function MarketDetailDialog({ payload, onClose }: MarketDetailDialogProps): React.ReactElement {
  const { t } = useTranslation();

  const open = payload !== null;

  let title: React.ReactNode = '';
  if (payload?.type === 'toolcall') {
    const proc = payload.toolCallProcess;
    const toolName = proc.toolName || '';
    const isTaskTool = toolName === 'Task' || toolName === 'task';
    const IconComponent = getToolIcon(toolName, proc.toolCall?.args);
    const displayName = isTaskTool
      ? t('toolArtifact.subagentTask')
      : getDisplayName(toolName, t, proc.toolCall?.args);
    title = (
      <span className="flex items-center gap-2">
        <IconComponent className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
        <span>{displayName}</span>
      </span>
    );
  } else if (payload?.type === 'preview') {
    title = (
      <span className="flex items-center gap-2">
        <Zap className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
        <span>{payload.preview.title || 'Preview'}</span>
      </span>
    );
  }

  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o) onClose(); }}>
      <DialogContent
        className="max-w-3xl w-[90vw] h-[80vh] p-0 gap-0 flex flex-col overflow-hidden"
        onInteractOutside={(e) => {
          // PreviewViewer renders inside an iframe; clicks inside the iframe
          // can bubble out as outside-clicks and dismiss the dialog. Allow that
          // only via the explicit close button.
          if (payload?.type === 'preview') e.preventDefault();
        }}
      >
        {/* Radix requires a DialogTitle for a11y, even if we hide it visually
            because our header includes its own styled title element. */}
        <DialogTitle
          style={{
            position: 'absolute',
            width: 1,
            height: 1,
            padding: 0,
            margin: -1,
            overflow: 'hidden',
            clip: 'rect(0, 0, 0, 0)',
            whiteSpace: 'nowrap',
            border: 0,
          }}
        >
          {payload?.type === 'toolcall' ? 'Tool call detail' : 'Preview'}
        </DialogTitle>

        {/* Custom header */}
        <div
          className="flex items-center justify-between px-4 py-3 flex-shrink-0"
          style={{ borderBottom: '1px solid var(--color-border-muted)' }}
        >
          <div
            className="font-semibold truncate"
            style={{ color: 'var(--color-text-primary)', fontSize: 14 }}
          >
            {title}
          </div>
        </div>

        {/* Body */}
        <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
          {payload?.type === 'toolcall' && (
            <ToolCallDetailView toolCallProcess={payload.toolCallProcess} />
          )}
          {payload?.type === 'preview' && (
            <PreviewViewer
              url={payload.preview.url}
              port={payload.preview.port}
              title={payload.preview.title}
              loading={payload.preview.loading}
              error={payload.preview.error}
              reloadToken={payload.preview.reloadToken}
              onClose={onClose}
            />
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
