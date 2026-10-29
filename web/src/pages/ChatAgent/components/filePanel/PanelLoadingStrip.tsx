import React from 'react';
import { PanelRight } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { handOffToggleFocus } from './panelToggleFocus';

/**
 * What the panel's slot shows while its code arrives. On desktop the header's
 * files toggle gives way the moment it is pressed, so without this the panel
 * area is blank and there is no toggle anywhere. It draws the strip's corner
 * toggle where the real one lands, pressed, and closes as it does. A keyboard
 * press that opened the panel parks focus here until the real toggle takes
 * it. Plain utilities rather than the panel's CSS, which arrives with the panel.
 */
export function PanelLoadingStrip({ onClose }: { onClose: () => void }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col h-full border-l border-(--color-border-muted) bg-(--color-bg-page)">
      <div data-chrome="drag" className="flex items-center justify-end px-2 py-3 border-b border-(--color-border-muted) shrink-0">
        <button
          type="button"
          data-panel-toggle="panel"
          data-panel-toggle-stand-in
          onClick={(event) => { onClose(); handOffToggleFocus(event, 'header'); }}
          aria-pressed
          className="p-1.5 rounded-[4px] text-(--color-text-primary) bg-(--color-bg-hover)"
          title={t('chat.workspaceFiles')}
          aria-label={t('chat.workspaceFiles')}
        >
          <PanelRight className="h-4 w-4" />
        </button>
      </div>
    </div>
  );
}
