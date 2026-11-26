import React from 'react';
import { PanelRight } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { SkeletonBar } from '@/components/ui/skeleton';
import { handOffToggleFocus } from './panelToggleFocus';

/**
 * What the panel's slot shows while its code arrives. On desktop the header's
 * files toggle gives way the moment it is pressed, so without this the panel
 * area is blank and there is no toggle anywhere. It draws the strip's corner
 * toggle where the real one lands, pressed, and closes as it does. A keyboard
 * press that opened the panel parks focus here until the real toggle takes
 * it. Plain utilities rather than the panel's CSS, which arrives with the panel.
 *
 * The panel's two header rows stand in skeleton, a tab in the strip and the
 * toolbar under it, at the sizes the real ones take, so nothing moves when they
 * arrive. The body stays empty: it may be files, a browser or a page, and a
 * guess at it would be swapped for something else.
 */
export function PanelLoadingStrip({ onClose }: { onClose: () => void }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div aria-busy="true" className="flex flex-col h-full border-l border-(--color-border-muted) bg-(--color-bg-page)">
      <div data-chrome="drag" className="flex items-center justify-between gap-1 px-2 py-3 border-b border-(--color-border-muted) shrink-0">
        <span className="flex items-center gap-1.5 h-[28px] min-w-0 pl-[9px] text-xs leading-[18px]">
          <SkeletonBar className="h-3.5 w-3.5 shrink-0" />
          <SkeletonBar className="w-14" />
        </span>
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
      <div className="flex items-center min-h-[34px] py-0.5 pl-3 pr-2 text-xs shrink-0">
        <SkeletonBar className="w-40" />
      </div>
    </div>
  );
}
