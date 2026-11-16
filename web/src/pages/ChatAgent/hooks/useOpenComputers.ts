/**
 * Opening the Computers dialog from a surface that may sit on a page without
 * its host: a refusal's way out, in a banner or in the New workspace dialog.
 */
import { useNavigate } from 'react-router';

import { computersHostMounted, openComputersPanel, type ComputersPanelView } from './computerPanelStore';

/**
 * Opens the Computers dialog onto `panel` now. A page without the dialogs'
 * host goes to chat, which mounts one; otherwise the request would show
 * nothing here and open unasked on whichever page mounts one next.
 */
export function useOpenComputers(): (panel: ComputersPanelView) => void {
  const navigate = useNavigate();
  return (panel) => {
    if (!computersHostMounted()) navigate('/chat');
    openComputersPanel(panel);
  };
}
