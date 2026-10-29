import { useCallback, useState } from 'react';

// Shared across ChatView instances, so the view a thread switch activates
// inherits whether the drawer was open.
const sharedNav = { visible: false };

/** The mobile nav drawer's visibility. Desktop navigation is the app-shell AppSidebar. */
export function useNavPanel() {
  const [navPanelVisible, setNavPanelVisible] = useState(sharedNav.visible);
  // Whether the drawer slides in when it next appears: only when the user
  // opens it. Open on arrival (inherited from the previous thread), it appears
  // in place. State set beside the visibility it describes, so the render that
  // mounts the drawer reads both together.
  const [navSlideIn, setNavSlideIn] = useState(!sharedNav.visible);

  const handleNavMinimize = useCallback(() => {
    sharedNav.visible = false;
    setNavPanelVisible(false);
  }, []);

  const handleNavExpand = useCallback(() => {
    sharedNav.visible = true;
    setNavSlideIn(true);
    setNavPanelVisible(true);
  }, []);

  // On view activation (ChatView's become-active effect): inherit the shared state.
  const inheritNavOnActivate = useCallback((): void => {
    setNavPanelVisible(sharedNav.visible);
    if (sharedNav.visible) setNavSlideIn(false);
  }, []);

  return { navPanelVisible, navSlideIn, handleNavMinimize, handleNavExpand, inheritNavOnActivate };
}
