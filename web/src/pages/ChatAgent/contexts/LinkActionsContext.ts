import { createContext, useContext } from 'react';

/** What a chat does with a link in rendered markdown beyond opening it in a new tab. */
export interface LinkActions {
  /** Opens an `/a/` link in place, answering whether it took the click. Null
   *  outside a chat, where such a link stays a new-tab link. */
  openShareLink: ((href: string) => boolean) | null;
  /** Opens a web address in the computer's browser, in the panel's Browser tab.
   *  Null where there is none to open it in: outside a chat, on a phone, or on a
   *  computer with no browser. */
  openInBrowser: ((url: string) => void) | null;
}

export const NO_LINK_ACTIONS: LinkActions = { openShareLink: null, openInBrowser: null };

export const LinkActionsContext = createContext<LinkActions>(NO_LINK_ACTIONS);

export function useLinkActions(): LinkActions {
  return useContext(LinkActionsContext);
}
