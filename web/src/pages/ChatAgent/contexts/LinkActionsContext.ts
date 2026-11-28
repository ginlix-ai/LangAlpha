import { createContext, useContext } from 'react';

/** What a chat does with a link in rendered markdown beyond opening it in a new tab. */
export interface LinkActions {
  /** Opens an `/a/` link in place, answering whether it took the click. Null
   *  outside a chat, where such a link stays a new-tab link. */
  openShareLink: ((href: string) => boolean) | null;
  /** Opens a page of this app beside the chat where the panel can show it (a
   *  Drive file), answering whether it took the click. Null outside a chat,
   *  where the link goes to the page. */
  openAppRoute: ((route: string) => boolean) | null;
  /** Opens a web address in the computer's browser, in the panel's Browser tab.
   *  Null where there is none to open it in: outside a chat, on a phone, or on a
   *  computer with no browser. */
  openInBrowser: ((url: string) => void) | null;
  /** Whether the reader has a Drive. Without one a Drive link is text, since
   *  there is nothing for it to open. Outside a chat the link goes to the Drive
   *  page, which answers for itself. */
  drive: boolean;
}

export const NO_LINK_ACTIONS: LinkActions = { openShareLink: null, openAppRoute: null, openInBrowser: null, drive: true };

export const LinkActionsContext = createContext<LinkActions>(NO_LINK_ACTIONS);

export function useLinkActions(): LinkActions {
  return useContext(LinkActionsContext);
}
