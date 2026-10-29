import { createContext, useContext } from 'react';

/**
 * Opens an `/a/` link from rendered markdown in place, answering whether it
 * took the click. Null outside a chat, where such a link stays a new-tab link.
 */
export type ShareLinkOpener = (href: string) => boolean;

export const ShareLinkOpenerContext = createContext<ShareLinkOpener | null>(null);

export function useShareLinkOpener(): ShareLinkOpener | null {
  return useContext(ShareLinkOpenerContext);
}
