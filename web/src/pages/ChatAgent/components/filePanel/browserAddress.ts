/** An address as the toolbar draws it: the site stands out, the scheme of a secure
 *  page gives way to a lock, and the rest is quieter. */
export interface ShownAddress {
  secure: boolean;
  /** `http://` for a page that is not secure, which the lock does not stand in for. */
  scheme: string;
  host: string;
  rest: string;
  /** On the computer itself, so the person's own browser would reach something else. */
  local: boolean;
}

const LOCAL_HOST = /^(localhost|127(?:\.\d{1,3}){3}|0\.0\.0\.0|\[::1?\])$/i;

/** Null for anything not a web page, which is shown as it is. */
export function showAddress(url: string): ShownAddress | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return null;
  const path = parsed.pathname === '/' ? '' : parsed.pathname;
  return {
    secure: parsed.protocol === 'https:',
    scheme: parsed.protocol === 'https:' ? '' : 'http://',
    host: parsed.host,
    rest: `${path}${parsed.search}${parsed.hash}`,
    local: LOCAL_HOST.test(parsed.hostname),
  };
}
