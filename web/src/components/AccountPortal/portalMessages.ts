// The messages the app and the account portal exchange while the portal shows
// in Settings (v1). Dependency-free, so the rules test without a browser.

export const PORTAL_MESSAGE_SOURCE = 'account-portal';
export const PORTAL_MESSAGE_VERSION = 1;

export type PortalTheme = 'light' | 'dark';

/** The padding of the app's own page column, in px, so a portal page starts
 *  where the app's pages beside it do. */
export interface PortalLayout {
  top: number;
  inline: number;
}

/** What the app sends the portal. */
export type AppToPortal =
  | { type: 'hello'; locale: string; theme: PortalTheme; layout?: PortalLayout }
  /** `id` rises with each one; the portal echoes it in the `location` that answers it. */
  | { type: 'navigate'; path: string; id: number }
  | { type: 'theme'; theme: PortalTheme }
  | { type: 'layout'; layout: PortalLayout };

/** One of the portal's pages, as its own navigation lists it: `icon` is a
 *  lucide icon's name, which the app draws only from a list it knows. */
export interface PortalNavItem {
  path: string;
  label: string;
  icon: string;
}

/** The portal's pages in their groups: the account's own first, with no
 *  label, then one group per organization, named. */
export interface PortalNavGroup {
  label: string | null;
  items: PortalNavItem[];
}

/** What the portal sends the app. */
export type PortalToApp =
  /** `signedOut`: the portal cannot read this session; it says so again as `signed-out`. */
  | { type: 'ready'; path: string; title: string; signedOut?: boolean }
  | { type: 'location'; path: string; title: string; id?: number }
  | { type: 'account-changed' }
  | { type: 'open-app'; path: string }
  | { type: 'signed-out' }
  | { type: 'nav'; groups: PortalNavGroup[] };

export function envelope(message: AppToPortal) {
  return { source: PORTAL_MESSAGE_SOURCE, v: PORTAL_MESSAGE_VERSION, ...message };
}

const isText = (value: unknown): value is string => typeof value === 'string';

/**
 * The portal's navigation, read leniently: an item missing a field is dropped,
 * and anything not a list of groups is null. Read the same way from a message
 * and from the copy kept for the next visit.
 */
export function readNavGroups(value: unknown): PortalNavGroup[] | null {
  if (!Array.isArray(value)) return null;
  return value.flatMap((group: unknown) => {
    if (!group || typeof group !== 'object') return [];
    const g = group as Record<string, unknown>;
    if (!Array.isArray(g.items)) return [];
    const items = g.items.flatMap((item: unknown) => {
      if (!item || typeof item !== 'object') return [];
      const i = item as Record<string, unknown>;
      return isText(i.path) && i.path.startsWith('/') && isText(i.label) && i.label
        ? [{ path: i.path, label: i.label, icon: isText(i.icon) ? i.icon : '' }]
        : [];
    });
    return [{ label: isText(g.label) && g.label ? g.label : null, items }];
  });
}

/**
 * The portal's message in `event`, or null for anything else. The sending
 * window is checked as well as the origin: where the portal shares the app's
 * origin, other frames on that origin could post the same shape.
 */
export function readPortalMessage(
  event: Pick<MessageEvent, 'data' | 'origin' | 'source'>,
  expected: { origin: string | null; source: MessageEventSource | null | undefined },
): PortalToApp | null {
  if (!expected.origin || !expected.source) return null;
  if (event.source !== expected.source || event.origin !== expected.origin) return null;
  const data: unknown = event.data;
  if (!data || typeof data !== 'object') return null;
  const m = data as Record<string, unknown>;
  if (m.source !== PORTAL_MESSAGE_SOURCE || m.v !== PORTAL_MESSAGE_VERSION) return null;
  switch (m.type) {
    case 'ready':
      return isText(m.path)
        ? { type: 'ready', path: m.path, title: isText(m.title) ? m.title : '', ...(m.signedOut === true && { signedOut: true }) }
        : null;
    case 'location':
      return isText(m.path)
        ? { type: 'location', path: m.path, title: isText(m.title) ? m.title : '', ...(typeof m.id === 'number' && { id: m.id }) }
        : null;
    case 'open-app':
      return isText(m.path) ? { type: 'open-app', path: m.path } : null;
    case 'account-changed':
    case 'signed-out':
      return { type: m.type };
    case 'nav': {
      const groups = readNavGroups(m.groups);
      return groups ? { type: 'nav', groups } : null;
    }
    default:
      return null;
  }
}

/** The origin the portal answers from: its base resolved against this page. */
export function portalOrigin(base: string, page: string): string | null {
  try {
    return new URL(base, page).origin;
  } catch {
    return null;
  }
}
