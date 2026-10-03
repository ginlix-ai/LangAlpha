import type { TFunction } from 'i18next';
import { isDmAddress, messagingAppName, platformOf } from '@/pages/ChatAgent/components/messaging/messageDelivery';

const METHOD_KEY: Record<string, string> = {
  slack: 'automation.deliverToSlack',
  discord: 'automation.deliverToDiscord',
};

/** A delivery channel's name as the reader sees it. A DM address reads
 *  "<App> DM". The agent can pick a channel the form does not offer, which
 *  keeps its app's name, or a chat address capitalized. */
export function deliveryMethodName(method: string, t: TFunction): string {
  const key = METHOD_KEY[method];
  if (key) return t(key);
  if (method.includes(':') && isDmAddress(method)) {
    const app = platformOf(method) as string;
    const appKey = METHOD_KEY[app];
    return t('automation.deliverToDm', { app: appKey ? t(appKey) : messagingAppName(app) });
  }
  if (!method.includes(':')) return messagingAppName(method) ?? method;
  return method.charAt(0).toUpperCase() + method.slice(1);
}
