import type { TFunction } from 'i18next';

const METHOD_KEY: Record<string, string> = {
  email: 'automation.deliverToEmail',
  slack: 'automation.deliverToSlack',
  discord: 'automation.deliverToDiscord',
};

/** A delivery channel's name as the reader sees it. The agent can pick a
 *  channel the form does not offer, which keeps its raw name, capitalized. */
export function deliveryMethodName(method: string, t: TFunction): string {
  const key = METHOD_KEY[method];
  return key ? t(key) : method.charAt(0).toUpperCase() + method.slice(1);
}
