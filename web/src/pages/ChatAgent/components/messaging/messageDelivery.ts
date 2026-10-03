/**
 * A `send_message` call, read once for every surface that draws it: the step
 * row, the inline card and the detail panel.
 *
 * The outcome is the result's `message_delivery` artifact; what was sent is
 * the call's own arguments, which the artifact does not repeat. A result
 * recorded before the artifact existed has none, and is drawn as any other
 * tool result rather than parsed out of its text.
 */

export type DeliveryStatus = 'sent' | 'partial' | 'failed' | 'unknown';

/** `not_sent` is a file the send never reached; `null` is one whose fate the
 *  result does not say (a send whose own outcome is unknown). */
export type DeliveryFileStatus = 'sent' | 'linked' | 'failed' | 'not_sent' | null;

export interface DeliveryFile {
  path: string;
  status: DeliveryFileStatus;
  reason: string | null;
}

export interface MessageDelivery {
  status: DeliveryStatus;
  /** Lowercased address prefix (`telegram`), or null when nothing names one. */
  platform: string | null;
  address: string | null;
  /** It went into the conversation the turn is in. */
  current: boolean;
  /** An earlier attempt of the same call delivered it; nothing went twice. */
  duplicate: boolean;
  /** The result's sentence about the outcome. */
  outcome: string;
  /** The message as the agent wrote it (markdown). */
  text: string;
  files: DeliveryFile[];
  /** The workspace the files were read from, when the call named one. */
  workspaceId: string | null;
}

export const MESSAGE_DELIVERY_TYPE = 'message_delivery';

const STATUSES = new Set<DeliveryStatus>(['sent', 'partial', 'failed', 'unknown']);
const FILE_STATUSES = new Set(['sent', 'linked', 'failed']);

const APP_NAMES: Record<string, string> = {
  slack: 'Slack',
  discord: 'Discord',
  telegram: 'Telegram',
  imessage: 'iMessage',
  feishu: 'Feishu',
};

/** The site whose favicon stands for each app. */
const APP_DOMAINS: Record<string, string> = {
  slack: 'slack.com',
  discord: 'discord.com',
  telegram: 'telegram.org',
  imessage: 'apple.com',
  feishu: 'feishu.cn',
};

/** The domain an app's favicon comes from; none for an app this build does not list. */
export function messagingAppDomain(platform: string | null | undefined): string | null {
  const key = platform?.trim().toLowerCase();
  return (key && APP_DOMAINS[key]) || null;
}

/** The app an address belongs to: `slack:T1/C2` → `slack`, bare `imessage` → `imessage`. */
export function platformOf(address: unknown): string | null {
  if (typeof address !== 'string') return null;
  const prefix = address.split(':', 1)[0].trim().toLowerCase();
  return prefix || null;
}

/** A platform's display name; an app this build does not list is capitalized. */
export function messagingAppName(platform: string | null | undefined): string | null {
  const key = platform?.trim().toLowerCase();
  if (!key) return null;
  return APP_NAMES[key] ?? key.charAt(0).toUpperCase() + key.slice(1);
}

function str(value: unknown): string | null {
  return typeof value === 'string' && value.trim() ? value : null;
}

function readFiles(raw: unknown): DeliveryFile[] {
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((entry): DeliveryFile[] => {
    if (!entry || typeof entry !== 'object') return [];
    const { path, status, reason } = entry as Record<string, unknown>;
    if (!str(path)) return [];
    return [{
      path: path as string,
      status: FILE_STATUSES.has(status as string) ? (status as DeliveryFileStatus) : 'failed',
      reason: str(reason),
    }];
  });
}

/** The delivery a `message_delivery` artifact describes, or null for any other artifact. */
export function readMessageDelivery(
  artifact: Record<string, unknown> | null | undefined,
  args: Record<string, unknown> | null | undefined,
): MessageDelivery | null {
  if (!artifact || artifact.type !== MESSAGE_DELIVERY_TYPE) return null;
  const status = STATUSES.has(artifact.status as DeliveryStatus)
    ? (artifact.status as DeliveryStatus)
    : 'unknown';
  const target = str(args?.target);
  const address = str(artifact.address) ?? target;

  let files = readFiles(artifact.files);
  // A send refused before delivery reports no files, but the call still named
  // them, and they are part of what did not go.
  if (files.length === 0 && Array.isArray(args?.files)) {
    files = (args.files as unknown[])
      .filter((path): path is string => !!str(path))
      .map((path) => ({ path, status: status === 'failed' ? 'not_sent' : null, reason: null }));
  }

  return {
    status,
    platform: platformOf(artifact.platform) ?? platformOf(address),
    address,
    current: artifact.current === true,
    duplicate: artifact.duplicate === true,
    outcome: str(artifact.message) ?? '',
    text: typeof args?.text === 'string' ? args.text : '',
    files,
    workspaceId: str(args?.workspace_id),
  };
}

/** Files that went out, as a link or as the file itself. */
export function deliveredFileCount(files: DeliveryFile[]): number {
  return files.filter((f) => f.status === 'sent' || f.status === 'linked').length;
}
