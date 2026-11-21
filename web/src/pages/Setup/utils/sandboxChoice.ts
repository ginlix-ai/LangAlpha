/**
 * The step's three cards over the server's four kinds: Daytona and E2B share
 * the cloud card, and which of them is a choice inside it.
 */
import type { InstallGroup, SandboxKind, SandboxOption, SetupStatus } from '@/api/setup';

export type SandboxCard = 'local' | 'docker' | 'cloud';
export type CloudKind = Extract<SandboxKind, 'daytona' | 'e2b'>;

export const CLOUD_KINDS: readonly CloudKind[] = ['daytona', 'e2b'];

const isCloud = (kind: string): kind is CloudKind => (CLOUD_KINDS as readonly string[]).includes(kind);

export function cardOf(kind: string): SandboxCard | null {
  if (kind === 'local' || kind === 'docker') return kind;
  return isCloud(kind) ? 'cloud' : null;
}

export function optionOf(status: SetupStatus, kind: SandboxKind): SandboxOption | undefined {
  return status.sandbox.options.find((option) => option.kind === kind);
}

export function cardAvailable(status: SetupStatus, card: SandboxCard): boolean {
  return status.sandbox.options.some((option) => cardOf(option.kind) === card && option.available);
}

/** Local is offered only by the desktop app; elsewhere it is no choice to explain. */
export function shownCards(status: SetupStatus): SandboxCard[] {
  const cards: SandboxCard[] = ['docker', 'cloud'];
  return cardAvailable(status, 'local') ? ['local', ...cards] : cards;
}

/**
 * The card picked when the step opens. Where the agent runs is the user's call,
 * so nothing is picked for them unless there is one card to pick; a choice
 * already made (saved, in config, or a server set up before) shows as made.
 */
export function initialCard(status: SetupStatus): SandboxCard | null {
  const { provider, provider_source: source } = status.sandbox;
  if (source !== 'detected' || status.completed) {
    const card = cardOf(provider);
    if (card && cardAvailable(status, card)) return card;
  }
  const available = shownCards(status).filter((card) => cardAvailable(status, card));
  return available.length === 1 ? available[0] : null;
}

export function initialCloudKind(status: SetupStatus): CloudKind {
  if (isCloud(status.sandbox.provider) && status.sandbox.provider_source !== 'detected') {
    return status.sandbox.provider;
  }
  return CLOUD_KINDS.find((kind) => optionOf(status, kind)?.key_set) ?? CLOUD_KINDS[0];
}

/**
 * Nothing chosen yet, and what the server would fall back to cannot run. Leaving
 * now would make Home's computer there, and Home's computer is never replaced,
 * so the step has to be answered first. Cloud is always on offer, so this is
 * never a dead end. A provider set on the server is not the user's to change here.
 */
export function mustChooseComputer(status: SetupStatus | undefined): boolean {
  if (!status?.needs_setup || status.sandbox.provider_source !== 'detected') return false;
  const fallback = optionOf(status, status.sandbox.provider as SandboxKind);
  return fallback !== undefined && !fallback.available;
}

/** A required group stays on whatever asks it off. */
export function toggleInstallGroup(groups: readonly InstallGroup[], id: string): InstallGroup[] {
  return groups.map((group) => (group.id === id && !group.required ? { ...group, selected: !group.selected } : group));
}

export function selectedGroupIds(groups: readonly InstallGroup[]): string[] {
  return groups.filter((group) => group.selected).map((group) => group.id);
}

/** What the groups switched on come to, or null where a size is unknown, since
 *  a total missing a part would read as smaller than it is. */
export function selectedGroupsSize(groups: readonly InstallGroup[]): { downloadMb: number; diskMb: number } | null {
  const selected = groups.filter((group) => group.selected);
  if (selected.some((group) => group.download_mb == null || group.disk_mb == null)) return null;
  return {
    downloadMb: selected.reduce((sum, group) => sum + (group.download_mb ?? 0), 0),
    diskMb: selected.reduce((sum, group) => sum + (group.disk_mb ?? 0), 0),
  };
}
