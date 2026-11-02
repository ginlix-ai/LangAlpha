import type { useTranslation } from 'react-i18next';

import type { Computer, ResourceTier } from '@/types/api';

import { isComputerStatusTransitional } from '../../utils/computerStatus';
import { providerLabel } from '../providerUi';
import { tierFacts, tierLabel, type TierOffer } from '../tierUi';
import type { SshReady } from '../sshTarget';

type Translate = ReturnType<typeof useTranslation>['t'];

export type MachineArtKind = 'macbook' | 'macStudio' | 'macMini' | 'imac' | 'cloud' | 'linux';
/** `prep` is getting ready (made, prepared or starting), `stopping` on its way down. */
export type MachineState = 'on' | 'off' | 'prep' | 'stopping';

/**
 * Which drawing a computer gets. A hosted computer is a box on a desk, sized
 * by its spec. One the user brings is drawn as what its probe found.
 */
export function machineArtOf(computer: Pick<Computer, 'kind' | 'ssh' | 'config'>): MachineArtKind {
  if (computer.kind === 'local') return ownMachineArt(localMachine(computer));
  if (computer.kind !== 'ssh') return 'cloud';
  const ssh = computer.ssh;
  return ownMachineArt({ model: ssh?.model, os: ssh?.os, helper: ssh?.helper });
}

/** What the machine the server runs on said of itself as it last started. */
export interface LocalMachineFacts {
  model?: string | null;
  os?: string | null;
  distro?: string | null;
  arch?: string | null;
  cpu?: string | null;
  cores?: number | null;
  memory_bytes?: number | null;
}

/**
 * A `local` computer has no SSH target to carry these, so they are read from
 * the row's `config`, where the start keeps them. Its system is told by its
 * name: only a Mac's says macOS.
 */
export function localMachine(computer: Pick<Computer, 'config'>): LocalMachineFacts {
  const config = computer.config ?? {};
  const facts = (config.machine ?? {}) as LocalMachineFacts;
  return {
    ...facts,
    model: typeof config.model === 'string' ? config.model : null,
    os: facts.distro?.startsWith('macOS') ? 'Darwin' : null,
  };
}

/**
 * A machine the user brings: a Mac by the model name System Information
 * gives, a Linux machine as a desk. A Mac whose model was never read (added
 * before the probe asked, or under remote control from an older helper) is
 * drawn as a MacBook, the Mac most people bring.
 */
export function ownMachineArt({ model, os, helper }: { model?: string | null; os?: string | null; helper?: boolean }): MachineArtKind {
  const name = model ?? '';
  if (name.startsWith('MacBook')) return 'macbook';
  if (name.startsWith('Mac Studio') || name.startsWith('Mac Pro')) return 'macStudio';
  if (name.startsWith('Mac mini')) return 'macMini';
  if (name.startsWith('iMac')) return 'imac';
  if (helper || os === 'Darwin' || name) return 'macbook';
  return 'linux';
}

/**
 * What a machine the user brings is called by what its probe read: its model,
 * else "Mac" or "Linux", else null for the caller's own fallback.
 */
export function ownMachineModel({ model, os, helper }: { model?: string | null; os?: string | null; helper?: boolean }): string | null {
  if (model) return model;
  if (os === 'Darwin' || helper) return 'Mac';
  return os === 'Linux' ? 'Linux' : null;
}

/** The model as the card names it, for a machine the user brings. */
export function machineModelOf(computer: Pick<Computer, 'kind' | 'ssh' | 'config'>): string | null {
  if (computer.kind === 'local') return ownMachineModel(localMachine(computer));
  if (computer.kind !== 'ssh') return null;
  const ssh = computer.ssh;
  return ownMachineModel({ model: ssh?.model, os: ssh?.os, helper: ssh?.helper });
}

/** What the drawing shows: the machine's light follows its status line. */
export function machineStateOf(computer: Pick<Computer, 'status'>, preparing: boolean): MachineState {
  if (preparing) return 'prep';
  if (computer.status === 'running') return 'on';
  if (computer.status === 'stopping') return 'stopping';
  return isComputerStatusTransitional(computer.status) ? 'prep' : 'off';
}

/** What a form's preview draws: the machine, null for the dashed ghost, and its corners. */
export interface PreviewMachine {
  art: MachineArtKind | null;
  name: string;
  detail: string;
  brand: string;
  tier?: ResourceTier;
}

/**
 * A hosted machine, its box sized by the spec, with the spec in the corner:
 * its numbers where the provider gave them, else its name.
 */
export function hostedPreview(t: Translate, kind: string | null, offer: TierOffer, tier: ResourceTier): PreviewMachine {
  const brand = kind ? providerLabel(kind, t) : '';
  const spec = tierFacts(t, offer.specs.find((s) => s.tier === tier))?.join(' · ') ?? tierLabel(t, tier);
  return { art: 'cloud', name: brand, detail: spec, brand, tier };
}

/**
 * The machine the server runs on, which the browser is on too, so what it is
 * is known before anything has run there.
 */
export function localPreview(t: Translate, kind: string, mac: boolean): PreviewMachine {
  return { art: mac ? 'macbook' : 'linux', name: providerLabel(kind, t), detail: '', brand: '' };
}

/**
 * A machine the user brings, drawn once it has been reached and its model
 * read. This Mac is a Mac from the start; any other is the ghost until then.
 */
export function ownPreview(t: Translate, ready: SshReady | null, thisMac: boolean): PreviewMachine {
  const ownMachine = t('computer.card.ownMachine', 'Own machine');
  if (ready) {
    return { art: ownMachineArt(ready.probe), name: ownMachineModel(ready.probe) ?? ownMachine, detail: ready.probe.hostname, brand: '' };
  }
  if (thisMac) return { art: 'macbook', name: t('computer.remoteControl.thisMacBadge', 'This Mac'), detail: '', brand: '' };
  return { art: null, name: ownMachine, detail: t('computer.card.previewPending', "Drawn once it's reached"), brand: '' };
}
