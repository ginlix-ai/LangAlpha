/**
 * The provider vocabulary: what each sandbox backend is called, how it is
 * described, and whether it is a machine the user brings. One table, read by
 * the create form and by the computer rows, so a provider is described the
 * same way everywhere.
 */
import type { useTranslation } from 'react-i18next';

import { isPlatformMode } from '@/config/hostMode';
import type { Computer, ComputerProviderOption, WorkspaceQuota } from '@/types/api';

import type { RadioCardOption } from './radioCardGroup';

type Translate = ReturnType<typeof useTranslation>['t'];

interface ProviderUi {
  /** Labels are product names and read the same in every language; a phrase has a key instead. */
  label?: string;
  labelKey?: string;
  descKey: string;
  /** The user's own machine, which they size, power and keep. */
  ownMachine?: true;
}

const PROVIDER_UI: Partial<Record<string, ProviderUi>> = {
  daytona: { label: 'Daytona', descKey: 'computer.provider.daytonaDesc' },
  e2b: { label: 'E2B', descKey: 'computer.provider.e2bDesc' },
  docker: { label: 'Docker', descKey: 'computer.provider.dockerDesc' },
  ssh: { labelKey: 'computer.provider.sshLabel', descKey: 'computer.provider.sshDesc', ownMachine: true },
};

/**
 * A machine the user brings is sized, powered and kept by them, so the app
 * offers no tier, spec change or always-on for it.
 */
export function providerManagesSpec(kind: string): boolean {
  return !providerIsOwnMachine(kind);
}

/**
 * A machine the user brings. Any computer may be primary, this one included,
 * but Home on it needs a cloud computer to stand in while it can't be reached,
 * so making it primary says so. A primary the user brings can be removed,
 * Home moving to another computer first; a hosted primary is kept, so
 * nothing is offered on it.
 */
export function providerIsOwnMachine(kind: string): boolean {
  return !!providerUi(kind)?.ownMachine;
}

/** What New computer adds: a machine of the user's own only where one is on offer. */
export function newComputerNote(t: Translate, providers: readonly ComputerProviderOption[] | undefined): string {
  return providers?.some((p) => providerIsOwnMachine(p.kind))
    ? t('computer.card.newOwn', 'A cloud computer, or a machine you already have, reached over SSH.')
    : t('computer.card.newCloud', 'Another cloud computer, for work you want kept on its own machine.');
}

/**
 * Removing a machine the user brings stops using it and leaves their files
 * there, and deleting a workspace on one leaves its folder unless the user
 * asks for it to go; on every other provider the disk goes with the computer.
 */
export function providerKeepsFilesOnRemove(kind: string): boolean {
  return providerIsOwnMachine(kind);
}

/**
 * Our key sits in the machine's `authorized_keys` for as long as we log in
 * there, so removing the computer can take it out; a sandbox has no such line.
 */
export function providerHoldsOurKey(kind: string): boolean {
  return providerIsOwnMachine(kind);
}

// hasOwn: a wire value like 'constructor' must not resolve to a prototype member.
function providerUi(kind: string) {
  return Object.hasOwn(PROVIDER_UI, kind) ? PROVIDER_UI[kind] : undefined;
}

/** A kind this build does not know is shown as the server spells it. */
export function providerLabel(kind: string, t: Translate): string {
  const ui = providerUi(kind);
  return ui?.labelKey ? t(ui.labelKey) : (ui?.label ?? kind);
}

/**
 * What a machine runs on, as a list of computers names it: this Mac, a Mac
 * under remote control, or its provider. A helper is an `ssh` computer only
 * underneath, so it is not called one.
 */
export function computerSourceLabel(
  t: Translate,
  computer: Pick<Computer, 'kind' | 'ssh'>,
  thisMac: boolean,
): string {
  if (thisMac) return t('computer.remoteControl.thisMacBadge', 'This Mac');
  if (computer.ssh?.helper) return t('computer.remoteControl.provider', 'Remote control');
  return providerLabel(computer.kind, t);
}

export function providerDescription(kind: string, t: Translate): string {
  const ui = providerUi(kind);
  return ui ? t(ui.descKey) : '';
}

/**
 * Whether a computer on this provider counts toward the plan's computer cap.
 * A machine the user brings is theirs to keep, so it never does.
 */
export function providerCountsTowardCap(kind: string): boolean {
  return !providerUi(kind)?.ownMachine;
}

/** Whether the user holds as many counted computers as the plan allows. Unknown never is. */
export function computerCapReached(quota: WorkspaceQuota | null | undefined): boolean {
  const cap = quota?.computers;
  return isPlatformMode && !!cap && cap.limit >= 0 && cap.used >= cap.limit;
}

/**
 * What locks providers for a new computer beyond the plan: the plan's
 * computer cap reached, and a rule against machines the user brings, as the
 * sentence that says it (null when there is none).
 */
export interface ProviderGate {
  atCap: boolean;
  ownMachinesRule: string | null;
}

/**
 * Why a new computer cannot go on this provider, or null when it can: a rule
 * against machines the user brings, the plan lacks the provider, or the cap
 * is reached and the provider's computers count toward it.
 */
export function providerLock(provider: ComputerProviderOption, gate: ProviderGate): 'not_on_plan' | 'at_cap' | 'rule' | null {
  // A rule against machines the user brings outranks the plan: nothing about
  // the plan would open it.
  if (gate.ownMachinesRule !== null && providerIsOwnMachine(provider.kind)) return 'rule';
  if (provider.allowed === false) return 'not_on_plan';
  return gate.atCap && providerCountsTowardCap(provider.kind) ? 'at_cap' : null;
}

export function isProviderLocked(provider: ComputerProviderOption, gate: ProviderGate): boolean {
  return providerLock(provider, gate) !== null;
}

/** The deployment default when it is open, else the first one that is. */
export function defaultProviderKind(providers: readonly ComputerProviderOption[], gate: ProviderGate): string | null {
  const open = providers.filter((p) => !isProviderLocked(p, gate));
  return (open.find((p) => p.is_default) ?? open[0])?.kind ?? null;
}

/**
 * Whether the rows name each machine's provider: only when there is something
 * to tell apart, either a choice on offer or machines already on more than one.
 */
export function rowsShowProvider(
  providers: readonly ComputerProviderOption[] | undefined,
  computers: readonly Pick<Computer, 'kind'>[],
): boolean {
  return (providers?.length ?? 0) > 1 || new Set(computers.map((c) => c.kind)).size > 1;
}

/** The provider cards; a rule's lock takes its sentence as the description. */
export function providerOptions(t: Translate, providers: readonly ComputerProviderOption[], gate: ProviderGate): RadioCardOption<string>[] {
  return providers.map((p) => {
    const lock = providerLock(p, gate);
    return {
      value: p.kind,
      title: providerLabel(p.kind, t),
      description: lock === 'rule' && gate.ownMachinesRule !== null ? gate.ownMachinesRule : providerDescription(p.kind, t),
      note: lock === 'rule' ? t('organization.machines.notAllowed')
        : lock === 'not_on_plan'
        ? t('workspace.quotaNotOnPlan', 'Not on your plan')
        : lock === 'at_cap' ? t('computer.provider.capReached', 'Limit reached') : null,
      disabled: lock !== null,
    };
  });
}

/** The This Mac card's value. Not a provider kind: this Mac is added by remote control as an `ssh` computer. */
export const THIS_MAC_OPTION = 'this-mac';

/**
 * Whether this Mac can still be added, or a computer already is this Mac; or,
 * held, what adding the machine the reader is on would take: the desktop app,
 * on a Mac in a browser (`needsApp`), or Windows support (`windows`).
 */
export type ThisMacCard = 'offered' | 'added' | 'needsApp' | 'windows';

/** This Mac's card, after the hosted providers: offered in the desktop app on a Mac, held elsewhere. */
export function thisMacOption(t: Translate, state: ThisMacCard, gate: ProviderGate): RadioCardOption<string> {
  if (gate.ownMachinesRule !== null && state !== 'windows') {
    return {
      value: THIS_MAC_OPTION,
      title: t('computer.remoteControl.cardTitle', 'This Mac'),
      description: gate.ownMachinesRule,
      note: t('organization.machines.notAllowed'),
      disabled: true,
    };
  }
  if (state === 'windows') {
    return {
      value: THIS_MAC_OPTION,
      title: t('computer.remoteControl.cardTitlePc', 'This PC'),
      description: t('computer.remoteControl.windowsSoon', 'Windows support is coming soon'),
      disabled: true,
    };
  }
  return {
    value: THIS_MAC_OPTION,
    title: t('computer.remoteControl.cardTitle', 'This Mac'),
    description: state === 'needsApp'
      ? t('computer.remoteControl.needsApp', 'Needs the desktop app')
      : t('computer.remoteControl.cardDesc', 'Works even with LangAlpha closed'),
    note: state === 'added'
      ? t('computer.remoteControl.alreadyAdded', 'Already added')
      : state === 'offered' ? t('computer.ssh.previewLabel', 'Preview') : null,
    disabled: state !== 'offered',
  };
}
