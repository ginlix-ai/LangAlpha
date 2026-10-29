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
  return !providerUi(kind)?.ownMachine;
}

/**
 * Home lives on the primary, which is never removed, so it has to be a machine
 * the deployment can build again; one the user brings may be wiped or gone.
 */
export function providerCanBePrimary(kind: string): boolean {
  return !providerUi(kind)?.ownMachine;
}

/**
 * Removing a machine the user brings stops using it and leaves their files
 * there, and deleting a workspace on one leaves its folder unless the user
 * asks for it to go; on every other provider the disk goes with the computer.
 */
export function providerKeepsFilesOnRemove(kind: string): boolean {
  return !!providerUi(kind)?.ownMachine;
}

/**
 * Our key sits in the machine's `authorized_keys` for as long as we log in
 * there, so removing the computer can take it out; a sandbox has no such line.
 */
export function providerHoldsOurKey(kind: string): boolean {
  return !!providerUi(kind)?.ownMachine;
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
 * Why a new computer cannot go on this provider, or null when it can: the
 * plan lacks the provider, or `atCap` and the provider's computers count.
 */
export function providerLock(provider: ComputerProviderOption, atCap: boolean): 'not_on_plan' | 'at_cap' | null {
  if (provider.allowed === false) return 'not_on_plan';
  return atCap && providerCountsTowardCap(provider.kind) ? 'at_cap' : null;
}

export function isProviderLocked(provider: ComputerProviderOption, atCap = false): boolean {
  return providerLock(provider, atCap) !== null;
}

/** The deployment default when the plan allows it, else the first one it does. */
export function defaultProviderKind(providers: readonly ComputerProviderOption[], atCap = false): string | null {
  const open = providers.filter((p) => !isProviderLocked(p, atCap));
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

export function providerOptions(
  t: Translate,
  providers: readonly ComputerProviderOption[],
  atCap = false,
): RadioCardOption<string>[] {
  return providers.map((p) => {
    const lock = providerLock(p, atCap);
    return {
      value: p.kind,
      title: providerLabel(p.kind, t),
      description: providerDescription(p.kind, t),
      note: lock === 'not_on_plan'
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
export function thisMacOption(t: Translate, state: ThisMacCard): RadioCardOption<string> {
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
