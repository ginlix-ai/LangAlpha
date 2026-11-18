import { describe, expect, it } from 'vitest';
import type { ComputerProviderOption } from '@/types/api';
import { defaultProviderKind, isProviderLocked, providerLock } from '../providerUi';

const daytona: ComputerProviderOption = { kind: 'daytona', is_default: true, allowed: true };
const ssh: ComputerProviderOption = { kind: 'ssh', is_default: false, allowed: true };
const open = { atCap: false, ownMachinesRule: null };
const ruled = { atCap: false, ownMachinesRule: 'Northfield does not allow your own machines.' };

describe('the provider gate', () => {
  it('locks a machine the user brings by the rule, ahead of the plan', () => {
    expect(providerLock(ssh, ruled)).toBe('rule');
    expect(providerLock({ ...ssh, allowed: false }, ruled)).toBe('rule');
    expect(providerLock(ssh, open)).toBeNull();
    expect(providerLock(daytona, ruled)).toBeNull();
  });

  it('at the cap leaves only a machine the user brings, unless the rule takes that too', () => {
    expect(defaultProviderKind([daytona, ssh], { ...open, atCap: true })).toBe('ssh');
    expect([daytona, ssh].some((p) => !isProviderLocked(p, { ...ruled, atCap: true }))).toBe(false);
    expect(defaultProviderKind([daytona, ssh], { ...ruled, atCap: true })).toBeNull();
  });
});
