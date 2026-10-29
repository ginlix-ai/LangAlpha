import { describe, it, expect, vi, beforeAll } from 'vitest';

import i18n from '@/i18n';
import type { ComputerSpecChange, SpecChangeErrorCode, UnsavedFile } from '@/types/api';

vi.mock('@/config/hostMode', () => ({
  HOST_MODE: 'platform',
  isPlatformMode: true,
  APP_ENTRY_PATH: '/app',
}));

import { specErrorFiles, specErrorFrom, specErrorFromOutcome } from '../specErrors';

const t = i18n.t.bind(i18n);

function failed(code: SpecChangeErrorCode, message = 'server sentence', files: UnsavedFile[] = []): ComputerSpecChange {
  return {
    target_tier: 'standard',
    from_tier: 'performance',
    state: 'failed',
    started_at: '2026-09-24T18:00:00Z',
    error: { code, message, files },
  };
}

function refused(status: number, detail: unknown) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });
}

beforeAll(async () => {
  await i18n.changeLanguage('en-US');
});

describe('specErrorFromOutcome', () => {
  it.each(['disk_too_small', 'not_allowed'] as const)('relays the server sentence for %s', (code) => {
    const message = 'Cannot downgrade: files on this computer (2.5 GiB) exceed what Standard holds. Free up space first.';
    expect(specErrorFromOutcome(failed(code, message), t)).toEqual({ message });
  });

  it('no longer claims an interrupted change left the spec as it was', () => {
    expect(specErrorFromOutcome(failed('interrupted'), t).message).toBe(
      "The spec change was interrupted before it finished. Your files are safe. Check the computer's current spec and try again.",
    );
  });

  it('keeps the translated copy for codes this client owns', () => {
    expect(specErrorFromOutcome(failed('turn_active'), t).message).toMatch(/^An Agent is working on this computer/);
    expect(specErrorFromOutcome(failed('busy'), t).message).toMatch(/^The computer was busy/);
  });

  it('names the files a backup could not take, and says less when it names none', () => {
    const files = [{ path: 'data/ticks.parquet', reason: 'too_large' as const, size: 6_549_825_126 }];
    const named = specErrorFromOutcome(failed('backup_incomplete', 'x', files), t);
    expect(named.message).toMatch(/these files could not be backed up/);
    expect(specErrorFiles(named.files, t, 'en-US')).toEqual([
      'data/ticks.parquet: Too large to back up here (6.1 GB). Move or split it.',
    ]);
    const unnamed = specErrorFromOutcome(failed('backup_incomplete'), t);
    expect(unnamed).toEqual({
      message: "Nothing was changed. This computer's files could not be backed up first. Try again in a moment.",
    });
  });

  it('falls back to the generic failure for unknown, and for a failure with no error', () => {
    const generic = 'The spec change failed. Your files are safe and the computer stays at its previous spec.';
    expect(specErrorFromOutcome(failed('unknown'), t).message).toBe(generic);
    expect(specErrorFromOutcome({ ...failed('unknown'), error: null }, t).message).toBe(generic);
  });
});

describe('specErrorFrom', () => {
  it('maps a 409 by its code', () => {
    expect(specErrorFrom(refused(409, { code: 'spec_in_progress', message: 'x' }), t, 'performance').message).toBe(
      'A spec change is already running on this computer.',
    );
  });

  it('reads a plan refusal as entitlement copy', () => {
    expect(specErrorFrom(refused(403, 'Forbidden'), t, 'max').message).toBe(i18n.t('workspace.notOnPlan'));
  });

  it('names the size whose count is spent when the platform gives no sentence', () => {
    expect(specErrorFrom(refused(429, null), t, 'xlarge').message).toBe(
      "You've reached your Extra Large workspace limit.",
    );
  });

  it("relays the server's sentence for a size the provider does not run", () => {
    const detail = 'E2B is the only provider that runs the xlarge tier.';
    expect(specErrorFrom(refused(400, detail), t, 'xlarge').message).toBe(detail);
  });
});
