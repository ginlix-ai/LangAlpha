// @vitest-environment node
/**
 * Where the agent runs is the user's call, so the step picks nothing for them
 * unless there is one card left to pick. A required group stays on whatever
 * asks it off, and it is the switch the user reads as locked.
 */
import { describe, expect, it } from 'vitest';
import type { InstallGroup, SetupStatus } from '@/api/setup';
import {
  initialCard,
  initialCloudKind,
  mustChooseComputer,
  selectedGroupIds,
  selectedGroupsSize,
  shownCards,
  toggleInstallGroup,
} from '../sandboxChoice';

const GROUPS: InstallGroup[] = [
  { id: 'core', required: true, selected: true, download_mb: 200, disk_mb: 800 },
  { id: 'browsing', required: false, selected: true, download_mb: 100, disk_mb: 400 },
  { id: 'chromium', required: false, selected: false, download_mb: 300, disk_mb: 500 },
];

function status(overrides: {
  local?: boolean;
  docker?: boolean;
  cloud?: boolean;
  provider?: string;
  source?: SetupStatus['sandbox']['provider_source'];
  completed?: boolean;
  daytonaKey?: boolean;
  e2bKey?: boolean;
} = {}): SetupStatus {
  const { local = true, docker = true, cloud = true, provider = 'docker', source = 'detected', completed = false } = overrides;
  return {
    completed,
    needs_setup: !completed,
    sandbox: {
      provider,
      provider_source: source,
      options: [
        { kind: 'local', available: local, detail: local ? null : 'not_offered', install_groups: GROUPS },
        { kind: 'docker', available: docker, detail: docker ? null : 'daemon_unreachable' },
        { kind: 'daytona', available: cloud, detail: null, key_set: overrides.daytonaKey ?? false },
        { kind: 'e2b', available: cloud, detail: null, key_set: overrides.e2bKey ?? false },
      ],
    },
    keys: [],
  };
}

describe('the card picked when the step opens', () => {
  it('picks nothing on a fresh server, whatever detection would run', () => {
    expect(initialCard(status())).toBeNull();
  });

  it('picks the one card that can be picked', () => {
    expect(initialCard(status({ local: false, docker: false }))).toBe('cloud');
    expect(initialCard(status({ local: false, cloud: false }))).toBe('docker');
  });

  it('shows a saved choice as made, a cloud kind on the cloud card', () => {
    expect(initialCard(status({ provider: 'local', source: 'saved' }))).toBe('local');
    expect(initialCard(status({ provider: 'e2b', source: 'saved' }))).toBe('cloud');
  });

  it('shows what is in effect once setup has been finished', () => {
    expect(initialCard(status({ provider: 'docker', completed: true }))).toBe('docker');
  });

  it('does not pick a choice that can no longer run', () => {
    expect(initialCard(status({ provider: 'docker', source: 'saved', docker: false }))).toBeNull();
  });
});

describe('the cards shown', () => {
  it('leaves this computer out where the server does not offer it, and says why for the rest', () => {
    expect(shownCards(status())).toEqual(['local', 'docker', 'cloud']);
    expect(shownCards(status({ local: false }))).toEqual(['docker', 'cloud']);
    expect(shownCards(status({ local: false, docker: false }))).toEqual(['docker', 'cloud']);
  });
});

describe('the cloud vendor', () => {
  it('follows a saved choice, then a key already held', () => {
    expect(initialCloudKind(status({ provider: 'e2b', source: 'saved' }))).toBe('e2b');
    expect(initialCloudKind(status({ e2bKey: true }))).toBe('e2b');
    expect(initialCloudKind(status())).toBe('daytona');
  });
});

describe('install groups', () => {
  it('toggles an optional group and never a required one', () => {
    const off = toggleInstallGroup(GROUPS, 'browsing');
    expect(selectedGroupIds(off)).toEqual(['core']);
    expect(selectedGroupIds(toggleInstallGroup(off, 'browsing'))).toEqual(['core', 'browsing']);
    expect(selectedGroupIds(toggleInstallGroup(GROUPS, 'core'))).toEqual(['core', 'browsing']);
  });

  it('totals what is switched on, and gives no total missing a part', () => {
    expect(selectedGroupsSize(GROUPS)).toEqual({ downloadMb: 300, diskMb: 1200 });
    expect(selectedGroupsSize(toggleInstallGroup(GROUPS, 'chromium'))).toEqual({ downloadMb: 600, diskMb: 1700 });
    expect(selectedGroupsSize([{ id: 'core', required: true, selected: true }])).toBeNull();
  });
});

describe('mustChooseComputer', () => {
  const unrunnable = (sandbox: Partial<SetupStatus['sandbox']> = {}, needsSetup = true): SetupStatus => ({
    ...status({ docker: false }),
    needs_setup: needsSetup,
    sandbox: { ...status({ docker: false }).sandbox, ...sandbox },
  });

  it('holds the exit while the fallback cannot run and nothing is chosen', () => {
    expect(mustChooseComputer(unrunnable())).toBe(true);
  });

  it('lets go once the fallback can run, or a choice or the server names one', () => {
    expect(mustChooseComputer(unrunnable({ provider: 'local' }))).toBe(false);
    expect(mustChooseComputer(unrunnable({ provider_source: 'saved' }))).toBe(false);
    expect(mustChooseComputer(unrunnable({ provider_source: 'environment' }))).toBe(false);
  });

  it('never holds a finished server, or one whose status could not be read', () => {
    expect(mustChooseComputer(unrunnable({}, false))).toBe(false);
    expect(mustChooseComputer(undefined)).toBe(false);
  });
});
