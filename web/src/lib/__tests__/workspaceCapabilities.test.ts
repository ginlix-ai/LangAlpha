// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { capabilitiesOf } from '../workspaceCapabilities';
import type { SharedWorkspaceEntry } from '@/types/sharedWorkspaces';

const entry = { workspace: { workspace_id: 'ws-1' }, shared: { role: 'member' } } as SharedWorkspaceEntry;
const WITHHELD = { memos: false, userMemory: false, restoreOffer: false, machineSettings: false, claudeCode: false, skills: false };

describe('capabilitiesOf', () => {
  it('offers a personal workspace everything, as it always had', () => {
    expect(capabilitiesOf(null)).toEqual({
      memos: true, userMemory: true, restoreOffer: true, machineSettings: true, claudeCode: true, skills: true,
      placeholderKey: 'chat.placeholderDefault',
    });
  });

  it("withholds the holder's stores, settings, skills and Claude Code from a shared workspace", () => {
    expect(capabilitiesOf(entry)).toEqual({ ...WITHHELD, placeholderKey: 'sharedWorkspace.placeholder' });
  });

  it('holds them back while the list is read, with the default placeholder', () => {
    expect(capabilitiesOf(undefined)).toEqual({ ...WITHHELD, placeholderKey: 'chat.placeholderDefault' });
  });
});
