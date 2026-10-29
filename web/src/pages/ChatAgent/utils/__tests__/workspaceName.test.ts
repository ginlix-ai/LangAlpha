/**
 * A refused workspace name reads in the user's language wherever it lands:
 * the create form, both rename surfaces and the duplicate toast. The server's
 * `code` decides the copy; its English sentence is only a fallback.
 */
import { beforeAll, describe, expect, it } from 'vitest';

import i18n from '@/i18n';

import { denialMessage } from '../denialMessage';
import { workspaceNameErrorMessage } from '../workspaceName';

const t = i18n.t.bind(i18n);

function refused(status: number, detail: unknown) {
  return Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });
}

const TAKEN = refused(409, {
  code: 'workspace_name_taken',
  message: 'A workspace named "Research" already exists.',
  name: 'Research',
  workspace_id: '0b7c1d2e-3f40-4a5b-8c6d-7e8f90a1b2c3',
});

const INVALID = refused(400, {
  code: 'workspace_name_invalid',
  message: 'Workspace name "_internal" is reserved.',
});

beforeAll(async () => {
  await i18n.changeLanguage('en-US');
});

describe('workspaceNameErrorMessage', () => {
  it('names the workspace already holding the name', () => {
    expect(workspaceNameErrorMessage(TAKEN, t)).toBe(
      'A workspace named "Research" already exists. Choose another name.',
    );
  });

  it('tells a duplicate to try again, since the user never chose that name', () => {
    expect(workspaceNameErrorMessage(TAKEN, t, 'duplicate')).toBe(
      'A workspace named "Research" was created at the same moment. Try again.',
    );
  });

  it('words a refusal by its reason', () => {
    const invalid = (extra: Record<string, unknown>) =>
      refused(400, { code: 'workspace_name_invalid', message: 'x', ...extra });
    expect(workspaceNameErrorMessage(invalid({ reason: 'reserved', name: 'tools' }), t)).toBe(
      '"tools" is a reserved folder name. Choose another name.',
    );
    expect(workspaceNameErrorMessage(invalid({ reason: 'empty' }), t)).toBe(
      'A workspace name needs at least one letter or digit.',
    );
    expect(workspaceNameErrorMessage(invalid({ reason: 'too_long' }), t)).toBe(
      'A workspace name can be at most 80 characters.',
    );
  });

  it('asks a rename to wait while the folder is moving', () => {
    const moving = refused(409, { code: 'workspace_folder_moving', message: 'moving' });
    expect(workspaceNameErrorMessage(moving, t)).toBe(
      "This workspace's folder is being moved. Try renaming it again in a moment.",
    );
  });

  it('translates an unusable name', () => {
    expect(workspaceNameErrorMessage(INVALID, t)).toBe(
      "That name can't be used for a workspace. Choose another name.",
    );
  });

  it('reads in the user\'s language', () => {
    const zh = i18n.getFixedT('zh-CN');
    expect(workspaceNameErrorMessage(TAKEN, zh)).toBe('已存在名为“Research”的工作区，请换一个名称。');
  });

  it('leaves every other failure to its own reader', () => {
    expect(workspaceNameErrorMessage(refused(409, 'conflict'), t)).toBeNull();
    expect(workspaceNameErrorMessage(refused(429, { message: 'Quota exhausted' }), t)).toBeNull();
    expect(workspaceNameErrorMessage(refused(422, [{ loc: ['body', 'name'], msg: 'too long' }]), t)).toBeNull();
    expect(workspaceNameErrorMessage(new Error('Network Error'), t)).toBeNull();
    // A taken name the body did not spell out has nothing to interpolate.
    expect(workspaceNameErrorMessage(refused(409, { code: 'workspace_name_taken', message: 'taken' }), t)).toBeNull();
  });
});

describe('denialMessage', () => {
  it('reads a refused workspace name by its code', () => {
    expect(denialMessage(TAKEN, t)).toBe('A workspace named "Research" already exists. Choose another name.');
    expect(denialMessage(INVALID, t)).toBe("That name can't be used for a workspace. Choose another name.");
  });

  it('relays the sentence of a structured refusal it has no copy for', () => {
    const bare = refused(409, { code: 'workspace_name_taken', message: 'A workspace named "Research" already exists.' });
    expect(denialMessage(bare, t)).toBe('A workspace named "Research" already exists.');
  });
});
