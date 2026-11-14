// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { LA_STEP_LABELS, laLookupId, laStepOf, parseLaCommand, shellWords } from '../laCommand';
import { LA_COMMANDS } from '../laGrammar.generated';

const ID = '346bee3b-8f31-43d3-b8a3-f6489e03d7aa';
const WS = '0f1e2d3c-4b5a-4968-8776-655443322110';

describe('shellWords', () => {
  it('splits on blanks and removes quotes the way the shell does', () => {
    expect(shellWords(`la workspaces create 'Model test'`)).toEqual(['la', 'workspaces', 'create', 'Model test']);
    expect(shellWords(`la workspaces create "Model test"`)).toEqual(['la', 'workspaces', 'create', 'Model test']);
    expect(shellWords(`la workspaces create Model\\ test`)).toEqual(['la', 'workspaces', 'create', 'Model test']);
    expect(shellWords(`a'b'"c"d`)).toEqual(['abcd']);
    expect(shellWords(`la  x\t y `)).toEqual(['la', 'x', 'y']);
    expect(shellWords(`la ''`)).toEqual(['la', '']);
  });

  it('keeps what quotes make literal', () => {
    expect(shellWords(`la 'a | b; $HOME && c'`)).toEqual(['la', 'a | b; $HOME && c']);
    expect(shellWords(`la "it's \\"done\\" \\$5"`)).toEqual(['la', `it's "done" $5`]);
    expect(shellWords(`la "a\\nb"`)).toEqual(['la', 'a\\nb']);
    expect(shellWords(`la 'two\nlines'`)).toEqual(['la', 'two\nlines']);
    expect(shellWords(`la one\\\ntwo`)).toEqual(['la', 'onetwo']);
  });

  it.each([
    ['a pipe', 'la threads list | head'],
    ['an or', 'la threads list || true'],
    ['an and', 'la threads list && echo ok'],
    ['a semicolon', 'la threads list; echo ok'],
    ['a background job', 'la threads list &'],
    ['a redirection', 'la threads list > out.txt'],
    ['a stderr redirection', 'la threads list 2>&1'],
    ['an input redirection', 'la threads list < in'],
    ['a heredoc', 'la threads send x <<EOF'],
    ['a command substitution', 'la threads output $(cat id)'],
    ['backticks', 'la threads output `cat id`'],
    ['a variable', 'la threads output $ID'],
    ['a braced variable', 'la threads output ${ID}'],
    ['a variable in double quotes', 'la threads send x "hi $USER"'],
    ['a substitution in double quotes', 'la threads send x "hi $(whoami)"'],
    ['a newline', 'la threads list\nla workspaces list'],
    ['a subshell', '(la threads list)'],
    ['a glob', 'la threads output *'],
    ['a tilde', 'la workspaces create ~/x'],
    ['a comment', 'la threads list # all'],
    ['a brace expansion', 'la threads output {a,b}'],
    ['history', 'la threads send x hi!'],
    ['an unclosed single quote', "la workspaces create 'Model"],
    ['an unclosed double quote', 'la workspaces create "Model'],
    ['a trailing backslash', 'la threads list \\'],
  ])('refuses %s', (_label, command) => {
    expect(shellWords(command)).toBeNull();
  });
});

describe('parseLaCommand', () => {
  it('reads each command in scope', () => {
    expect(parseLaCommand('la workspaces list --include-home')).toEqual({ kind: 'listWorkspaces', target: null });
    expect(parseLaCommand(`la workspaces create 'Model test'`)).toEqual({
      kind: 'createWorkspace', target: { type: 'workspace', ref: { name: 'Model test' } },
    });
    expect(parseLaCommand(`la workspaces archive ${WS}`)?.kind).toBe('archiveComputer');
    expect(parseLaCommand(`la workspaces delete ${WS} -y --delete-files`)).toEqual({
      kind: 'deleteWorkspace', target: { type: 'workspace', ref: { id: WS } },
    });
    expect(parseLaCommand('la threads list --limit 5')).toEqual({ kind: 'listThreads', target: null });
    expect(parseLaCommand(`la threads show ${ID}`)?.kind).toBe('showThread');
    expect(parseLaCommand(`la threads output ${ID}`)).toEqual({ kind: 'readThread', target: { type: 'thread', id: ID } });
    expect(parseLaCommand(`la threads new -w 'NVDA deep dive' Value NVDA`)).toEqual({
      kind: 'startThread', target: { type: 'workspace', ref: { name: 'NVDA deep dive' } },
    });
    expect(parseLaCommand(`la threads send ${ID} 'Keep going' --detach`)).toEqual({
      kind: 'sendToThread', target: { type: 'thread', id: ID },
    });
    expect(parseLaCommand(`la threads follow ${ID}`)?.kind).toBe('followThread');
    expect(parseLaCommand(`la threads retry ${ID} -m gpt-5`)?.kind).toBe('retryThread');
    expect(parseLaCommand(`la threads cancel ${ID}`)?.kind).toBe('cancelThread');
    expect(parseLaCommand(`la threads archive ${ID}`)?.kind).toBe('archiveThread');
    expect(parseLaCommand(`la threads delete ${ID} -y`)).toEqual({ kind: 'deleteThread', target: { type: 'thread', id: ID } });
    expect(parseLaCommand('la computers list')).toEqual({ kind: 'listComputers', target: null });
    expect(parseLaCommand('la models list')).toEqual({ kind: 'listModels', target: null });
    expect(parseLaCommand('la automations list --status active')).toEqual({ kind: 'listAutomations', target: null });
  });

  it('takes the long name and a plain path to either', () => {
    expect(parseLaCommand('langalpha workspaces list')?.kind).toBe('listWorkspaces');
    expect(parseLaCommand('/usr/local/bin/la workspaces list')?.kind).toBe('listWorkspaces');
    expect(parseLaCommand('./langalpha models list')?.kind).toBe('listModels');
    expect(parseLaCommand('  la models list  ')?.kind).toBe('listModels');
    expect(parseLaCommand('lab models list')).toBeNull();
    expect(parseLaCommand('ls -la')).toBeNull();
    expect(parseLaCommand('python la models list')).toBeNull();
    expect(parseLaCommand('command la models list')).toBeNull();
  });

  it('refuses an env prefix and any shell syntax', () => {
    expect(parseLaCommand('LANGALPHA_TOKEN=x la workspaces list')).toBeNull();
    expect(parseLaCommand('la workspaces list | jq .')).toBeNull();
    expect(parseLaCommand('la workspaces list --json > ws.json')).toBeNull();
    expect(parseLaCommand(`la threads output ${ID} && la threads list`)).toBeNull();
    expect(parseLaCommand('cd /tmp && la threads list')).toBeNull();
    expect(parseLaCommand('')).toBeNull();
  });

  it('reads -w as an id, Home, or a name, and a new workspace only by name', () => {
    expect(parseLaCommand(`la threads list -w ${WS}`)?.target).toEqual({ type: 'workspace', ref: { id: WS } });
    expect(parseLaCommand('la threads list --workspace=HOME')?.target).toEqual({ type: 'workspace', ref: { home: true } });
    expect(parseLaCommand(`la threads list -w 'NVDA deep dive'`)?.target).toEqual({ type: 'workspace', ref: { name: 'NVDA deep dive' } });
    // A new workspace's name is a name even when it looks like an id.
    expect(parseLaCommand(`la workspaces create ${WS}`)?.target).toEqual({ type: 'workspace', ref: { name: WS } });
  });

  it('reads the words by the CLI grammar, and is null where the CLI would refuse them', () => {
    expect(parseLaCommand(`la --server http://localhost:8000 threads delete -y ${ID} --json`)).toEqual({
      kind: 'deleteThread', target: { type: 'thread', id: ID },
    });
    expect(parseLaCommand('la threads output')).toBeNull();
    expect(parseLaCommand('la threads list --bogus')).toBeNull();
    expect(parseLaCommand(`la threads send ${ID} hi --wait --detach`)).toBeNull();
    expect(parseLaCommand('la workspaces')).toBeNull();
  });

  it('is null for a command it has no label for, and for help or the version', () => {
    expect(parseLaCommand('la threads approve x')).toBeNull();
    expect(parseLaCommand('la workspaces list --help')).toBeNull();
    expect(parseLaCommand('la -h workspaces list')).toBeNull();
    expect(parseLaCommand('la --version')).toBeNull();
  });
});

describe('the step labels', () => {
  it('name only commands the CLI has, by an operand or flag each one takes', () => {
    for (const [words, label] of Object.entries(LA_STEP_LABELS)) {
      const spec = LA_COMMANDS.find((c) => c.words.join(' ') === words);
      expect(spec, words).toBeDefined();
      if (!label.target) continue;
      const names = [...(spec!.operands ?? []).map((o) => o.replace(/\.\.\.$/, '')), ...Object.keys(spec!.flags ?? {})];
      expect(names, words).toContain(label.target.from);
    }
  });
});

describe('laStepOf and laLookupId', () => {
  it('reads only a Bash call with a string command', () => {
    expect(laStepOf('Bash', { command: 'la models list' })?.kind).toBe('listModels');
    expect(laStepOf('ExecuteCode', { command: 'la models list' })).toBeNull();
    expect(laStepOf('Bash', { command: 42 })).toBeNull();
    expect(laStepOf('Bash', undefined)).toBeNull();
    // Blanked on a public share: the step reads as a plain Bash call.
    expect(laStepOf('Bash', {})).toBeNull();
  });

  it('looks up by id only', () => {
    expect(laLookupId(parseLaCommand(`la threads output ${ID}`))).toEqual({ type: 'thread', id: ID });
    expect(laLookupId(parseLaCommand(`la workspaces delete ${WS} -y`))).toEqual({ type: 'workspace', id: WS });
    expect(laLookupId(parseLaCommand(`la workspaces create x`))).toBeNull();
    expect(laLookupId(parseLaCommand('la threads list -w home'))).toBeNull();
    expect(laLookupId(parseLaCommand('la models list'))).toBeNull();
  });
});
