// @vitest-environment node
/**
 * What the deliverables strip is allowed to claim a turn produced.
 *
 * Both halves of a turn lie on their own: the reply names files a script never
 * wrote and links the same one twice, and the tool calls name scratch paths the
 * reply deliberately never mentioned. The strip is what the two agree on.
 */
import { describe, it, expect } from 'vitest';
import { collectTurnFiles, turnFilesByTurn } from '../turnFiles';
import type { ToolCallLike } from '../fileRefResolver';
import type { ComputerFolders } from '../agentPaths';

/** The viewed workspace's folders, and the other workspaces on its computer. */
function own(dirName: string, previousDirNames?: string[], siblings: ComputerFolders['siblings'] = []): ComputerFolders {
  return { dirName, previousDirNames, siblings };
}

function assistant(text: string, toolCallProcesses: Record<string, ToolCallLike> = {}) {
  return { role: 'assistant', contentSegments: [{ type: 'text', content: text }], toolCallProcesses };
}

// `toolCallResult` is what the transcript stores when a call returns, and the
// only such field: a stop marks every open call `isComplete` with no result.
function write(order: number, path: string) {
  return {
    toolName: 'Write', order, isComplete: true, toolCallResult: { content: 'ok' },
    toolCall: { args: { file_path: path, content: 'x' } },
  };
}

function edit(order: number, path: string, oldString: string, newString: string) {
  return {
    toolName: 'Edit', order, isComplete: true, toolCallResult: { content: 'ok' },
    toolCall: { args: { file_path: path, old_string: oldString, new_string: newString } },
  };
}

describe('collectTurnFiles', () => {
  it('lists the files the reply links, in the order it names them', () => {
    const files = collectTurnFiles([
      assistant('See [the model](results/model.docx) and [the notes](notes/summary.md).'),
    ]);
    expect(files.map((f) => f.path)).toEqual(['results/model.docx', 'notes/summary.md']);
  });

  it('keeps a file only a write tool named', () => {
    const files = collectTurnFiles([
      assistant('Done, see [the chart](results/chart.html).', { a: write(0, 'results/chart.html'), b: write(1, 'data/prices.csv') }),
    ]);
    expect(files.map((f) => f.path)).toEqual(['results/chart.html', 'data/prices.csv']);
  });

  it('counts an edit as the lines it changed, not the anchor lines it carried', () => {
    const files = collectTurnFiles([
      assistant('Updated [the model](report.md).', {
        a: edit(0, 'report.md', 'def run():\n    old()\n    return 1', 'def run():\n    new()\n    also()\n    return 1'),
      }),
    ]);
    expect(files[0].stats).toEqual({ added: 2, removed: 1 });
  });

  it('sums the edits a turn made to one file and keeps it listed once', () => {
    const files = collectTurnFiles([
      assistant('', { a: edit(0, 'report.md', 'a', 'b'), b: edit(1, 'report.md', 'c', 'd\ne') }),
      assistant('Then [the model](report.md) was ready.'),
    ]);
    expect(files).toHaveLength(1);
    expect(files[0].stats).toEqual({ added: 3, removed: 2 });
  });

  it('counts a deletion as removed lines only, with no phantom line added', () => {
    // `''.split('\n')` is one element, so an empty replacement used to report
    // the line it did not add. A trailing newline is the other half: it ends
    // the last line rather than opening an empty one, and a fix that maps `''`
    // to no lines without handling it turns this pair of counts back around.
    const cut = collectTurnFiles([
      assistant('Trimmed [the model](report.md).', { a: edit(0, 'report.md', '    old()\n    stale()', '') }),
    ]);
    expect(cut[0].stats).toEqual({ added: 0, removed: 2 });

    const cutLine = collectTurnFiles([
      assistant('Trimmed [the model](report.md).', { a: edit(0, 'report.md', '    stale()\n', '') }),
    ]);
    expect(cutLine[0].stats).toEqual({ added: 0, removed: 1 });
  });

  it('reports no line count for a write, which never says what it replaced', () => {
    const files = collectTurnFiles([assistant('', { a: write(0, 'results/report.md') })]);
    expect(files[0].stats).toBeUndefined();
  });

  it('drops an image the reply shows, however it was written', () => {
    // The renderer draws `[name](x.png)` as an image too, so both forms are
    // already on screen and a card for either points at what the reader sees.
    const files = collectTurnFiles([
      assistant('![chart](results/chart.png)\nAlso [the raw plot](results/plot.png), from [the data](data/prices.csv).', {
        a: write(0, 'results/chart.png'),
        b: write(1, 'results/plot.png'),
        c: write(2, 'data/prices.csv'),
      }),
    ]);
    expect(files.map((f) => f.path)).toEqual(['data/prices.csv']);
  });

  it('keeps a name the reply wrote with parentheses in it', () => {
    // CommonMark allows one level of balanced parens in a bare destination, so
    // the reply's own link renders and opens. The card has to agree, and the
    // embedded image has to stay suppressed rather than earn a duplicate.
    const files = collectTurnFiles([
      assistant('![chart](results/chart(1).png)\nSee [the report](results/report(1).pdf).', {
        a: write(0, 'results/chart(1).png'),
        b: write(1, 'results/report(1).pdf'),
      }),
    ]);
    expect(files.map((f) => f.path)).toEqual(['results/report(1).pdf']);
  });

  it('counts a macro workbook as a deliverable, as the panel and the relay do', () => {
    const files = collectTurnFiles([assistant('See [the model](results/model.xlsm).')]);
    expect(files.map((f) => f.path)).toEqual(['results/model.xlsm']);
  });

  it('keeps the spot in the file the reply pointed at', () => {
    const files = collectTurnFiles([assistant('See [the section](notes/plan.md#L42).')]);
    expect(files[0].location).toEqual({ line: 42 });
  });

  it('carries the workspace a Chief of Staff relay named', () => {
    const files = collectTurnFiles([assistant('See [the model](__wsref__/ws-7/results/model.md).')]);
    expect(files[0]).toMatchObject({ path: 'results/model.md', workspaceId: 'ws-7' });
  });

  it('lists what a person opens and leaves the agent\'s own scaffolding out', () => {
    const files = collectTurnFiles([
      assistant('Ran [the fetcher](scripts/fetch_prices.py) and [the job](run.sh), wrote [the memo](results/memo.md).', {
        a: write(0, 'scripts/fetch_prices.py'),
        b: write(1, 'notebooks/scratch.ipynb'),
        c: write(2, 'results/raw.json'),
        d: write(3, 'results/memo.md'),
        e: write(4, 'results/comps.csv'),
      }),
    ]);
    expect(files.map((f) => f.path)).toEqual(['results/memo.md', 'results/comps.csv']);
  });

  it('leaves the agent\'s notes file out, wherever the workspace keeps it', () => {
    const files = collectTurnFiles([
      assistant('Updated [the report](weekly/report.md) and [my notes](/home/workspace/agent.md).', {
        a: write(0, 'weekly/report.md'),
        b: write(1, 'agent.md'),
        c: write(2, '/home/workspace/alpha/agent.md'),
      }),
    ], own('alpha'));
    expect(files.map((f) => f.path)).toEqual(['weekly/report.md']);
  });

  it('keeps an agent.md the user asked for in a subfolder', () => {
    const files = collectTurnFiles([
      assistant('Drafted [the spec](reports/agent.md).', {
        a: write(0, 'docs/agent.md'),
        b: write(1, './agent.md'),
        c: write(2, 'reports/agent.md'),
      }),
    ]);
    expect(files.map((f) => f.path)).toEqual(['reports/agent.md', 'docs/agent.md']);
  });

  it('keeps an agent.md in a folder that is not the workspace\'s own', () => {
    const files = collectTurnFiles([
      assistant('Wrote two.', {
        a: write(0, '/home/workspace/docs/agent.md'),
        b: write(1, '/tmp/agent.md'),
        c: write(2, '/home/workspace/alpha/agent.md'),
      }),
    ], own('alpha'));
    expect(files.map((f) => f.path)).toEqual(['docs/agent.md', '/tmp/agent.md']);
  });

  it('keeps a project-folder agent.md until the folder name is known', () => {
    const turn = [assistant('Wrote it.', { a: write(0, '/home/workspace/alpha/agent.md') })];
    expect(collectTurnFiles(turn).map((f) => f.path)).toEqual(['alpha/agent.md']);
    expect(collectTurnFiles(turn, own('alpha'))).toEqual([]);
  });

  it('leaves out the notes file a turn wrote before the workspace was renamed', () => {
    const files = collectTurnFiles([
      assistant('Updated [my notes](/home/workspace/research-ab12/agent.md) and [the report](weekly/report.md).', {
        a: write(0, '/home/workspace/research-ab12/agent.md'),
        b: write(1, '/home/workspace/Research/agent.md'),
        c: write(2, '/home/workspace/docs/agent.md'),
      }),
    ], own('Research', ['research-ab12']));
    expect(files.map((f) => f.path)).toEqual(['weekly/report.md', 'docs/agent.md']);
  });

  it('names a file under the workspace folder by its path inside it', () => {
    // The card opens what it names, relative to the folder: `Research/weekly/…`
    // read from there is a folder nested in the workspace, which does not exist.
    const files = collectTurnFiles([
      assistant('See [the report](/home/workspace/Research/weekly/report.md).', {
        a: write(0, 'weekly/report.md'),
        b: write(1, '/home/workspace/research-ab12/data/prices.csv'),
      }),
    ], own('Research', ['research-ab12']));
    expect(files.map((f) => f.path)).toEqual(['weekly/report.md', 'data/prices.csv']);
  });

  it('reads a path through the workspace\'s own folder from its root', () => {
    // From the working directory `../Home/x` is `x`. Read as written it climbs
    // out of the workspace, so the reply's link earned no card and its chart
    // never counted as embedded.
    const turn = [
      assistant(
        'See [the comparison](../Home/e2e_probe_comparison/probe_notes_comparison.md), [my notes](../Home/agent.md) and ![chart](../Home/charts/a.png).',
        {
          a: write(0, 'e2e_probe_comparison/probe_notes_comparison.md'),
          b: write(1, 'charts/a.png'),
          c: write(2, '../Home/data/prices.csv'),
        },
      ),
    ];
    expect(collectTurnFiles(turn).map((f) => f.path))
      .toEqual(['e2e_probe_comparison/probe_notes_comparison.md', 'charts/a.png']);
    expect(collectTurnFiles(turn, own('Home'))).toEqual([
      { path: 'e2e_probe_comparison/probe_notes_comparison.md' },
      { path: 'data/prices.csv' },
    ]);
  });

  it.each([
    ['climbed into', '../Home/report.md', '../NVDA/report.md'],
    ['rooted', '/home/workspace/Home/report.md', '/home/workspace/NVDA/report.md'],
  ])('tells the workspace\'s own folder from a sibling\'s, %s', (_how, ours, theirs) => {
    const files = collectTurnFiles(
      [assistant(`Compare [ours](${ours}) with [theirs](${theirs}).`)],
      own('Home', [], [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }]),
    );
    expect(files).toEqual([{ path: 'report.md' }, { path: 'report.md', workspaceId: 'ws-nvda' }]);
  });

  it('reads paths under the computer\'s own root as it reads the stock root', () => {
    // A computer whose user cannot create the stock root keeps its folders
    // under one of its own, and the reply's absolute links start there.
    const root = '/home/someone/langalpha';
    const files = collectTurnFiles(
      [assistant(
        `See [the report](file://${root}/Home/weekly/report.md), [my notes](${root}/Home/agent.md) and [theirs](${root}/NVDA/report.md).`,
        { a: write(0, `${root}/Home/data/prices.csv`) },
      )],
      { ...own('Home', [], [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }]), rootDir: root },
    );
    expect(files).toEqual([
      { path: 'weekly/report.md' },
      { path: 'report.md', workspaceId: 'ws-nvda' },
      { path: 'data/prices.csv' },
    ]);
  });

  it('opens a file on another computer in the workspace whose folder holds it, and nothing else there', () => {
    // The Chief of Staff links a file there at its whole path on that machine.
    // The rest of that machine is not one this view reads: a card would read
    // the path on this computer and, missing, offer a file that shares its name.
    const ssh = '/home/nosudo/langalpha-2';
    const folders: ComputerFolders = {
      ...own('Home', [], [{ workspaceId: 'ws-nvda', dirName: 'NVDA' }]),
      otherRoots: [ssh],
      otherComputers: [{ root: ssh, workspaces: [{ workspaceId: 'ws-ssh', dirName: 'ssh-e2e' }] }],
    };
    const files = collectTurnFiles(
      [assistant(
        `See [q3](${ssh}/ssh-e2e/reports/q3.md), [the deck](file://${ssh}/ssh-e2e/deck.pptx), `
        + `[its notes](${ssh}/ssh-e2e/agent.md), [a stray](${ssh}/scratch/x.csv) and [a local one](/tmp/out.csv).`,
        { a: write(0, `${ssh}/scratch/y.csv`), b: write(1, `${ssh}/ssh-e2e/data/z.csv`) },
      )],
      folders,
    );
    expect(files).toEqual([
      { path: 'reports/q3.md', workspaceId: 'ws-ssh' },
      { path: 'deck.pptx', workspaceId: 'ws-ssh' },
      { path: '/tmp/out.csv' },
      { path: 'data/z.csv', workspaceId: 'ws-ssh' },
    ]);
  });

  it('gives no card to a write that names a computer, which wrote on that one', () => {
    // A relative path there is under that computer's root, and a card would
    // read it on this one, where a file of that name may well exist.
    const onIt = (order: number, path: string) => {
      const call = write(order, path);
      return { ...call, toolCall: { args: { ...call.toolCall.args, computer: 'final-qa' } } };
    };
    const files = collectTurnFiles([
      assistant('', { a: onIt(0, 'report.md'), b: onIt(1, '/home/nosudo/langalpha-2/ssh-e2e/z.csv'), c: write(2, 'notes.md') }),
    ]);
    expect(files).toEqual([{ path: 'notes.md' }]);
  });

  it('leaves out system paths, section links and folders', () => {
    const files = collectTurnFiles([
      assistant('[skills](.agents/skills/report/SKILL.md), [a section](#findings), [a folder](results/), [a route](/settings)'),
    ]);
    expect(files).toEqual([]);
  });

  it('ignores a failed write and a user message', () => {
    const files = collectTurnFiles([
      { role: 'user', content: 'write [the model](report.md)' },
      assistant('', { a: { ...write(0, 'report.md'), isFailed: true } }),
    ]);
    expect(files).toEqual([]);
  });

  it('claims nothing for a write still in flight when the turn stopped', () => {
    // A stopped turn settles with its last call unanswered, and the stop hand
    // marks it complete anyway (`useChatMessages` folds every open call). The
    // call names a path, but no result says the file was written, and a card is
    // a claim that it was.
    const pending = {
      toolName: 'Write', order: 0, isComplete: true,
      toolCall: { args: { file_path: 'results/never.md' } },
    };
    expect(collectTurnFiles([assistant('Saving it now.', { a: pending })])).toEqual([]);
  });

  it('leaves out a link that climbs above the workspace root', () => {
    // A reply's links read from the root, so one still climbing after
    // normalization names nothing the deck can offer.
    expect(collectTurnFiles([assistant('See [it](../outside.md).')])).toEqual([]);
  });

  it('counts a plain-text answer as a deliverable', () => {
    const files = collectTurnFiles([assistant('Here are [the notes](results/notes.txt).')]);
    expect(files.map((f) => f.path)).toEqual(['results/notes.txt']);
  });
});

/**
 * A file name is whatever the filesystem accepted, and a link destination is a
 * URL, so the two disagree about `#`, `?`, `%` and every byte above ASCII. The
 * deck reads the destination once and the name has to survive it.
 */
describe('collectTurnFiles — names the agent actually writes', () => {
  it('decodes a percent-escaped name and dedupes it against the plain one', () => {
    const files = collectTurnFiles([
      assistant('See [报告](results/%E5%AD%A3%E5%BA%A6%E6%8A%A5%E5%91%8A.md) and [again](results/季度报告.md).'),
    ]);
    expect(files.map((f) => f.path)).toEqual(['results/季度报告.md']);
  });

  it('keeps an escaped `#` or `?` as part of the name', () => {
    // `%23` decodes to `#`, which a second reading as a link syntax cut away:
    // the path became `results/issue`, which carries no extension, so the file
    // read as working material and earned no card at all.
    const files = collectTurnFiles([
      assistant('See [one](results/issue%231.md) and [two](results/a%3Fb.md).'),
    ]);
    expect(files.map((f) => f.path)).toEqual(['results/issue#1.md', 'results/a?b.md']);
  });

  it('carries non-Latin names, spaces and punctuation through to the card', () => {
    const files = collectTurnFiles([
      assistant('[a](<results/日本語 ファイル.pdf>) [b](<results/한국어 보고서.md>) [c](results/Отчёт.md) [d](results/tag[1].md)', {
        w: write(0, 'results/图表 (2026).png'),
        x: write(1, '/home/workspace/results/데이터.csv'),
      }),
    ]);
    expect(files.map((f) => f.path)).toEqual([
      'results/日本語 ファイル.pdf',
      'results/한국어 보고서.md',
      'results/Отчёт.md',
      'results/tag[1].md',
      'results/图表 (2026).png',
      'results/데이터.csv',
    ]);
  });

  it('keeps the location a non-Latin reference pointed at', () => {
    const files = collectTurnFiles([assistant('See [估值](results/季度报告.md#估值假设).')]);
    expect(files).toEqual([{ path: 'results/季度报告.md', workspaceId: undefined, location: { anchor: '估值假设' } }]);
  });
});

/**
 * The deck is a memoized prop on the bubble that ends a turn, so the value's
 * identity is a correctness property of the transcript, not an optimization:
 * a fresh array re-renders every settled bubble on every streamed token.
 */
describe('turnFilesByTurn', () => {
  const turn = (message: Record<string, unknown>, turnIndex: number) => ({ message, turnIndex });

  it('hands back the same array while a turn’s messages are unchanged', () => {
    const settled = assistant('Built [the review](results/review.md).');
    const projected = [turn(settled as Record<string, unknown>, 0)];

    const first = turnFilesByTurn(projected).get(0);
    const second = turnFilesByTurn([turn(settled as Record<string, unknown>, 0)]).get(0);

    expect(first).toEqual([{ path: 'results/review.md', workspaceId: undefined, location: undefined }]);
    expect(second).toBe(first);
  });

  it('rebuilds when the turn gains a message', () => {
    const first = assistant('Working on it.');
    const before = turnFilesByTurn([turn(first as Record<string, unknown>, 0)]).get(0);
    const after = turnFilesByTurn([
      turn(first as Record<string, unknown>, 0),
      turn(assistant('Done, see [the report](results/report.md).') as Record<string, unknown>, 0),
    ]).get(0);

    expect(before).toBeUndefined();
    expect(after?.map((f) => f.path)).toEqual(['results/report.md']);
  });

  it('rebuilds when the workspace folder name arrives', () => {
    const settled = assistant('Done.', { a: write(0, '/home/workspace/alpha/agent.md') });
    const projected = [turn(settled as Record<string, unknown>, 0)];
    expect(turnFilesByTurn(projected).get(0)?.map((f) => f.path)).toEqual(['alpha/agent.md']);
    expect(turnFilesByTurn(projected, own('alpha')).get(0)).toBeUndefined();
  });

  it('rebuilds when a rename moves the folder, and not for a copy of the same names', () => {
    const settled = assistant('Done, see [the report](results/report.md).', {
      a: write(0, '/home/workspace/alpha/agent.md'),
    });
    const projected = [turn(settled as Record<string, unknown>, 0)];
    const paths = (files?: { path: string }[]) => files?.map((f) => f.path);

    const before = turnFilesByTurn(projected, own('alpha', [])).get(0);
    expect(paths(before)).toEqual(['results/report.md']);
    // Renamed to Beta: the old folder is only in the previous names now.
    const renamed = turnFilesByTurn(projected, own('Beta', ['alpha'])).get(0);
    expect(paths(renamed)).toEqual(['results/report.md']);
    // A refetch hands over a new array with the same names; the cards keep their identity.
    expect(turnFilesByTurn(projected, own('Beta', ['alpha'])).get(0)).toBe(renamed);
    // Before the record carries the old folder, its notes file reads as a deliverable.
    expect(paths(turnFilesByTurn(projected, own('Beta')).get(0))).toEqual(['results/report.md', 'alpha/agent.md']);
  });

  it('claims nothing for a turn still streaming', () => {
    const streaming = { ...assistant('Saving [the report](results/report.md)'), isStreaming: true };
    expect(turnFilesByTurn([turn(streaming as Record<string, unknown>, 0)]).size).toBe(0);
  });
});

describe('workspace identity', () => {
  it('keeps one path in two workspaces as two files', () => {
    const files = collectTurnFiles([
      assistant('Compare [ours](__wsref__/ws-1/results/report.md) with [theirs](__wsref__/ws-2/results/report.md).'),
    ]);
    expect(files.map((f) => [f.workspaceId, f.path])).toEqual([
      ['ws-1', 'results/report.md'],
      ['ws-2', 'results/report.md'],
    ]);
  });
});

describe('a bracket-heavy line does not freeze the transcript', () => {
  /**
   * A label that rescans the rest of the line from every unmatched `[` is
   * quadratic, and this runs on the main thread as a turn settles. At 80k
   * brackets it took ~14s, which freezes the tab; bounded it is ~0.35s. The
   * ceiling is deliberately loose, because what this locks is the complexity
   * class, not a stopwatch reading.
   */
  it('stays fast on a prose line full of unmatched brackets', () => {
    const started = performance.now();
    expect(collectTurnFiles([assistant('['.repeat(80_000))])).toEqual([]);
    expect(performance.now() - started).toBeLessThan(3_000);
  });

  it('gives up on a label past the bound rather than scanning the line', () => {
    // The cap is what buys the line above, so the trade it makes is pinned
    // too: 512 is far past any label an agent writes.
    expect(collectTurnFiles([assistant(`[${'x'.repeat(500)}](results/report.md)`)]))
      .toEqual([{ path: 'results/report.md' }]);
    expect(collectTurnFiles([assistant(`[${'x'.repeat(600)}](results/report.md)`)])).toEqual([]);
  });
});

describe('a title is not part of the destination', () => {
  // The secretary now qualifies and preserves titled links, so a relayed
  // `[report](results/report.pdf "Download")` opens from the transcript. The
  // card for it has to follow, or the reply names a deliverable the deck does
  // not offer.
  it.each([
    ['[r](results/report.pdf "Download")'],
    ["[r](results/report.pdf 'Download')"],
    ['[r](results/report.pdf (Download))'],
  ])('collects %s', (text) => {
    expect(collectTurnFiles([assistant(text)])).toEqual([{ path: 'results/report.pdf' }]);
  });

  it('reads a title after an angle-bracketed destination', () => {
    expect(collectTurnFiles([assistant('[d](<results/Q3 deck.pptx> "Deck")')]))
      .toEqual([{ path: 'results/Q3 deck.pptx' }]);
  });

  it('counts a titled image as embedded, so its Write earns no duplicate card', () => {
    const titled = assistant('![chart](charts/c.png "Chart")', { a: write(0, 'charts/c.png') });
    expect(collectTurnFiles([titled])).toEqual([]);
    // The control that makes the line above mean something: the same Write with
    // nothing embedding the chart does earn a card.
    expect(collectTurnFiles([assistant('Done.', { a: write(0, 'charts/c.png') })]))
      .toEqual([{ path: 'charts/c.png' }]);
  });

  it('leaves an unterminated title alone, the way the secretary does', () => {
    expect(collectTurnFiles([assistant('[r](results/report.pdf "unterminated)')])).toEqual([]);
  });

  it('cards the report behind a chart that is the link', () => {
    // A clickable chart names two files at once. Reading as far as the image
    // left `](work/report.md)` with no `[` in front of it, so the document the
    // click opens was the one thing the deck never heard about.
    const linked = assistant('[![chart](charts/c.png)](work/report.md)', { a: write(0, 'charts/c.png') });
    expect(collectTurnFiles([linked])).toEqual([{ path: 'work/report.md' }]);
  });

  it('keeps a chart that is the link embedded, label or no label', () => {
    // The chart is on screen either way, so its Write still earns no card,
    // which is the half of the reading that already worked.
    const labelled = assistant('[![chart](charts/c.png) open it](work/report.md)', { a: write(0, 'charts/c.png') });
    expect(collectTurnFiles([labelled])).toEqual([{ path: 'work/report.md' }]);
  });
});
