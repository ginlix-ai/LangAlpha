// @vitest-environment node
import { describe, it, expect } from 'vitest';
import type { ReadyAttachment } from '@/components/ui/chat-input.types';
import { BRIEF_PREAMBLE, kickoffSend, type OnboardingKickoff } from '../welcome/kickoff';
import { screenshotAttachment } from '../welcome/screenshots';

const TITLE = 'Getting started';

/** A kickoff with nothing but what a test sets. */
const kickoff = (over: Partial<OnboardingKickoff> = {}): OnboardingKickoff => ({
  task: '',
  brokerages: [],
  brokeragesUsed: [],
  attachments: [],
  screenshots: 0,
  brief: null,
  ...over,
});

const instruction = (context: Record<string, unknown>[]) => context[0].instruction as string;

describe('kickoffSend', () => {
  it("a proposal opens hidden under its own title, with no words of the user's, and names the action, language and brokerage", () => {
    const send = kickoffSend(kickoff({ brokerages: ['Robinhood', 'moomoo'] }), 'en-US', TITLE);
    expect(send.initialMessage).toBe('');
    expect(send.hiddenOpener).toBe(true);
    expect(send.threadTitle).toBe(TITLE);
    // The onboarding skill alone: the diagnosis is the agent's to read, once
    // the user picks it.
    expect(send.additionalContext).toEqual([
      {
        type: 'skills',
        name: 'onboarding',
        instruction: expect.stringContaining('Action: propose. Language: en-US. Brokerage: Robinhood and moomoo.'),
      },
    ]);
    expect(instruction(send.additionalContext)).toContain(
      "This is the user's first conversation, right after onboarding.",
    );
    expect(instruction(send.additionalContext)).toContain('do not ask for it again');
  });

  it("names the app's language as it is, for the agent to write in", () => {
    const { additionalContext } = kickoffSend(kickoff(), 'zh-CN', TITLE);
    expect(instruction(additionalContext)).toContain('Language: zh-CN. Brokerage: none.');
  });

  it('a first task is sent as the message, shown and titled from it, with no brokerage named as none', () => {
    const send = kickoffSend(kickoff({ task: 'Summarize what moved the US market this week' }), 'en-US', TITLE);
    expect(send.initialMessage).toBe('Summarize what moved the US market this week');
    expect(send).not.toHaveProperty('hiddenOpener');
    expect(send).not.toHaveProperty('threadTitle');
    expect(send.additionalContext).toHaveLength(1);
    expect(send.additionalContext[0]).toMatchObject({ name: 'onboarding' });
    expect(instruction(send.additionalContext)).toContain('Action: first_task. Language: en-US. Brokerage: none.');
  });
});

describe('kickoffSend with what the user said instead of connecting', () => {
  const shots = [
    screenshotAttachment({ id: 's1', name: 'positions.png', dataUrl: 'data:image/png;base64,AAAAAAAA', bytes: 30 }),
    screenshotAttachment({ id: 's2', name: 'watchlist.jpg', dataUrl: 'data:image/jpeg;base64,BBBB', bytes: 27 }),
  ];

  it('names the brokerages used and the screenshot count in the instruction, only when there are any', () => {
    const bare = kickoffSend(kickoff(), 'en-US', TITLE);
    expect(instruction(bare.additionalContext)).not.toContain('Brokerages used');
    expect(instruction(bare.additionalContext)).not.toContain('Screenshots');

    const { additionalContext } = kickoffSend(
      kickoff({ brokeragesUsed: ['Fidelity', 'Acme Brokers'], attachments: shots, screenshots: 2 }),
      'en-US',
      TITLE,
    );
    expect(instruction(additionalContext)).toContain(
      "Brokerage: none. Brokerages used: Fidelity and Acme Brokers. Screenshots: 2. This is the user's first conversation",
    );
  });

  it('attaches each screenshot as an image context after the skill', () => {
    const { additionalContext } = kickoffSend(kickoff({ attachments: shots, screenshots: 2 }), 'en-US', TITLE);
    expect(additionalContext.slice(1)).toEqual([
      { type: 'image', data: 'data:image/png;base64,AAAAAAAA', description: 'positions.png' },
      { type: 'image', data: 'data:image/jpeg;base64,BBBB', description: 'watchlist.jpg' },
    ]);
    const task = kickoffSend(kickoff({ task: 'Go', attachments: shots, screenshots: 2 }), 'en-US', TITLE);
    expect(task.additionalContext).toHaveLength(3);
    expect(task.additionalContext[2]).toMatchObject({ type: 'image', description: 'watchlist.jpg' });
  });

  it('describes the screenshots for the bubble under a task the user wrote, and has no bubble to describe them for otherwise', () => {
    // A proposal draws no bubble, so the agent's read of the screens is
    // where the user sees them.
    expect(kickoffSend(kickoff({ attachments: shots, screenshots: 2 }), 'en-US', TITLE).attachmentMeta).toBeNull();
    expect(kickoffSend(kickoff({ task: 'Go' }), 'en-US', TITLE).attachmentMeta).toBeNull();
    const { attachmentMeta } = kickoffSend(kickoff({ task: 'Go', attachments: shots, screenshots: 2 }), 'en-US', TITLE);
    // The type the data URL names, and the size of the bytes it holds.
    expect(attachmentMeta).toEqual([
      { name: 'positions.png', type: 'image/png', size: 6, preview: null, dataUrl: 'data:image/png;base64,AAAAAAAA' },
      { name: 'watchlist.jpg', type: 'image/jpeg', size: 3, preview: null, dataUrl: 'data:image/jpeg;base64,BBBB' },
    ]);
  });
});

describe("kickoffSend with today's market brief", () => {
  const brief = '## Stocks slip as yields climb\n_type: market_update_';

  it('rides as a reminder the user is not shown, named in the instruction', () => {
    const { additionalContext } = kickoffSend(kickoff({ brief }), 'en-US', TITLE);
    expect(instruction(additionalContext)).toContain(
      "Brokerage: none. Market brief: attached. This is the user's first conversation",
    );
    expect(additionalContext[1]).toEqual({
      type: 'directive',
      content: `${BRIEF_PREAMBLE}\n<market-brief>\n${brief}\n</market-brief>`,
    });
  });

  it('comes before the screenshots, and rides a first task too', () => {
    const shot = screenshotAttachment({
      id: 's1',
      name: 'positions.png',
      dataUrl: 'data:image/png;base64,AAAA',
      bytes: 26,
    });
    const { additionalContext } = kickoffSend(
      kickoff({ task: 'Go', brief, attachments: [shot], screenshots: 1 }),
      'en-US',
      TITLE,
    );
    expect(additionalContext.map((c) => c.type)).toEqual(['skills', 'directive', 'image']);
  });

  it('is trimmed, and neither named nor attached without one or with a blank one', () => {
    expect(kickoffSend(kickoff({ brief: '  ## Headline \n' }), 'en-US', TITLE).additionalContext[1]).toEqual({
      type: 'directive',
      content: `${BRIEF_PREAMBLE}\n<market-brief>\n## Headline\n</market-brief>`,
    });
    for (const none of [null, '   ']) {
      const { additionalContext } = kickoffSend(kickoff({ brief: none }), 'en-US', TITLE);
      expect(instruction(additionalContext)).not.toContain('Market brief');
      expect(additionalContext.some((c) => c.type === 'directive')).toBe(false);
    }
  });
});

describe("kickoffSend with the composer's files", () => {
  const attachment = (
    name: string,
    type: string,
    body: string,
    dataUrl: string | null,
    preview: string | null = null,
  ) => ({ file: new File([body], name, { type }), type, dataUrl, preview }) satisfies ReadyAttachment;

  it('carries the files that finished reading, as context for the agent and as the bubble shows them', () => {
    const send = kickoffSend(
      kickoff({
        task: 'Read these',
        attachments: [
          attachment('chart.png', 'image/png', 'abcd', 'data:image/png;base64,AAAA', 'blob:chart'),
          // Still reading: nothing to send yet.
          attachment('draft.pdf', 'application/pdf', 'ab', null),
          // No thumbnail is sent as none, not as an empty one.
          attachment('report.pdf', 'application/pdf', 'abc', 'data:application/pdf;base64,BBBB', ''),
        ],
      }),
      'en-US',
      TITLE,
    );
    expect(send.additionalContext.slice(1)).toEqual([
      { type: 'image', data: 'data:image/png;base64,AAAA', description: 'chart.png' },
      { type: 'pdf', data: 'data:application/pdf;base64,BBBB', description: 'report.pdf' },
    ]);
    expect(send.attachmentMeta).toEqual([
      { name: 'chart.png', type: 'image/png', size: 4, preview: 'blob:chart', dataUrl: 'data:image/png;base64,AAAA' },
      {
        name: 'report.pdf',
        type: 'application/pdf',
        size: 3,
        preview: null,
        dataUrl: 'data:application/pdf;base64,BBBB',
      },
    ]);
  });

  it('has no files to send when none has finished reading', () => {
    const send = kickoffSend(
      kickoff({ task: 'Read this', attachments: [attachment('draft.pdf', 'application/pdf', 'ab', null)] }),
      'en-US',
      TITLE,
    );
    expect(send.additionalContext).toHaveLength(1);
    expect(send.attachmentMeta).toBeNull();
  });
});

describe('screenshotAttachment', () => {
  it('reads a screenshot back into the file the composer would hold, its type the one its data URL names', async () => {
    const dataUrl = 'data:image/png;base64,iVBORw0KGgo=';
    const attached = screenshotAttachment({ id: 's1', name: 'positions.png', dataUrl, bytes: dataUrl.length });
    expect(attached).toMatchObject({ type: 'image/png', dataUrl, preview: null });
    expect(attached.file.name).toBe('positions.png');
    expect(attached.file.type).toBe('image/png');
    // The PNG signature, byte for byte.
    expect([...new Uint8Array(await attached.file.arrayBuffer())]).toEqual([137, 80, 78, 71, 13, 10, 26, 10]);
  });
});
