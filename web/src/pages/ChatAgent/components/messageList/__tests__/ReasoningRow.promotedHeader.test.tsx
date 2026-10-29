/**
 * A header promoted to the row's label must not also open the body.
 *
 * `extractLeadingBoldHeader` promotes a leading bold line whether or not a
 * blank line follows it, but `extractReasoningHeaders` counts *phases*, which
 * need that blank line to separate them. A thought written `**Header**\nBody`
 * therefore promotes its header and counts zero phases, and reading that zero
 * as "no header here" put the promoted line back at the top of the body.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render } from '@testing-library/react';
import { ReasoningRow } from '../ReasoningRow';
import type { ReasoningActivityItem } from '../activityTypes';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('../../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="body">{content}</div>,
}));

const HEADER = 'Checking the filings';
const BODY = 'I need to pull the 10-K first.';

const row = (content: string) =>
  render(
    <ReasoningRow
      item={{ content, reasoningElapsedMs: 4200 } as ReasoningActivityItem}
      defaultExpanded
    />,
  );

const bodyText = (c: HTMLElement) => c.querySelector('[data-testid="body"]')?.textContent ?? '';

describe('ReasoningRow, a header promoted to the label', () => {
  it('leaves the body with the header removed when one newline separates them', () => {
    const { container } = row(`**${HEADER}**\n${BODY}`);
    expect(container.textContent).toContain(HEADER);
    expect(bodyText(container)).toBe(BODY);
  });

  it('keeps doing so when a blank line separates them', () => {
    const { container } = row(`**${HEADER}**\n\n${BODY}`);
    expect(bodyText(container)).toBe(BODY);
  });

  it('keeps every phase in the body when the thought has a run of them', () => {
    // The label only ever shows one phase, so the open row is where the reader
    // sees the sequence: the headers stay in the body on purpose.
    const content = `**${HEADER}**\n\nfirst\n\n**Reading them**\n\nsecond`;
    const { container } = row(content);
    expect(bodyText(container)).toBe(content);
  });
});
