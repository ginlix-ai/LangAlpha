/**
 * A group of rows with nothing to say when it has none renders no empty
 * line in their place, and a row always has a mark to show.
 */
import { describe, expect, it } from 'vitest';
import { render } from '@testing-library/react';
import { Group } from '../parts';
import { AccessRow } from '../AccessRow';

describe('Group', () => {
  it('renders nothing in place of no rows where it has no empty text', () => {
    const { container } = render(<Group title="Your computers">{[]}</Group>);
    expect(container.querySelector('p')).toBeNull();
    expect(container.querySelector('ul')).toBeNull();
  });

  it('says its empty text in place of no rows', () => {
    const { getByText } = render(<Group title="Keys" empty="No keys yet">{[]}</Group>);
    expect(getByText('No keys yet')).toBeInTheDocument();
  });
});

describe('AccessRow', () => {
  it('takes a provider or an icon for its mark, never neither', () => {
    // @ts-expect-error a row with no mark would draw an empty provider's art
    const noMark = <AccessRow name="Lab server" />;
    expect(noMark).toBeTruthy();
  });
});
