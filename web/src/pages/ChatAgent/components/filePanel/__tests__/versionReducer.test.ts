import { describe, expect, it } from 'vitest';
import { LIVE_VIEW, versionReducer, type VersionState } from '../useArtifactVersionView';

const on = (version: number | null, updatedTo: number | null = null): VersionState => ({ version, updatedTo });

/** Where an owner's view of an HTML file sits among its versions, in the panel and on `/a/`. */
describe('versionReducer', () => {
  it('an Update moves a view on the latest to the new version and says so', () => {
    expect(versionReducer(on(3), { type: 'follow', from: 3, to: 4 })).toEqual(on(4, 4));
  });

  it('a view on an older version, or on the live file, stays where it is', () => {
    const older = on(2);
    expect(versionReducer(older, { type: 'follow', from: 3, to: 4 })).toBe(older);
    expect(versionReducer(LIVE_VIEW, { type: 'follow', from: 3, to: 4 })).toBe(LIVE_VIEW);
  });

  it('the notice settles once said, but not for an Update that came after it', () => {
    expect(versionReducer(on(4, 4), { type: 'settle', seq: 4 })).toEqual(on(4));
    const newer = on(5, 5);
    expect(versionReducer(newer, { type: 'settle', seq: 4 })).toBe(newer);
  });

  it("the reader's pick overtakes the notice", () => {
    expect(versionReducer(on(4, 4), { type: 'pick', version: 2 })).toEqual(on(2));
    expect(versionReducer(on(4, 4), { type: 'pick', version: null })).toEqual(LIVE_VIEW);
  });

  it('a pick of the version already shown changes nothing', () => {
    const shown = on(2);
    expect(versionReducer(shown, { type: 'pick', version: 2 })).toBe(shown);
  });
});
