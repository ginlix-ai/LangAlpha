// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { runsInHome } from '../place';

describe('runsInHome', () => {
  it("is true only for a row naming Home's id", () => {
    expect(runsInHome({ workspace_id: 'home-1' }, 'home-1')).toBe(true);
    expect(runsInHome({ workspace_id: 'ws-1' }, 'home-1')).toBe(false);
    expect(runsInHome({ workspace_id: null }, 'home-1')).toBe(false);
  });

  it("reads no row as Home before Home's id is known", () => {
    expect(runsInHome({ workspace_id: 'home-1' }, undefined)).toBe(false);
    expect(runsInHome({ workspace_id: null }, undefined)).toBe(false);
  });
});
