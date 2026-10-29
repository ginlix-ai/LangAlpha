// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { awayZoneName } from '../schedule';

describe('awayZoneName', () => {
  it('leaves the reader\'s own zone unnamed', () => {
    expect(awayZoneName('America/New_York', 'America/New_York', 'en-US')).toBeNull();
  });

  it('treats a retired name as the zone it became', () => {
    expect(awayZoneName('Asia/Calcutta', 'Asia/Kolkata', 'en-US')).toBeNull();
  });

  it('names a zone that is not the reader\'s', () => {
    expect(awayZoneName('Asia/Tokyo', 'America/New_York', 'en-US')).toBe('Japan Standard Time');
  });

  it('names nothing when no zone is set', () => {
    expect(awayZoneName(null, 'America/New_York', 'en-US')).toBeNull();
  });
});
