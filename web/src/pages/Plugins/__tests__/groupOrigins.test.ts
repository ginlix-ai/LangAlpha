import { describe, it, expect } from 'vitest';
import {
  matchesFilter,
  pluginSourceOrigin,
  UPLOADED_ORIGIN,
} from '../utils/groupOrigins';

describe('pluginSourceOrigin', () => {
  it('groups zip installs under the uploaded origin', () => {
    expect(pluginSourceOrigin({ source_type: 'zip', source_ref: null })).toBe(UPLOADED_ORIGIN);
    expect(pluginSourceOrigin({ source_type: 'zip', source_ref: 'pkg.zip' })).toBe(UPLOADED_ORIGIN);
  });

  it('reduces a repo URL to host/owner/repo', () => {
    expect(
      pluginSourceOrigin({ source_type: 'git', source_ref: 'https://github.com/cursor/plugins' }),
    ).toBe('github.com/cursor/plugins');
  });

  it('drops the deep-link parts: tree paths, .git, and #subdir fragments', () => {
    expect(
      pluginSourceOrigin({
        source_type: 'git',
        source_ref: 'https://github.com/acme/widgets/tree/v1.0.0/plugins/widget',
      }),
    ).toBe('github.com/acme/widgets');
    expect(
      pluginSourceOrigin({
        source_type: 'git',
        source_ref: 'https://github.com/acme/widgets.git',
      }),
    ).toBe('github.com/acme/widgets');
    expect(
      pluginSourceOrigin({
        source_type: 'git',
        source_ref: 'https://github.com/cursor/plugins#subdir=cli-for-agent',
      }),
    ).toBe('github.com/cursor/plugins');
  });

  it('falls back to the hostname for direct archive URLs', () => {
    expect(
      pluginSourceOrigin({
        source_type: 'git',
        source_ref: 'https://example.com/dist/pkg.tar.gz',
      }),
    ).toBe('example.com/dist/pkg.tar.gz');
  });

  it('returns the raw ref when it is not a URL at all', () => {
    expect(pluginSourceOrigin({ source_type: 'git', source_ref: 'not a url' })).toBe('not a url');
  });
});

describe('matchesFilter', () => {
  it('matches case-insensitively across any provided field', () => {
    expect(matchesFilter('CTX', 'other', null, 'my-ctx-server')).toBe(true);
    expect(matchesFilter('missing', 'alpha', 'beta')).toBe(false);
  });

  it('treats an empty or whitespace filter as match-all', () => {
    expect(matchesFilter('', 'anything')).toBe(true);
    expect(matchesFilter('   ', 'anything')).toBe(true);
    expect(matchesFilter('  x ', 'x')).toBe(true);
  });

  it('never matches on null/undefined fields', () => {
    expect(matchesFilter('x', null, undefined)).toBe(false);
  });
});
