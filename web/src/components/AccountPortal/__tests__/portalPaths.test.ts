import { describe, expect, it, vi } from 'vitest';

vi.mock('@/config/supportLinks', () => ({ PLATFORM_URL: '/account' }));

const { portalPathOfHref } = await import('../portalPaths');

const PAGE = 'https://app.example.com/settings';

describe('portalPathOfHref', () => {
  it('reads a portal link as its portal path, query kept', () => {
    expect(portalPathOfHref('/account/billing', PAGE)).toBe('/billing');
    expect(portalPathOfHref('/account/redeem?promo=AB12', PAGE)).toBe('/redeem?promo=AB12');
  });

  it('leaves links outside the portal alone', () => {
    expect(portalPathOfHref('/account-x/billing', PAGE)).toBeNull();
    expect(portalPathOfHref('https://other.example.com/account/billing', PAGE)).toBeNull();
  });

  it('leaves a link with a fragment to open at top level, so an invitation keeps its token', () => {
    expect(portalPathOfHref('/account/join#invitation=qaInvite_0123456789abcdef', PAGE)).toBeNull();
  });
});
