/**
 * The desktop notification for a vendor's terms waiting on the user: posted
 * once per request, taken down once it is answered or the page goes.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { render } from '@testing-library/react';
import type { PendingVendorTerms } from '@/api/vendorTerms';

const desktop = vi.hoisted(() => ({
  desktopNotify: vi.fn(async () => ({ shown: true })),
  clearDesktopNotifications: vi.fn(),
}));
vi.mock('@/lib/desktop', () => desktop);

const pending = vi.hoisted(() => ({ list: [] as PendingVendorTerms[] }));
vi.mock('@/hooks/useVendorTerms', () => ({ usePendingVendorTerms: () => pending.list }));

import { VendorTermsNotifier } from '../VendorTermsNotifier';

const terms = (vendor: string, name: string): PendingVendorTerms => ({
  vendor,
  name,
  terms_url: `https://${vendor}.example.com/tos`,
  requested_at: '2026-11-15T00:00:00Z',
});

beforeEach(() => {
  desktop.desktopNotify.mockClear();
  desktop.clearDesktopNotifications.mockClear();
  pending.list = [];
});

describe('VendorTermsNotifier', () => {
  it('posts each request once, as a question, and takes it down when answered', () => {
    pending.list = [terms('firecrawl', 'Firecrawl')];
    const view = render(<VendorTermsNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledTimes(1);
    expect(desktop.desktopNotify).toHaveBeenCalledWith({
      id: 'vendor-terms:firecrawl:2026-11-15T00:00:00Z',
      kind: 'needs_input',
      title: "Accept Firecrawl's terms",
      body: "Your agent needs you to accept Firecrawl's terms",
      path: '/',
    });

    pending.list = [terms('firecrawl', 'Firecrawl'), terms('acme', 'Acme')];
    view.rerender(<VendorTermsNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledTimes(2);

    pending.list = [terms('acme', 'Acme')];
    view.rerender(<VendorTermsNotifier />);
    expect(desktop.desktopNotify).toHaveBeenCalledTimes(2);
    expect(desktop.clearDesktopNotifications).toHaveBeenLastCalledWith(['vendor-terms:firecrawl:2026-11-15T00:00:00Z']);

    view.unmount();
    expect(desktop.clearDesktopNotifications).toHaveBeenLastCalledWith(['vendor-terms:acme:2026-11-15T00:00:00Z']);
  });
});
