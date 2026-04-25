/**
 * E2E tests for the Dashboard page.
 * Covers market indices, news feed, watchlist/portfolio CRUD, and onboarding.
 */
import {
  test,
  expect,
  mockAPI,
  resetMockServer,
  configureSSE,
} from './fixtures.js';
import {
  sampleIndexSnapshot,
  sampleNewsArticle,
  sampleWatchlistItem,
  samplePortfolioHolding,
} from './helpers/mockResponses.js';

// Shared index snapshot data used across multiple tests
const allIndexSnapshots = [
  sampleIndexSnapshot('GSPC', 5500.25, 25.5),
  sampleIndexSnapshot('IXIC', 17200.5, -50.0),
  sampleIndexSnapshot('DJI', 39800.0, 120.0),
  sampleIndexSnapshot('RUT', 2050.75, 10.25),
  sampleIndexSnapshot('VIX', 15.3, -0.5),
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Suppress the onboarding dialog via localStorage before page load. */
async function suppressOnboarding(page) {
  await page.addInitScript(() => {
    localStorage.setItem(
      'langalpha-onboarding-ignored-at',
      String(Date.now()),
    );
  });
}

/** Additional routes beyond defaultResponses that the dashboard needs. */
function dashboardOverrides(extra = {}) {
  return {
    // market-status is fetched by the dashboard (via marketUtils.fetchMarketStatus)
    'GET /market-data/market-status': {
      market: 'open',
      afterHours: false,
      earlyHours: false,
      serverTime: new Date().toISOString(),
    },
    // stock search is used by AddWatchlistItemDialog / AddPortfolioHoldingDialog
    'GET /market-data/search/stocks': (route) => {
      const url = new URL(route.request().url());
      const q = (url.searchParams.get('query') || '').toUpperCase();
      const results =
        q.length > 0
          ? [
              {
                symbol: 'AAPL',
                name: 'Apple Inc.',
                exchangeShortName: 'NASDAQ',
                currency: 'USD',
              },
            ]
          : [];
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ query: q, results, count: results.length }),
      });
    },
    // stock names endpoint (POST)
    'POST /market-data/stocks/names': { names: {} },
    ...extra,
  };
}

/**
 * Configure the mock SSE server for intraday index endpoints.
 * The app calls GET /api/v1/market-data/intraday/indexes/:symbol for each
 * index. These go to the mock server directly (not intercepted by page.route).
 */
async function configureIntradayDefaults() {
  const symbols = ['GSPC', 'IXIC', 'DJI', 'RUT', 'VIX'];
  for (const sym of symbols) {
    await configureSSE({
      method: 'GET',
      path: `/api/v1/market-data/intraday/indexes/${sym}`,
      json: { data: [] },
    });
  }
}

// ---------------------------------------------------------------------------
// Test setup
// ---------------------------------------------------------------------------

test.beforeEach(async ({ page }) => {
  await resetMockServer();
  await configureIntradayDefaults();
  await suppressOnboarding(page);
});

// ---------------------------------------------------------------------------
// Market Indices & News
// ---------------------------------------------------------------------------

test.describe('Market Indices & News', () => {
  test('index cards show prices', async ({ page }) => {
    await mockAPI(page, dashboardOverrides({
      'GET /market-data/snapshots/indexes': { snapshots: allIndexSnapshots },
    }));

    await page.goto('/dashboard');

    // Verify S&P 500 card renders with name, symbol, and price
    await expect(page.locator('h3', { hasText: 'S&P 500' })).toBeVisible();
    await expect(page.getByText('^GSPC')).toBeVisible();
    await expect(page.getByText('5,500.25')).toBeVisible();

    // Verify NASDAQ card
    await expect(page.locator('h3', { hasText: 'NASDAQ' })).toBeVisible();
    await expect(page.getByText('17,200.50')).toBeVisible();

    // Verify all 5 index cards are present
    await expect(page.locator('h3', { hasText: 'Dow Jones' })).toBeVisible();
    await expect(page.locator('h3', { hasText: 'Russell 2000' })).toBeVisible();
    await expect(page.locator('h3', { hasText: 'VIX' })).toBeVisible();
  });

  test('news headlines render with tabs', async ({ page }) => {
    const articles = [
      sampleNewsArticle({ id: 'n1', title: 'Tech Stocks Surge' }),
      sampleNewsArticle({ id: 'n2', title: 'Fed Holds Rates Steady' }),
    ];

    await mockAPI(page, dashboardOverrides({
      'GET /news': {
        results: articles,
        count: articles.length,
        next_cursor: null,
      },
    }));

    await page.goto('/dashboard');

    // Headlines render
    await expect(
      page.locator('h3[title="Tech Stocks Surge"]'),
    ).toBeVisible();
    await expect(
      page.locator('h3[title="Fed Holds Rates Steady"]'),
    ).toBeVisible();

    // Tab buttons are present
    await expect(
      page.getByRole('button', { name: 'Market Pulse' }),
    ).toBeVisible();
    await expect(
      page.getByRole('button', { name: 'Your Portfolio' }),
    ).toBeVisible();
    await expect(
      page.getByRole('button', { name: 'Your Watchlist' }),
    ).toBeVisible();

    // Clicking "Your Portfolio" tab shows the empty state message
    await page.getByRole('button', { name: 'Your Portfolio' }).click();
    await expect(
      page.locator('text=Add stocks to your portfolio'),
    ).toBeVisible();
  });

  test('clicking news opens detail modal', async ({ page }) => {
    const article = sampleNewsArticle({
      id: 'n-detail',
      title: 'Earnings Beat Expectations',
    });

    await mockAPI(page, dashboardOverrides({
      'GET /news': {
        results: [article],
        count: 1,
        next_cursor: null,
      },
      'GET /news/n-detail': {
        id: 'n-detail',
        title: 'Earnings Beat Expectations',
        description: 'Major companies reported strong quarterly results.',
        published_at: '2025-01-15T10:00:00Z',
        source: { name: 'Bloomberg', favicon_url: '' },
        image_url: '',
        tickers: ['AAPL'],
        keywords: ['earnings', 'stocks'],
      },
    }));

    await page.goto('/dashboard');

    // Click the headline
    await page.locator('h3[title="Earnings Beat Expectations"]').click();

    // Modal opens with article content
    await expect(
      page.locator('text=Major companies reported strong quarterly results.'),
    ).toBeVisible();

    // Close via Escape
    await page.keyboard.press('Escape');
    await expect(
      page.locator('text=Major companies reported strong quarterly results.'),
    ).not.toBeVisible();
  });

  test('index card navigates to market view', async ({ page }) => {
    await mockAPI(page, dashboardOverrides({
      'GET /market-data/snapshots/indexes': { snapshots: allIndexSnapshots },
    }));

    await page.goto('/dashboard');

    // Wait for the S&P 500 card to appear
    await expect(page.locator('h3', { hasText: 'S&P 500' })).toBeVisible();

    // Click the S&P 500 card (the motion.div wrapping the card)
    await page.locator('h3', { hasText: 'S&P 500' }).click();

    // Should navigate to /market?symbol=^GSPC
    await page.waitForURL('**/market?symbol=*GSPC*');
    expect(page.url()).toContain('/market');
    expect(page.url()).toContain('GSPC');
  });
});

// ---------------------------------------------------------------------------
// Watchlist CRUD
// ---------------------------------------------------------------------------

test.describe('Watchlist CRUD', () => {
  test('watchlist items render', async ({ page }) => {
    const item = sampleWatchlistItem();

    await mockAPI(page, dashboardOverrides({
      'GET /users/me/watchlists/*/items': {
        items: [item],
        total: 1,
      },
      'GET /market-data/snapshots/stocks': {
        snapshots: [
          {
            symbol: 'TSLA',
            price: 250.0,
            change: 5.0,
            change_percent: 2.04,
            previous_close: 245.0,
          },
        ],
      },
    }));

    await page.goto('/dashboard');

    // Ensure we are on the Watch tab (default)
    await expect(page.locator('h2', { hasText: 'Watchlist' })).toBeVisible();

    // Symbol renders
    await expect(page.getByTestId('watchlist-row-TSLA')).toBeVisible();
  });

  test('add watchlist item', async ({ page }) => {
    let addCaptured = null;

    await mockAPI(page, dashboardOverrides({
      'POST /users/me/watchlists/*/items': (route) => {
        const req = route.request();
        addCaptured = req.postDataJSON();
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            watchlist_item_id: 'wi-new',
            symbol: addCaptured?.symbol || 'AAPL',
            name: 'Apple Inc.',
          }),
        });
      },
      // After add, the refetch of watchlist items returns the new item
      'GET /users/me/watchlists/*/items': {
        items: [],
        total: 0,
      },
      'GET /market-data/snapshots/stocks': {
        snapshots: [
          {
            symbol: 'AAPL',
            price: 180.0,
            change: 2.0,
            change_percent: 1.12,
            previous_close: 178.0,
          },
        ],
      },
    }));

    await page.goto('/dashboard');

    // Click "Add Symbol" button
    await page.getByRole('button', { name: 'Add Symbol' }).click();

    // Dialog opens with title
    const dialog = page.locator('[role="dialog"]');
    await expect(
      dialog.locator('text=Add Watchlist Item').first(),
    ).toBeVisible();

    // Type search query (scoped to dialog to avoid matching the dashboard search)
    await dialog
      .getByPlaceholder('Search by symbol or company name...')
      .fill('AAPL');

    // Wait for search results
    await expect(page.locator('text=Apple Inc.').first()).toBeVisible();

    // Select the stock
    await page
      .locator('button', { hasText: 'Apple Inc.' })
      .first()
      .click();

    // On page 2 now: click "Add to Watchlist" and wait for the POST to complete
    await Promise.all([
      page.waitForResponse((r) => r.url().includes('/watchlists/') && r.request().method() === 'POST'),
      page.getByRole('button', { name: 'Add to Watchlist' }).click(),
    ]);

    // Verify the POST was made with the correct symbol
    expect(addCaptured).toBeTruthy();
    expect(addCaptured.symbol).toBe('AAPL');
  });

  test('delete watchlist item', async ({ page }) => {
    const item = sampleWatchlistItem({ watchlist_item_id: 'wi-del' });
    let deleteCalled = false;

    await mockAPI(page, dashboardOverrides({
      'GET /users/me/watchlists/*/items': {
        items: [item],
        total: 1,
      },
      'GET /market-data/snapshots/stocks': {
        snapshots: [
          {
            symbol: 'TSLA',
            price: 250.0,
            change: 5.0,
            change_percent: 2.04,
            previous_close: 245.0,
          },
        ],
      },
      'DELETE /users/me/watchlists/*/items/*': (route) => {
        deleteCalled = true;
        return route.fulfill({ status: 200, body: '{}' });
      },
    }));

    await page.goto('/dashboard');

    // Wait for the item to render
    await expect(page.getByTestId('watchlist-row-TSLA')).toBeVisible();

    // Right-click to open context menu (Radix ContextMenu)
    await page.getByTestId('watchlist-row-TSLA').click({ button: 'right' });

    // Click "Delete" in context menu (Radix renders items as role="menuitem")
    await page.getByRole('menuitem', { name: 'Delete' }).click();

    // Verify the DELETE request was made
    expect(deleteCalled).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Suggestion chips (regression coverage for the dashboard chat input)
// ---------------------------------------------------------------------------

test.describe('Dashboard chat suggestion chips', () => {
  test('chips: a11y-gated by focus, click fills input, blur unmounts', async ({ page }) => {
    await mockAPI(page, dashboardOverrides({}));
    await page.goto('/dashboard');

    const bubbles = page.getByTestId('dashboard-suggestion-bubble');
    const textarea = page.getByTestId('dashboard-chat-input').locator('textarea');
    // Role-based check pins the a11y invariant (chips not in tab order / a11y tree
    // when unfocused), so a future inert/aria-hidden refactor would still satisfy it
    // even if DOM mounting changed.
    const tslaChip = page.getByRole('button', { name: 'Compare TSLA vs BYD' });

    // Unfocused: chips absent from DOM AND a11y tree.
    await expect(bubbles).toHaveCount(0);
    await expect(tslaChip).toHaveCount(0);

    // Focus mounts chips in DOM and a11y tree.
    await textarea.focus();
    await expect(bubbles).toHaveCount(4);
    await expect(tslaChip).toHaveCount(1);

    // Click chip -> textarea value updates. Chip's onMouseDown preventDefault
    // keeps focus on the textarea, so chips remain mounted.
    await tslaChip.click();
    await expect(textarea).toHaveValue('Compare TSLA vs BYD');

    // setValue queues setTimeout(focus, 0) to refocus the textarea. Drain the
    // macrotask queue before blurring, otherwise the queued refocus fires AFTER
    // our blur and remounts the chips.
    await page.evaluate(() => new Promise((r) => setTimeout(r, 0)));

    // Blur -> chips unmount from both DOM and a11y tree.
    await textarea.blur();
    await expect(bubbles).toHaveCount(0);
    await expect(tslaChip).toHaveCount(0);
  });
});

// ---------------------------------------------------------------------------
// Portfolio CRUD
// ---------------------------------------------------------------------------

test.describe('Portfolio CRUD', () => {
  /** Switch to the Holdings tab and set up portfolio with a sample holding. */
  function portfolioOverrides(holdingOverrides = {}, extraOverrides = {}) {
    const holding = samplePortfolioHolding(holdingOverrides);
    return dashboardOverrides({
      'GET /users/me/portfolio': { holdings: [holding] },
      'GET /market-data/snapshots/stocks': {
        snapshots: [
          {
            symbol: holding.symbol,
            price: 180.0,
            change: 2.0,
            change_percent: 1.12,
            previous_close: 178.0,
          },
        ],
      },
      ...extraOverrides,
    });
  }

  test('portfolio holdings render', async ({ page }) => {
    await mockAPI(page, portfolioOverrides());

    // Clear the tab selection so it defaults to watchlist, then switch
    await page.addInitScript(() => {
      localStorage.removeItem('portfolio_active_tab');
    });

    await page.goto('/dashboard');

    // Switch to Holdings tab
    await page.getByRole('button', { name: 'Holdings' }).click();

    // Heading changes
    await expect(page.locator('h2', { hasText: 'Portfolio' })).toBeVisible();

    // Symbol and share count visible
    await expect(page.getByTestId('portfolio-row-AAPL')).toBeVisible();
    await expect(page.locator('text=10 shares')).toBeVisible();

    // Market value ($1,800.00 = 10 shares * $180.00)
    await expect(page.getByText('$1,800.00').first()).toBeVisible();
  });

  test('add portfolio holding', async ({ page }) => {
    let addCaptured = null;

    await mockAPI(page, dashboardOverrides({
      'GET /users/me/portfolio': { holdings: [] },
      'POST /users/me/portfolio': (route) => {
        addCaptured = route.request().postDataJSON();
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            user_portfolio_id: 'ph-new',
            symbol: addCaptured?.symbol || 'AAPL',
            quantity: addCaptured?.quantity || '10',
            average_cost: addCaptured?.average_cost || '150.00',
          }),
        });
      },
    }));

    await page.goto('/dashboard');

    // Switch to Holdings tab
    await page.getByRole('button', { name: 'Holdings' }).click();

    // Click "Add Transaction"
    await page.getByRole('button', { name: 'Add Transaction' }).click();

    // Dialog opens
    const dialog = page.locator('[role="dialog"]');
    await expect(
      dialog.locator('text=Add Portfolio Holding').first(),
    ).toBeVisible();

    // Search for stock (scoped to dialog to avoid matching the dashboard search)
    await dialog
      .getByPlaceholder('Search by symbol or company name...')
      .fill('AAPL');
    await expect(dialog.locator('text=Apple Inc.').first()).toBeVisible();

    // Select stock
    await page
      .locator('button', { hasText: 'Apple Inc.' })
      .first()
      .click();

    // Fill in quantity and average cost
    await page.locator('input[placeholder="e.g. 10.5"]').fill('25');
    await page.locator('input[placeholder="e.g. 175.50"]').fill('150');

    // Click "Add to Portfolio" and wait for the POST to complete
    await Promise.all([
      page.waitForResponse((r) => r.url().includes('/portfolio') && r.request().method() === 'POST'),
      page.getByRole('button', { name: 'Add to Portfolio' }).click(),
    ]);

    // Verify the POST payload
    expect(addCaptured).toBeTruthy();
    expect(addCaptured.symbol).toBe('AAPL');
    expect(addCaptured.quantity).toBe('25');
    expect(addCaptured.average_cost).toBe('150');
  });

  test('edit portfolio holding', async ({ page }) => {
    let updateCaptured = null;

    await mockAPI(page, portfolioOverrides({}, {
      'PUT /users/me/portfolio/*': (route) => {
        updateCaptured = route.request().postDataJSON();
        return route.fulfill({
          status: 200,
          contentType: 'application/json',
          body: JSON.stringify({
            user_portfolio_id: 'ph-1',
            symbol: 'AAPL',
            quantity: updateCaptured?.quantity,
            average_cost: updateCaptured?.average_cost,
          }),
        });
      },
    }));

    await page.goto('/dashboard');

    // Switch to Holdings tab
    await page.getByRole('button', { name: 'Holdings' }).click();
    await expect(page.getByTestId('portfolio-row-AAPL')).toBeVisible();

    // Right-click to open context menu (Radix ContextMenu)
    await page.getByTestId('portfolio-row-AAPL').click({ button: 'right' });

    // Click "Edit" in context menu (Radix renders items as role="menuitem")
    await page.getByRole('menuitem', { name: 'Edit' }).click();

    // Edit dialog opens with pre-filled values
    await expect(
      page.locator('text=Edit holding').first(),
    ).toBeVisible();

    // The edit dialog has two number inputs: Quantity and Average Cost Per Share
    const editDialogInputs = page.locator('[role="dialog"] input[type="number"]');
    await editDialogInputs.nth(0).fill('20');
    await editDialogInputs.nth(1).fill('155');

    // Click Save and wait for the PUT to complete
    await Promise.all([
      page.waitForResponse((r) => r.url().includes('/portfolio/') && r.request().method() === 'PUT'),
      page.getByRole('button', { name: 'Save' }).click(),
    ]);

    // Verify PUT was called with new values
    expect(updateCaptured).toBeTruthy();
    expect(updateCaptured.quantity).toBe(20);
    expect(updateCaptured.average_cost).toBe(155);
  });

  test('delete portfolio holding with confirm', async ({ page }) => {
    let deleteCalled = false;

    await mockAPI(page, portfolioOverrides({}, {
      'DELETE /users/me/portfolio/*': (route) => {
        deleteCalled = true;
        return route.fulfill({ status: 200, body: '{}' });
      },
    }));

    await page.goto('/dashboard');

    // Switch to Holdings tab
    await page.getByRole('button', { name: 'Holdings' }).click();
    await expect(page.getByTestId('portfolio-row-AAPL')).toBeVisible();

    // Right-click to open context menu (Radix ContextMenu)
    await page.getByTestId('portfolio-row-AAPL').click({ button: 'right' });

    // Click "Delete" in context menu (Radix renders items as role="menuitem")
    await page.getByRole('menuitem', { name: 'Delete' }).click();

    // Confirm dialog appears
    await expect(
      page.locator('text=Remove this holding from your portfolio?'),
    ).toBeVisible();

    // Click the confirm "Delete" button in the ConfirmDialog
    // The ConfirmDialog has two buttons: Cancel and the confirm label (Delete)
    const confirmDialog = page.locator('[role="dialog"]', {
      hasText: 'Remove this holding',
    });
    await confirmDialog.getByRole('button', { name: 'Delete' }).click();

    // Verify the DELETE request was sent
    expect(deleteCalled).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Onboarding
// ---------------------------------------------------------------------------

test.describe('Personalization', () => {
  test('personalization banner appears when onboarding incomplete', async ({ page }) => {
    // Do NOT suppress personalization for this test -- override beforeEach
    await page.addInitScript(() => {
      localStorage.removeItem('langalpha-onboarding-ignored-at');
      localStorage.removeItem('langalpha-personalization-snoozed-at');
    });

    await mockAPI(page, dashboardOverrides({
      'GET /users/me': {
        user_id: 'local-dev-user',
        name: 'Test User',
        email: 'test@test.com',
        onboarding_completed: false,
        has_api_key: true,
        has_oauth_token: false,
      },
    }));

    await page.goto('/dashboard');

    // Personalization banner should appear
    await expect(
      page.locator('text=Personalize your experience'),
    ).toBeVisible();

    // Dismiss button present (X icon with aria-label)
    const dismissBtn = page.getByRole('button', { name: 'Close' });
    await expect(dismissBtn).toBeVisible();

    // Click dismiss to close the banner
    await dismissBtn.click();

    // Banner should disappear
    await expect(
      page.locator('text=Personalize your experience'),
    ).not.toBeVisible();
  });
});
