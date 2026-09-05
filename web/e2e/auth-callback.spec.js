/**
 * The OAuth callback runs in one of two documents and cannot tell which from
 * its own markup: the tab the user was already in, or the child window
 * loginWithProvider opened. The success path has always branched on that. The
 * failure path did not, and a popup that navigates itself back to the login
 * route leaves a SECOND login page on screen, in a window the user now has to
 * find and close, while the real one waits untouched in the opener.
 *
 * Both branches are swept because the two are one `window.opener` apart and the
 * wrong one is invisible in ordinary browser QA -- a popup is exactly what a
 * headed test run does not open for you.
 */
import { test, expect, mockAPI } from './fixtures.js';

const DENIED = '/callback?error=access_denied&error_description=Sign-in%20was%20declined';

/** Count window.close() calls without letting one actually close the page. */
async function spyOnClose(page) {
  await page.addInitScript(() => {
    window.__closed = 0;
    window.close = () => { window.__closed += 1; };
  });
}

/** Present this document as the child window loginWithProvider opened. */
async function asPopup(page) {
  await page.addInitScript(() => {
    Object.defineProperty(window, 'opener', { value: { name: 'opener' }, configurable: true });
  });
}

test.describe('OAuth callback failure', () => {
  test('in a popup, the way out closes the window instead of navigating it', async ({ page }) => {
    await spyOnClose(page);
    await asPopup(page);
    await mockAPI(page);
    await page.goto(DENIED);

    await expect(page.getByText('Sign-in was declined')).toBeVisible();
    // The other way this page reaches the failure UI is a timeout, eight seconds
    // after it settled on "Signing you in" with nothing else on screen changing.
    // Only a live region makes that swap audible, so the role is asserted on the
    // branch a test can reach synchronously.
    await expect(page.getByRole('alert')).toContainText('Sign-in was declined');
    const button = page.getByRole('button');
    // Not "Back to login": there is nothing behind this window to go back to.
    await expect(button).toHaveText('Close');

    await button.click();
    expect(await page.evaluate(() => window.__closed)).toBe(1);
    // The popup stayed on the callback rather than growing a second login page.
    expect(new URL(page.url()).pathname).toBe('/callback');
  });

  test('in the top-level tab, the way out is still the login page', async ({ page }) => {
    await spyOnClose(page);
    await mockAPI(page);
    await page.goto(DENIED);

    await expect(page.getByText('Sign-in was declined')).toBeVisible();
    const button = page.getByRole('button');
    await expect(button).toHaveText('\u2190 Back to sign in');

    await button.click();
    await expect.poll(() => new URL(page.url()).pathname).not.toBe('/callback');
    expect(await page.evaluate(() => window.__closed)).toBe(0);
  });
});
