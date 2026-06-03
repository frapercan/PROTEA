import { test, expect } from "@playwright/test";

// Verify the mobile drawer + backdrop fully occlude page content even after
// scrolling. The hard requirement: with the drawer open, every viewport
// pixel must either be the drawer (opaque white) or the dim backdrop —
// nothing from the page underneath should be visible OR scrollable.
//
// Bug repro pre-fix: opening the drawer on a long page (e.g. /jobs) and
// trying to scroll on the strip to the right of the drawer would scroll
// the page underneath because (a) the backdrop did not block touch /
// wheel events strongly enough on iOS Safari and (b) the body scroll lock
// only froze `overflow`, leaving the `html` element scrollable on iOS.
// Fix: lock html + body overflow, set the backdrop touch-action: none and
// overscroll-behavior: contain.

test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try { localStorage.setItem("protea_policy_accepted_v1", "1"); } catch {}
  });
});

const MENU_BTN = 'button[aria-controls="protea-mobile-nav"]';
const DRAWER = '#protea-mobile-nav[role="dialog"]';
// The backdrop has aria-hidden and lives next to the drawer. It is the
// only fixed inset-0 element with `bg-stone-900/40`.
const BACKDROP = 'div.lg\\:hidden.fixed.inset-0[aria-hidden="true"]';

for (const width of [390, 430, 768]) {
  test(`drawer + backdrop occlude viewport at width=${width}`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    await page.goto("/jobs");
    await page.waitForLoadState("networkidle");

    // Scroll the page partway down before opening the drawer; the drawer
    // must still cover the visible viewport at the new scroll offset.
    await page.evaluate(() => window.scrollTo(0, 400));

    // Tablet (768) is the lg breakpoint boundary; the menu button still
    // shows because `lg:hidden` resolves to "hidden at >=1024px".
    await page.locator(MENU_BTN).click();
    await expect(page.locator(DRAWER)).toBeVisible();

    // The backdrop must be present and pinned to the viewport.
    const backdrop = page.locator(BACKDROP);
    await expect(backdrop).toBeVisible();
    const backdropBox = await backdrop.boundingBox();
    expect(backdropBox).not.toBeNull();
    expect(backdropBox!.x).toBeLessThanOrEqual(0);
    expect(backdropBox!.y).toBeLessThanOrEqual(0);
    expect(backdropBox!.width).toBeGreaterThanOrEqual(width - 1);
    expect(backdropBox!.height).toBeGreaterThanOrEqual(800);

    // While the drawer is open, attempting to scroll on the gap to the
    // right of the drawer must NOT change the page scroll offset.
    const before = await page.evaluate(() =>
      Math.max(document.documentElement.scrollTop, document.body.scrollTop),
    );
    // Aim the wheel at a point well to the right of the drawer width
    // (drawer is 85vw, max-w-xs = 320px). Use width-10 to land in the
    // backdrop strip on narrow viewports.
    await page.mouse.move(width - 10, 400);
    await page.mouse.wheel(0, 600);
    await page.waitForTimeout(120);
    const after = await page.evaluate(() =>
      Math.max(document.documentElement.scrollTop, document.body.scrollTop),
    );
    expect(after, `page scrolled while drawer open (before=${before} after=${after})`).toBe(before);

    // Screenshot for visual regression.
    await page.screenshot({ path: `e2e/screenshots/drawer-bleed-${width}.png` });
  });
}
