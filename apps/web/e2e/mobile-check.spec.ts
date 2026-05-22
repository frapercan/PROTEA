import { test, expect } from "@playwright/test";

// Dismiss the first-visit usage-policy modal before each test so it does not
// intercept clicks on the navigation chrome.
test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try { localStorage.setItem("protea_policy_accepted_v1", "1"); } catch {}
  });
});

const PAGES = [
  { path: "/jobs", name: "jobs" },
  { path: "/proteins", name: "proteins" },
  { path: "/embeddings", name: "embeddings" },
  { path: "/annotations", name: "annotations" },
  { path: "/query-sets", name: "query-sets" },
  { path: "/functional-annotation", name: "functional-annotation" },
];

const NAV_LABELS = [
  "Proteins", "GO Annotations", "Query Sets",
  "Embeddings", "KNN Search", "Scoring", "CAFA Evaluation",
  "Jobs", "Maintenance",
];

// The mobile top-bar hamburger. It carries aria-controls so it is
// distinguishable from the drawer's own close button (which shares the
// "Close navigation menu" label once the drawer is open).
const MENU_BTN = 'button[aria-controls="protea-mobile-nav"]';
// The mobile slide-in drawer (dialog) holding the sidebar nav.
const DRAWER = '#protea-mobile-nav[role="dialog"]';

// ── Navigation ─────────────────────────────────────────────────────────────

test("mobile top bar shows hamburger and hides desktop rail", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.locator(MENU_BTN)).toBeVisible();
  // Desktop sidebar rail (lg:flex) is hidden on mobile via the 'hidden' class.
  await expect(page.locator("aside.hidden").first()).toBeHidden();
});

test("hamburger opens drawer with all nav links", async ({ page }) => {
  await page.goto("/jobs");
  await page.locator(MENU_BTN).click();
  await expect(page.locator(DRAWER)).toBeVisible();
  // getByRole('link') only returns non-hidden elements; the drawer links
  // are the only visible nav links on mobile.
  for (const label of NAV_LABELS) {
    await expect(page.getByRole("link", { name: label }).first()).toBeVisible();
  }
});

test("drawer close button hides the drawer", async ({ page }) => {
  await page.goto("/jobs");
  const drawer = page.locator(DRAWER);
  await page.locator(MENU_BTN).click();
  await expect(drawer).toBeVisible();
  // Once open, the slide-in drawer covers the hamburger; the in-drawer
  // close button (or Esc / backdrop) is the close affordance.
  await drawer.getByRole("button", { name: "Close navigation menu" }).click();
  // Drawer slides off-screen (translate-x-full + invisible).
  await expect(drawer).toBeHidden();
});

test("Escape closes the drawer", async ({ page }) => {
  await page.goto("/jobs");
  const drawer = page.locator(DRAWER);
  await page.locator(MENU_BTN).click();
  await expect(drawer).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();
});

test("drawer closes automatically after navigating", async ({ page }) => {
  await page.goto("/jobs");
  await page.locator(MENU_BTN).click();
  const drawer = page.locator(DRAWER);
  await expect(drawer).toBeVisible();
  await page.getByRole("link", { name: "Proteins" }).first().click();
  await page.waitForURL(/\/proteins/);
  await expect(drawer).toBeHidden();
});

test("drawer closes on backdrop click", async ({ page }) => {
  await page.goto("/jobs");
  await page.locator(MENU_BTN).click();
  const drawer = page.locator(DRAWER);
  await expect(drawer).toBeVisible();
  // The backdrop overlay sits behind the drawer; click its top-right corner.
  await page.mouse.click(370, 400);
  await expect(drawer).toBeHidden();
});

// ── Jobs page ──────────────────────────────────────────────────────────────

test("jobs page shows card layout and hides desktop table", async ({ page }) => {
  await page.goto("/jobs");
  const main = page.locator("main#main");
  await expect(main.locator("div.hidden.lg\\:block").first()).toBeHidden();
  await expect(main.locator("div.lg\\:hidden").first()).toBeVisible();
});

// ── No overflow ────────────────────────────────────────────────────────────

for (const { path, name } of PAGES) {
  test(`no horizontal overflow on ${name}`, async ({ page }) => {
    await page.goto(path);
    await page.waitForLoadState("networkidle");
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = page.viewportSize()!.width;
    expect(bodyWidth, `${name}: scrollWidth=${bodyWidth} viewport=${viewportWidth}`).toBeLessThanOrEqual(viewportWidth + 2);
  });
}

// ── Screenshots ────────────────────────────────────────────────────────────

for (const { path, name } of PAGES) {
  test(`screenshot — ${name}`, async ({ page }) => {
    await page.goto(path);
    await page.waitForLoadState("networkidle");
    await page.screenshot({ path: `e2e/screenshots/mobile-${name}.png`, fullPage: true });
  });
}

test("screenshot — jobs menu open", async ({ page }) => {
  await page.goto("/jobs");
  await page.waitForLoadState("networkidle");
  await page.locator(MENU_BTN).click();
  await page.waitForTimeout(400);
  await page.screenshot({ path: "e2e/screenshots/mobile-menu-open.png" });
});
