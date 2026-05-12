import { test, expect } from "@playwright/test";

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

// Aria label is now locale-driven; the English label is "Open navigation menu".
const MENU_BTN = 'button[aria-label="Open navigation menu"], button[aria-label="Close navigation menu"]';

// ── Navigation ─────────────────────────────────────────────────────────────

test("header shows hamburger and hides desktop nav", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.locator(MENU_BTN)).toBeVisible();
  // Desktop nav (lg:flex) is hidden on mobile — it has display:none from 'hidden' class
  await expect(page.locator("nav.hidden").first()).toBeHidden();
});

test("hamburger opens dropdown with all nav links", async ({ page }) => {
  await page.goto("/jobs");
  await page.locator(MENU_BTN).click();
  // After opening, desktop nav links remain display:none, dropdown links become visible.
  // getByRole('link') only returns non-hidden elements.
  for (const label of NAV_LABELS) {
    await expect(page.getByRole("link", { name: label }).first()).toBeVisible();
  }
});

test("hamburger closes dropdown on second click", async ({ page }) => {
  await page.goto("/jobs");
  const btn = page.locator(MENU_BTN);
  // Dropdown div has class lg:hidden — target it specifically
  const dropdown = page.locator("div.lg\\:hidden.absolute");
  await btn.click();
  await expect(dropdown).toBeVisible();
  await btn.click();
  await expect(dropdown).toHaveCount(0);
});

test("menu closes automatically after navigating", async ({ page }) => {
  await page.goto("/jobs");
  await page.locator(MENU_BTN).click();
  const dropdown = page.locator("div.lg\\:hidden.absolute");
  await expect(dropdown).toBeVisible();
  // Click "Proteins" link in the dropdown (only this one is visible; desktop nav is display:none)
  await page.getByRole("link", { name: "Proteins" }).first().click();
  await page.waitForURL(/\/proteins/);
  // Dropdown should be removed from DOM after navigation
  await expect(dropdown).toHaveCount(0);
});

test("Support button hides text label on mobile", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.locator("header span.hidden.sm\\:inline")).toBeHidden();
});

// ── Jobs page ──────────────────────────────────────────────────────────────

test("jobs page shows card layout and hides desktop table", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.locator("div.hidden.lg\\:block").first()).toBeHidden();
  await expect(page.locator("div.lg\\:hidden").first()).toBeVisible();
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
  await page.waitForTimeout(300);
  await page.screenshot({ path: "e2e/screenshots/mobile-menu-open.png" });
});
