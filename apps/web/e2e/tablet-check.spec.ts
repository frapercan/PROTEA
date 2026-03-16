import { test, expect } from "@playwright/test";

// Tablet = 768px. Desktop nav shows at lg (1024px+), so tablet shows hamburger.

const PAGES = [
  { path: "/jobs", name: "jobs" },
  { path: "/proteins", name: "proteins" },
  { path: "/embeddings", name: "embeddings" },
  { path: "/annotations", name: "annotations" },
  { path: "/query-sets", name: "query-sets" },
  { path: "/functional-annotation", name: "functional-annotation" },
];

test("shows hamburger on tablet (desktop nav at lg, not md)", async ({ page }) => {
  await page.goto("/jobs");
  await expect(page.locator('button[aria-label="Toggle menu"]')).toBeVisible();
  await expect(page.locator("nav.hidden").first()).toBeHidden();
});

test("jobs page shows card layout on tablet", async ({ page }) => {
  await page.goto("/jobs");
  // Card layout uses lg:hidden (visible below 1024px), desktop table uses lg:block (hidden below 1024px)
  await expect(page.locator("div.hidden.lg\\:block").first()).toBeHidden();
  await expect(page.locator("div.lg\\:hidden").first()).toBeVisible();
});

for (const { path, name } of PAGES) {
  test(`no horizontal overflow on ${name}`, async ({ page }) => {
    await page.goto(path);
    await page.waitForLoadState("networkidle");
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = page.viewportSize()!.width;
    expect(bodyWidth, `${name}: scrollWidth=${bodyWidth} viewport=${viewportWidth}`).toBeLessThanOrEqual(viewportWidth + 2);
  });
}

for (const { path, name } of PAGES) {
  test(`screenshot — ${name}`, async ({ page }) => {
    await page.goto(path);
    await page.waitForLoadState("networkidle");
    await page.screenshot({ path: `e2e/screenshots/tablet-${name}.png`, fullPage: true });
  });
}
