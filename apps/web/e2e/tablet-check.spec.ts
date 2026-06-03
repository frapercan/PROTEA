import { test, expect } from "@playwright/test";

// Tablet = 768px. The desktop sidebar rail shows at lg (1024px+), so the
// tablet viewport shows the hamburger + mobile drawer.

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

test("shows hamburger on tablet (sidebar rail at lg, not md)", async ({ page }) => {
  await page.goto("/jobs");
  await expect(
    page.locator('button[aria-controls="protea-mobile-nav"]')
  ).toBeVisible();
  // Desktop sidebar rail (lg:flex) is hidden below 1024px.
  await expect(page.locator("aside.hidden").first()).toBeHidden();
});

test("jobs page shows card layout on tablet", async ({ page }) => {
  await page.goto("/jobs");
  // Card layout uses lg:hidden (visible below 1024px); desktop table uses
  // lg:block (hidden below 1024px). Scope to the main content region so the
  // sidebar's own responsive utilities are not matched.
  const main = page.locator("main#main");
  await expect(main.locator("div.hidden.lg\\:block").first()).toBeHidden();
  await expect(main.locator("div.lg\\:hidden").first()).toBeVisible();
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
