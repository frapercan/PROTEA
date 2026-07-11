// Critical route-move regression for the "interface as a book" redesign:
// the operational dashboard now lives under /instrument/*. This spec proves
//   1. the /instrument hub renders and links to the operational tools, and
//   2. an old top-level path (/en/jobs) permanently redirects to its new
//      /instrument/... home, so no stale bookmark or missed link 404s.
// Hermetic via the shared mock-api fixture (the hub itself fetches nothing).

import { test, expect } from "./fixtures/mock-api";

test.describe("instrument hub + redirects", () => {
  test("hub renders and links to the operational tools", async ({ page }) => {
    await page.goto("/en/instrument/");

    await expect(
      page.getByRole("heading", { name: "The instrument", level: 1 }),
    ).toBeVisible();

    // A representative tool from each group is reachable from the hub.
    await expect(page.getByRole("link", { name: /Jobs/ }).first()).toBeVisible();
    await expect(page.getByRole("link", { name: /Benchmark/ }).first()).toBeVisible();
    await expect(page.getByRole("link", { name: /Proteins/ }).first()).toBeVisible();
  });

  test("old top-level path redirects to /instrument/*", async ({ page }) => {
    await page.goto("/en/jobs/");
    await expect(page).toHaveURL(/\/en\/instrument\/jobs\/?$/);
  });

  test("old nested path redirects to /instrument/*", async ({ page }) => {
    await page.goto("/en/proteins/P12345/");
    await expect(page).toHaveURL(/\/en\/instrument\/proteins\/P12345\/?$/);
  });
});
