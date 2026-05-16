// Critical user flow: landing page renders, pipeline tiles route to
// their pages, and the Annotate form CTA scrolls into view. Driven
// against mocked /showcase + /jobs responses (see fixtures/mock-api.ts)
// so the spec does not require a live API.

import { test, expect } from "./fixtures/mock-api";

test.describe("landing page", () => {
  test("hero renders PROTEA title and best-result block", async ({ page }) => {
    await page.goto("/en/");

    await expect(page.getByRole("heading", { name: "PROTEA", level: 1 })).toBeVisible();
    // Best-overall card shows the mocked Fmax value to 3 dp.
    await expect(page.getByText("0.612")).toBeVisible();
    // ESM2 display name from mock pipes through.
    await expect(page.getByText("ESM2 650M")).toBeVisible();
  });

  test("stats grid shows live counts from showcase payload", async ({ page }) => {
    await page.goto("/en/");
    // The four headline stat cards (proteins / sequences / embeddings /
    // predictions) render the mocked counts. We assert by visible text
    // formatted with toLocaleString. waitFor handles the post-fetch
    // render gap when /showcase resolves.
    await expect(page.getByText("Platform Statistics", { exact: false })).toBeVisible();
    await expect(page.getByText("12,345").first()).toBeVisible();
    await expect(page.getByText("250,000").first()).toBeVisible();
  });

  test("pipeline tile navigates to its destination route", async ({ page }) => {
    await page.goto("/en/");
    // The first pipeline tile in the mock (sequences) targets /proteins.
    // Use role=button because the tile is a <button onClick>.
    const sequencesTile = page.getByRole("button", { name: /sequences/i }).first();
    await expect(sequencesTile).toBeVisible();
    await sequencesTile.click();
    await page.waitForURL(/\/proteins/);
    expect(page.url()).toMatch(/\/proteins/);
  });

  test("showcase API error surfaces retry button", async ({ page, mockApi }) => {
    mockApi.override("/showcase/", { detail: "downstream failed" }, 500);
    await page.goto("/en/");
    await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
  });
});
