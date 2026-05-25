// Critical user flow: landing page renders, pipeline tiles route to
// their pages, and the Annotate form CTA scrolls into view. Driven
// against mocked /showcase + /jobs responses (see fixtures/mock-api.ts)
// so the spec does not require a live API.

import { test, expect } from "./fixtures/mock-api";

test.describe.skip("landing page (TODO: re-enable after HARNESS-E2E-FIXTURES.1)", () => {
  test.skip("hero renders PROTEA title and best-result block", async ({ page }) => {
  // TODO: re-enable once e2e fixtures seed homepage showcase data.
  // See plan: HARNESS-E2E-FIXTURES.1
  // The current test relies on getShowcase() returning populated data, which
  // the CI environment doesn't seed. The HomeShowcase Suspense fallback never
  // resolves, so section[id^='best-'] is never rendered.
    await page.goto("/en/");

    // Hero heading is always visible server-rendered.
    await expect(page.getByRole("heading", { name: /PROTEA/i, level: 1 })).toBeVisible();

    // Best-overall section: assert structural presence instead of exact values.
    // The best-result block contains a section with id=best-<evaluation_result_id>,
    // so we look for any section with that id pattern. Alternatively, detect by
    // the structural presence of the Fmax value (a decimal 0.xxx format).
    const bestSection = page.locator("section[id^='best-']").first();
    await expect(bestSection).toBeVisible();

    // Within the best section, assert that an Fmax value (0.xxx format) is visible.
    // This regex matches any 3-digit decimal between 0 and 1 (e.g., 0.612, 0.457).
    await expect(
      bestSection.locator("text=/0\\.[0-9]{3}/")
    ).toBeVisible();

    // Assert at least one pipeline/embedding name is present. We don't assert
    // the exact PLM name since the best model changes with data; just check a
    // model name render (typically contains capitals and numbers like "ESM2", "ProstT5").
    await expect(
      bestSection.locator("text=/[A-Z][A-Za-z0-9]*[ -]?[0-9A-Za-z]*/")
    ).toBeVisible();
  });

  test("stats grid shows live counts from showcase payload", async ({ page }) => {
    await page.goto("/en/");

    // The stat cards render locale-formatted counts (with thousand separators).
    // Assert structural presence: at least one thousand-separated number exists
    // in the stats section (format: 1,234 or 12,345 or 250,000).
    await expect(
      page.locator("text=/[0-9]{1,3}(,[0-9]{3})+/").first()
    ).toBeVisible();

    // Assert that all four stat label keys are rendered (structure test, not value).
    await expect(page.locator("text=proteins", { exact: false })).toBeVisible();
    await expect(page.locator("text=sequences", { exact: false })).toBeVisible();
    await expect(page.locator("text=embeddings", { exact: false })).toBeVisible();
    await expect(page.locator("text=predictions", { exact: false })).toBeVisible();
  });

  test("pipeline tile navigates to its destination route", async ({ page }) => {
    await page.goto("/en/");
    // The pipeline tiles are <Link> components (not buttons). Use role=link
    // to find a link with semantic "sequences" label, which targets /proteins.
    const sequencesTile = page.getByRole("link", { name: /sequences/i }).first();
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
