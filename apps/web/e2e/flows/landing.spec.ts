// Critical user flow: the front door is the argument, not a dashboard.
// `/` opens with the one-sentence thesis, sets the sealed nine-cell board
// as the hero (leading with the two cells it does not win), and offers the
// four pillars as chapters. Each `/pillar/n` carries the claim, the evidence,
// the pulled-footnote receipt, and the caveats. The hands-on annotate surface
// moved one level in to `/annotate` (covered by annotate.spec.ts).
//
// These pages make no client-side API calls, but we keep the mock-api fixture
// so the Playwright webServer (showcase mock) still boots for the project.

import { test, expect } from "./fixtures/mock-api";

test.describe("argument front door", () => {
  test("opens with the one-sentence thesis", async ({ page }) => {
    await page.goto("/en/");
    await expect(
      page.getByRole("heading", { level: 1, name: /Protein function is predictable/i }),
    ).toBeVisible();
  });

  test("the board leads with the two cells it does not win", async ({ page }) => {
    await page.goto("/en/");
    // The values are withdrawn while the campaign recomputes, so the test
    // asserts the finding that survives rather than the numbers that do not:
    // seven regimes the method carries and two it does not, the latter in
    // frontier rose inside a hairline bracket.
    await expect(page.getByText("carried").first()).toBeVisible();
    await expect(page.getByText("frontier").first()).toBeVisible();
    // The honest frame line is present.
    await expect(page.getByText(/the two we are not, we explain/i)).toBeVisible();
  });

  test("the four pillars are chapter links", async ({ page }) => {
    await page.goto("/en/");
    const chapter = page.getByRole("link", { name: /The taxonomy of evidence/i });
    await expect(chapter).toBeVisible();
    await chapter.click();
    await page.waitForURL(/\/pillar\/1/);
    expect(page.url()).toMatch(/\/pillar\/1/);
  });

  test("the quiet footer opens the instrument", async ({ page }) => {
    await page.goto("/en/");
    const instrument = page.getByRole("link", { name: /Open the instrument/i });
    await expect(instrument).toBeVisible();
    await instrument.click();
    await page.waitForURL(/\/benchmark/);
    expect(page.url()).toMatch(/\/benchmark/);
  });
});

test.describe("pillar page", () => {
  test("carries claim, evidence and the pulled-footnote receipt", async ({ page }) => {
    await page.goto("/en/pillar/1");

    await expect(page.getByRole("heading", { level: 1, name: /taxonomy of evidence/i })).toBeVisible();
    await expect(page.getByRole("heading", { name: /^The claim$/i })).toBeVisible();
    await expect(page.getByRole("heading", { name: /^The evidence$/i })).toBeVisible();
    await expect(page.getByRole("heading", { name: /Our caveats/i })).toBeVisible();

    // The receipt is a footnote you pull: a marker button, collapsed by default,
    // that expands the artifact path in place.
    const marker = page.getByRole("button", { name: /Show the receipt/i }).first();
    await expect(marker).toHaveAttribute("aria-expanded", "false");
    await marker.click();
    await expect(marker).toHaveAttribute("aria-expanded", "true");
    await expect(page.getByText("storage/feature_necessity/gain_report.json")).toBeVisible();
  });

  test("an out-of-range pillar renders the not-found boundary", async ({ page }) => {
    // parseN rejects anything outside 1..4, so the page calls notFound() and
    // the locale not-found boundary takes over. Assert the boundary is shown
    // and no pillar content leaked; the HTTP status is 404 in production and a
    // known 200 in the dev server, so we assert on the rendered boundary.
    await page.goto("/en/pillar/9");
    await expect(page.getByText("404").first()).toBeVisible();
    await expect(page.getByRole("heading", { name: /The claim/i })).toHaveCount(0);
  });
});
