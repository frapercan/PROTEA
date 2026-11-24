// Critical user flow: the front door is the instrument (the author, 2026-11-24).
// `/` opens with what PROTEA does in plain words and the annotate form, then
// live figures, what has changed and what comes next, and where to go from
// there. The pillar pages stay reachable by their own URL, with their own test.
//
// We keep the mock-api fixture so the Playwright webServer (showcase mock)
// still boots for the project. The figures are read on the server and show a
// dash when the API cannot answer.
import { test, expect } from "./fixtures/mock-api";

test.describe("instrument front door", () => {
  test("opens in plain language, with the annotate form on the page", async ({ page }) => {
    await page.goto("/en/");
    await expect(
      page.getByRole("heading", { level: 1, name: /predicts what a protein does/i }),
    ).toBeVisible();
    await expect(page.locator("#annotate-form")).toBeVisible();
  });

  test("names who built it", async ({ page }) => {
    await page.goto("/en/");
    await expect(page.getByRole("link", { name: /Pérez Canales/i })).toBeVisible();
    await expect(page.getByRole("link", { name: /^ORCID$/ })).toBeVisible();
  });

  test("shows the figures, what is new and what comes next, and no sealed board", async ({ page }) => {
    await page.goto("/en/");
    await expect(page.getByRole("heading", { level: 2, name: /In figures/i })).toBeVisible();
    await expect(page.getByRole("heading", { level: 2, name: /What is new/i })).toBeVisible();
    await expect(page.getByRole("heading", { level: 2, name: /What comes next/i })).toBeVisible();
    await expect(page.getByText(/we do not win, we explain/i)).toHaveCount(0);
  });

  test("the footer opens the instrument", async ({ page }) => {
    await page.goto("/en/");
    const all = page.getByRole("link", { name: /Open the instrument/i });
    for (const href of await all.evaluateAll((els) => els.map((e) => e.getAttribute("href")))) {
      expect(href).toMatch(/\/instrument\/?$/);
    }
    const instrument = all.first();
    await expect(instrument).toBeVisible();
    await instrument.click();
    await page.waitForURL(/\/instrument\/?$/);
    expect(page.url()).toMatch(/\/instrument\/?$/);
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
