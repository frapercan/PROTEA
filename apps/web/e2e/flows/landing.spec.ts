// Critical user flow: landing page renders, pipeline tiles route to
// their pages, stats reflect the showcase payload, and the showcase
// error path surfaces a Retry CTA. The homepage best-result block is
// rendered by an RSC (HomeShowcase), so its data comes from a real
// HTTP mock (e2e/mock-server/showcase-server.mjs) launched by the
// Playwright webServer config; client-side fetches (jobs flow, etc.)
// are still intercepted by the page.route() fixture in mock-api.ts.

import { test, expect } from "./fixtures/mock-api";
import { resetShowcaseMock, setShowcaseResponse } from "./fixtures/showcase-mock";

test.describe("landing page", () => {
  test.beforeEach(async () => {
    // Defaults: populated showcase payload with stable IDs/values.
    await resetShowcaseMock();
  });

  test("hero renders PROTEA title and best-result block", async ({ page }) => {
    await page.goto("/en/");

    // Hero heading is always visible (server-rendered, data-independent).
    await expect(page.getByRole("heading", { name: /PROTEA/i, level: 1 })).toBeVisible();

    // Best-overall section anchors as id="best-<evaluation_result_id>".
    // The mock seeds er-fixture-001 so we know what to expect.
    const bestSection = page.locator("section[id^='best-']").first();
    await expect(bestSection).toBeVisible();

    // Within the best section, assert an Fmax decimal renders. The
    // mock seeds 0.612 so the 0.xxx pattern is guaranteed.
    await expect(bestSection.locator("text=/0\\.[0-9]{3}/").first()).toBeVisible();

    // The seeded display name renders verbatim.
    await expect(bestSection.getByText("ESM2 650M")).toBeVisible();
  });

  test("stats grid shows live counts from showcase payload", async ({ page }) => {
    await page.goto("/en/");

    // The stat cards render locale-formatted counts with thousand
    // separators (e.g. en-US "12,345", es-ES "12.345"). `toLocaleString`
    // picks the server runtime locale, so accept either separator.
    await expect(
      page.locator("text=/[0-9]{1,3}([.,][0-9]{3})+/").first(),
    ).toBeVisible();

    // All four stat label keys must render. Scope to the stats grid
    // (tabular-nums classes are unique to those tiles) so we do not
    // race against navigation/header instances of the same words.
    const statsTiles = page.locator("div.grid").filter({ hasText: /Proteins/i });
    await expect(statsTiles.getByText(/Proteins/i).first()).toBeVisible();
    await expect(statsTiles.getByText(/Sequences/i).first()).toBeVisible();
    await expect(statsTiles.getByText(/Embeddings/i).first()).toBeVisible();
    await expect(statsTiles.getByText(/Predictions/i).first()).toBeVisible();
  });

  test("pipeline tile navigates to its destination route", async ({ page }) => {
    await page.goto("/en/");

    // The pipeline tiles are <Link> components. Use role=link with
    // the semantic "sequences" name; that tile targets /proteins.
    const sequencesTile = page.getByRole("link", { name: /sequences/i }).first();
    await expect(sequencesTile).toBeVisible();
    await sequencesTile.click();
    await page.waitForURL(/\/proteins/);
    expect(page.url()).toMatch(/\/proteins/);
  });

  test("showcase API error surfaces retry CTA", async ({ page }) => {
    // Force the server-side fetch to error so HomeShowcase renders
    // its error branch. The catch path emits data-testid="showcase-error"
    // and a Retry link back to the homepage.
    await setShowcaseResponse({ detail: "downstream failed" }, 500);
    await page.goto("/en/");
    await expect(page.getByTestId("showcase-error")).toBeVisible();
    await expect(page.getByRole("link", { name: "Retry" })).toBeVisible();
  });

  test("empty showcase renders the no-data CTA", async ({ page }) => {
    // Same surface used on a cold-start deploy (no benchmark data
    // ingested yet). The empty branch emits data-testid="showcase-empty"
    // and a "Get Started" CTA to /proteins.
    await setShowcaseResponse({
      protein_stats: { total: 0, canonical: 0 },
      best: null,
      counts: {
        proteins: 0,
        sequences: 0,
        embeddings: 0,
        prediction_sets: 0,
        predictions: 0,
        reranker_models: 0,
        evaluations: 0,
      },
      pipeline_stages: [],
    });
    await page.goto("/en/");
    await expect(page.getByTestId("showcase-empty")).toBeVisible();
    await expect(page.getByRole("link", { name: /get started/i })).toBeVisible();
  });
});
