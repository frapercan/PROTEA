// Critical reader on-ramp: how a single score is made.
//
// `/score/:accession/:go` is chapter zero of the interface-as-a-book. It walks
// one (protein, GO term) pair through the real pipeline order (retrieval,
// candidate, evidence, fusion, probability) in the scholarly register, reading
// REAL API fields and rendering honest empty/pending states where a value is
// not exposed rather than inventing one.
//
// The hermetic mock server returns `[]` for every non-showcase path (see
// e2e/mock-server/showcase-server.mjs), so under test the page resolves no
// prediction run and renders its structure in the pending/empty states. This
// smoke proves the route renders end to end for a sample accession/go and that
// the four numbered steps and the honest backend-gap note are all present.

import { test, expect } from "./fixtures/mock-api";

test.describe("score on-ramp", () => {
  const path = "/en/score/P12345/GO:0005515";

  test("renders the how-a-score-is-made walk for a sample pair", async ({ page }) => {
    await page.goto(path);

    await expect(
      page.getByRole("heading", { level: 1, name: /how a score is made/i }),
    ).toBeVisible();

    // The pair is echoed from the URL, verbatim.
    await expect(page.getByText("P12345").first()).toBeVisible();
    await expect(page.getByText("GO:0005515").first()).toBeVisible();
  });

  test("carries the four numbered steps and the caveats", async ({ page }) => {
    await page.goto(path);

    await expect(page.getByRole("heading", { name: /1\. The retrieval/i })).toBeVisible();
    await expect(page.getByRole("heading", { name: /2\. The evidence/i })).toBeVisible();
    await expect(page.getByRole("heading", { name: /3\. The fusion and the score/i })).toBeVisible();
    await expect(page.getByRole("heading", { name: /Our caveats/i })).toBeVisible();
  });

  test("states the single-pair-score backend gap instead of inventing a number", async ({ page }) => {
    await page.goto(path);

    // The honest gap: the fused probability for one pair is not a JSON field.
    await expect(
      page.getByText(/single-pair probability is not exposed by a JSON endpoint/i),
    ).toBeVisible();
    // And it names the real bulk contract rather than a fabricated value.
    await expect(page.getByText(/\/rerank\.tsv/i).first()).toBeVisible();
  });

  test("links back to the argument", async ({ page }) => {
    await page.goto(path);
    const back = page.getByRole("link", { name: /Back to the argument/i });
    await expect(back).toBeVisible();
    await back.click();
    await page.waitForURL(/\/en\/?$/);
    expect(page.url()).toMatch(/\/en\/?$/);
  });
});
