// Critical user flow: the scoring page (/scoring) lists ScoringConfigs
// from /scoring/configs/, opens the new-config form, and creates a
// config via POST /scoring/configs. The Load presets button hits
// /scoring/configs/presets and surfaces a success toast. Hermetic via
// the mock-api fixture.

import { test, expect } from "./fixtures/mock-api";

const CONFIG_LINEAR = {
  id: "sc-linear",
  name: "embed_only",
  formula: "linear",
  weights: {
    embedding_similarity: 1.0,
    identity_nw: 0.0,
    identity_sw: 0.0,
    evidence_weight: 0.0,
    taxonomic_proximity: 0.0,
  },
  evidence_weights: null,
  description: "Embedding similarity only",
  created_at: "2026-05-16T10:00:00Z",
};

const CONFIG_EVIDENCE_WEIGHTED = {
  id: "sc-ew",
  name: "alignment_blend",
  formula: "evidence_weighted",
  weights: {
    embedding_similarity: 0.5,
    identity_nw: 0.3,
    identity_sw: 0.2,
    evidence_weight: 0.0,
    taxonomic_proximity: 0.0,
  },
  evidence_weights: { IEA: 0.1 },
  description: "Custom evidence override",
  created_at: "2026-05-15T08:00:00Z",
};

test.describe("scoring flow", () => {
  test("renders configs returned by /scoring/configs/ with formula pills", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/configs/", [CONFIG_LINEAR, CONFIG_EVIDENCE_WEIGHTED]);

    await page.goto("/en/scoring/");

    await expect(
      page.getByRole("heading", { name: "Scoring Configs", level: 1 }),
    ).toBeVisible();
    await expect(page.getByText("embed_only")).toBeVisible();
    await expect(page.getByText("alignment_blend")).toBeVisible();
    // Formula chips render the formula name verbatim.
    await expect(page.getByText("linear", { exact: true }).first()).toBeVisible();
    await expect(
      page.getByText("evidence_weighted", { exact: true }).first(),
    ).toBeVisible();
    // The custom-evidence-weights badge appears for the second config.
    await expect(page.getByText("custom evidence weights")).toBeVisible();
  });

  test("empty-state shows the no-configs message when the API returns []", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/configs/", []);

    await page.goto("/en/scoring/");

    await expect(
      page.getByText(/No configs yet\. Load the presets or create one above\./),
    ).toBeVisible();
    // The new-config CTA is always available.
    await expect(
      page.getByRole("button", { name: /\+ New scoring config/ }),
    ).toBeVisible();
  });

  test("creating a config POSTs to /scoring/configs and appends to the list", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/configs/", []);
    // The create endpoint is unsuffixed (POST /scoring/configs). Match
    // the trailing-slash variant too via the override suffix matcher.
    mockApi.override("/scoring/configs", {
      ...CONFIG_LINEAR,
      id: "sc-new",
      name: "new_cfg",
    });

    await page.goto("/en/scoring/");

    await page.getByRole("button", { name: /\+ New scoring config/ }).click();
    // The form input has placeholder "my_config" (newConfigForm.namePlaceholder)
    // so we target it that way; the page does not wire htmlFor on the label.
    await page.getByPlaceholder("my_config").fill("new_cfg");
    await page.getByRole("button", { name: "Save config" }).click();

    // The new card is rendered via setConfigs((prev) => [...prev, c]).
    // Scope to the actual span (toasts also display the name briefly).
    await expect(
      page.locator("span.font-semibold").filter({ hasText: "new_cfg" }),
    ).toBeVisible({ timeout: 10_000 });
  });

  test("Load presets calls /scoring/configs/presets and surfaces created names", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/configs/presets", {
      created: ["embed_only", "alignment_blend"],
    });
    // The page fetches /scoring/configs/ on mount (call 1) and again
    // after presets land (call 2). Drive both through one function-body
    // override that flips after the first GET.
    mockApi.override("/scoring/configs/", ({ call }) =>
      call === 1 ? [] : [CONFIG_LINEAR, CONFIG_EVIDENCE_WEIGHTED],
    );

    await page.goto("/en/scoring/");
    await page.getByRole("button", { name: "Load presets" }).click();

    // The two presets surface in the list after the re-fetch. Scope to
    // the card span to avoid colliding with the toast banner that also
    // renders the names verbatim.
    await expect(
      page.locator("span.font-semibold").filter({ hasText: "embed_only" }),
    ).toBeVisible({ timeout: 10_000 });
    await expect(
      page.locator("span.font-semibold").filter({ hasText: "alignment_blend" }),
    ).toBeVisible({ timeout: 10_000 });
  });
});
