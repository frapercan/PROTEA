// Critical user flow: the reranker page (/reranker) lists trained
// LightGBM models from /scoring/rerankers/, surfaces training-time
// metrics + feature importance when a card is expanded, and gates the
// Train button on the name/prediction-set/evaluation-set triple. The
// ContextBanner reports prerequisites from /embeddings/prediction-sets
// + /annotations/evaluation-sets. Hermetic via mock-api.

import { test, expect } from "./fixtures/mock-api";

const PREDICTION_SETS = [
  {
    id: "ps-001",
    embedding_config_id: "emb-esm2",
    embedding_config_name: "ESM2 650M",
    annotation_set_id: "as-001",
    annotation_set_label: "QuickGO 2026-05-01",
    limit_per_entry: 100,
    distance_threshold: null,
    created_at: "2026-05-10T10:00:00Z",
    prediction_count: 250_000,
  },
];

const EVALUATION_SETS = [
  {
    id: "es-001",
    old_annotation_set_id: "as-001",
    new_annotation_set_id: "as-002",
    created_at: "2026-05-12T09:00:00Z",
    stats: { delta_proteins: 1234 },
  },
];

const ANNOTATION_SETS = [
  {
    id: "as-001",
    source: "quickgo",
    source_version: "2026-05-01",
    ontology_snapshot_id: "ont-001",
    created_at: "2026-05-01T10:00:00Z",
  },
  {
    id: "as-002",
    source: "quickgo",
    source_version: "2026-05-15",
    ontology_snapshot_id: "ont-001",
    created_at: "2026-05-15T10:00:00Z",
  },
];

const RERANKER_MODEL = {
  id: "rr-aaa-001",
  name: "reranker-nk-bpo-v1",
  prediction_set_id: "ps-001",
  evaluation_set_id: "es-001",
  category: "nk",
  aspect: "bpo",
  metrics: {
    test_fmax: 0.6321,
    best_iteration: 142,
    val_auc: 0.81,
    val_logloss: 0.42,
    val_f1: 0.55,
    train_samples: 12_500,
    val_samples: 2_500,
    positive_rate_train: 0.0734,
  },
  feature_importance: {
    embedding_similarity: 4200,
    identity_nw: 1800,
    taxonomic_distance: 950,
  },
  created_at: "2026-05-14T10:00:00Z",
};

test.describe("reranker flow", () => {
  test("page header renders and Train button is disabled when required inputs are empty", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/rerankers/", []);
    mockApi.override("/embeddings/prediction-sets/", PREDICTION_SETS);
    mockApi.override("/annotations/evaluation-sets", EVALUATION_SETS);
    mockApi.override("/annotations/sets/", ANNOTATION_SETS);

    await page.goto("/en/reranker/");

    await expect(
      page.getByRole("heading", { name: "Re-ranker Models", level: 1 }),
    ).toBeVisible();
    await expect(page.getByRole("button", { name: /^Train$/ })).toBeDisabled();
    // Empty-state copy renders when no rerankers exist.
    await expect(
      page.getByText(
        "No re-ranker models trained yet. Use the form above to train one.",
      ),
    ).toBeVisible();
  });

  test("reranker list renders trained models with headline metrics", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/rerankers/", [RERANKER_MODEL]);
    mockApi.override("/embeddings/prediction-sets/", PREDICTION_SETS);
    mockApi.override("/annotations/evaluation-sets", EVALUATION_SETS);
    mockApi.override("/annotations/sets/", ANNOTATION_SETS);

    await page.goto("/en/reranker/");

    await expect(page.getByText("reranker-nk-bpo-v1")).toBeVisible();
    // Category + aspect badges render uppercased.
    await expect(page.getByText("nk", { exact: true })).toBeVisible();
    await expect(page.getByText("bpo", { exact: true })).toBeVisible();
    // Test Fmax surfaces formatted to 4 dp in the collapsed header.
    await expect(page.getByText("0.6321")).toBeVisible();
  });

  test("expanding a reranker card reveals feature importance + delete button", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/rerankers/", [RERANKER_MODEL]);
    mockApi.override("/embeddings/prediction-sets/", PREDICTION_SETS);
    mockApi.override("/annotations/evaluation-sets", EVALUATION_SETS);
    mockApi.override("/annotations/sets/", ANNOTATION_SETS);

    await page.goto("/en/reranker/");

    // Click the card header to expand. The header contains the model
    // name; using its parent wrapper would be brittle, so we click the
    // model name directly (event bubbles to the wrapper onClick).
    await page.getByText("reranker-nk-bpo-v1").click();

    await expect(page.getByText("Feature importance (gain)")).toBeVisible();
    await expect(page.getByText("embedding_similarity")).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Delete reranker" }),
    ).toBeVisible();
  });

  test("Train button enables once name, prediction set, and evaluation set are picked", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/rerankers/", []);
    mockApi.override("/embeddings/prediction-sets/", PREDICTION_SETS);
    mockApi.override("/annotations/evaluation-sets", EVALUATION_SETS);
    mockApi.override("/annotations/sets/", ANNOTATION_SETS);

    await page.goto("/en/reranker/");

    const trainBtn = page.getByRole("button", { name: /^Train$/ });
    await expect(trainBtn).toBeDisabled();

    // Labels are not wired with htmlFor; address the Name input by
    // its placeholder. The two selects sit immediately after their
    // label, so we target them as the label's adjacent sibling.
    await page.getByPlaceholder("e.g. reranker-nk-bpo-v1").fill("rr-test-001");
    const predSelect = page.locator("label:has-text('Prediction set') + select").first();
    const evalSelect = page.locator("label:has-text('Evaluation set') + select").first();
    await predSelect.selectOption("ps-001");
    await evalSelect.selectOption("es-001");

    await expect(trainBtn).toBeEnabled();
  });
});
