// Critical user flow: the reranker page (/reranker) lists trained
// LightGBM models from /scoring/rerankers/, surfaces training-time
// metrics + feature importance when a card is expanded, and exposes
// the two lab-bridge import paths (multipart upload + register-by-URI)
// for boosters trained in protea-reranker-lab. The legacy in-PROTEA
// Train form was removed when TrainRerankerOperation was unregistered
// (POST /scoring/rerankers/train returns 405). The ContextBanner
// reports prerequisites from /embeddings/prediction-sets +
// /annotations/evaluation-sets. Hermetic via mock-api.

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
  feature_schema_sha: "feat0schema00abc",
  dataset_id: "ds-uuid-cafebabe",
  dataset_manifest_sha: "manif00sha00deadbeef",
  external_source: "protea-reranker-lab@cec8ccd",
  created_at: "2026-05-14T10:00:00Z",
};

test.describe("reranker flow", () => {
  test("page header renders import + register buttons and the empty-state copy when no rerankers exist", async ({
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
    // The two lab-bridge actions are the only entry points for new boosters
    // now that the in-PROTEA Train form is gone.
    await expect(
      page.getByRole("button", { name: /Import booster/ }),
    ).toBeVisible();
    await expect(
      page.getByRole("button", { name: /Register by URI/ }),
    ).toBeVisible();
    // Empty-state copy renders when no rerankers exist.
    await expect(
      page.getByText(
        "No re-ranker models registered yet. Use the buttons above to import a booster trained in protea-reranker-lab.",
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

  test("collapsed card shows monospace provenance strip with schema/manifest/dataset/source", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/scoring/rerankers/", [RERANKER_MODEL]);
    mockApi.override("/embeddings/prediction-sets/", PREDICTION_SETS);
    mockApi.override("/annotations/evaluation-sets", EVALUATION_SETS);
    mockApi.override("/annotations/sets/", ANNOTATION_SETS);

    await page.goto("/en/reranker/");

    // The strip lives on the collapsed card header and never requires
    // expanding the card. Its testid lets us scope the four pills
    // without colliding with the expanded-view ProvenancePanel.
    const strip = page.getByTestId("reranker-provenance-strip").first();
    await expect(strip).toBeVisible();
    await expect(strip.getByRole("button", { name: /^schema=/ })).toBeVisible();
    await expect(strip.getByRole("button", { name: /^manifest=/ })).toBeVisible();
    await expect(strip.getByRole("button", { name: /^dataset=/ })).toBeVisible();
    await expect(strip.getByRole("button", { name: /^source=/ })).toBeVisible();
  });

});
