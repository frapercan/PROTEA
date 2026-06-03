// Critical user flow: the landing-page AnnotateForm submits a FASTA,
// polls the embedding job to completion, launches prediction, polls
// that job, and finally auto-redirects to the prediction-set detail
// route. Hermetic via per-test mock-api overrides; the prediction
// lifecycle is driven by a function-body override that flips the job
// status on the second poll, so the spec exercises the full
// uploading -> embedding -> predicting -> done state machine without
// real workers or fake-timer hacks.
//
// Approach A (pure-mock). The annotate surface is a tight wrapper
// around four JSON endpoints, so a bundle-mode spec would add a CI
// dependency for negligible fidelity gain at this stage. A bundle-mode
// smoke can land later as F6.5b-2 if we want contract-drift coverage.

import { test, expect } from "./fixtures/mock-api";

const FASTA_SAMPLE = `>sp|TEST1|HELLO
MKTIIALSYIFCLVFA
`;

// Default annotate response fields. Tests can spread this and override
// only the keys they exercise, keeping per-test setup short.
const ANNOTATE_OK = {
  query_set_id: "qs-test-001",
  embedding_config_id: "emb-esm2",
  annotation_set_id: "as-001",
  ontology_snapshot_id: "ont-001",
  embedding_job_id: "job-embed-001",
  predict_payload: {
    embedding_config_id: "emb-esm2",
    annotation_set_id: "as-001",
    ontology_snapshot_id: "ont-001",
    query_set_id: "qs-test-001",
  },
  reranker_id: "reranker-001",
  sequence_count: 1,
};

const PREDICTION_SETS_OK = [
  {
    id: "ps-test-001",
    query_set_id: "qs-test-001",
    embedding_config_id: "emb-esm2",
    annotation_set_id: "as-001",
    ontology_snapshot_id: "ont-001",
    created_at: "2026-05-16T10:00:00Z",
  },
];

// Build a /jobs/<id> response that flips from running to succeeded
// after `flipAtCall` invocations, so the AnnotateForm's polling loop
// sees the transition. We keep the running phase short (call 1) so the
// spec stays under 10s even with POLL_MS=3000.
function jobLifecycle(id: string, flipAtCall = 2) {
  return ({ call }: { call: number }) => {
    const status = call >= flipAtCall ? "succeeded" : "running";
    return {
      id,
      operation: "compute_embeddings",
      operation_description: "test job",
      queue_name: "protea.embeddings",
      status,
      parent_job_id: null,
      created_at: "2026-05-16T10:00:00Z",
      started_at: "2026-05-16T10:00:01Z",
      finished_at: status === "succeeded" ? "2026-05-16T10:00:05Z" : null,
      progress_current: call,
      progress_total: 2,
    };
  };
}

test.describe("annotate flow", () => {
  test("submit button is disabled when the FASTA textarea is empty", async ({
    page,
    mockApi,
  }) => {
    // Default fixture mocks /jobs as running compute_embeddings, which
    // would trigger the queue-busy banner. Override to an empty list so
    // we assert the empty-input gating cleanly.
    mockApi.override("/jobs", []);
    await page.goto("/en/");
    const submit = page.getByRole("button", { name: /^Annotate$/ });
    await expect(submit).toBeDisabled();
  });

  test("queue-busy banner blocks submission while pipeline is saturated", async ({
    page,
  }) => {
    // Default mock has a running compute_embeddings job, which is in
    // BLOCKING_OPERATIONS, so the banner should render and the textarea
    // should be disabled even with content.
    await page.goto("/en/");
    await expect(page.getByText("Annotation temporarily unavailable")).toBeVisible();
    const textarea = page.getByLabel("Sequence input in FASTA format");
    await expect(textarea).toBeDisabled();
  });

  test("happy path: FASTA submission walks the embedding + prediction lifecycle and redirects", async ({
    page,
    mockApi,
  }) => {
    // Clear the queue so the submit button is enabled.
    mockApi.override("/jobs", []);
    mockApi.override("/annotate", ANNOTATE_OK);
    mockApi.override("/jobs/job-embed-001", jobLifecycle("job-embed-001"));
    mockApi.override("/embeddings/predict", { id: "job-predict-001", status: "queued" });
    mockApi.override("/jobs/job-predict-001", jobLifecycle("job-predict-001"));
    mockApi.override("/embeddings/prediction-sets/", PREDICTION_SETS_OK);

    await page.goto("/en/");
    const textarea = page.getByLabel("Sequence input in FASTA format");
    await expect(textarea).toBeEnabled();
    await textarea.fill(FASTA_SAMPLE);

    const submit = page.getByRole("button", { name: /^Annotate$/ });
    await expect(submit).toBeEnabled();
    await submit.click();

    // The button label cycles through the three stages. Use auto-retry
    // assertions so the 3s polling cadence does not flake. We give the
    // embedding + prediction stages generous timeouts (each takes one
    // POLL_MS tick before the override flips to succeeded).
    await expect(
      page.getByRole("button", { name: /Computing embeddings/ }),
    ).toBeVisible({ timeout: 15_000 });
    await expect(
      page.getByRole("button", { name: /Predicting GO terms/ }),
    ).toBeVisible({ timeout: 15_000 });

    // Final state surfaces the done label, then a 1.5s timer pushes to
    // /functional-annotation/{predictionSetId}?reranker_id=...
    await expect(page.getByText("Done! Redirecting to results...")).toBeVisible({
      timeout: 15_000,
    });
    await page.waitForURL(/\/functional-annotation\/ps-test-001/, {
      timeout: 10_000,
    });
    expect(page.url()).toMatch(/reranker_id=reranker-001/);
  });

  test("annotate API failure surfaces inline error and unlocks the form", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/jobs", []);
    mockApi.override("/annotate", { detail: "fasta parse failed" }, 422);

    await page.goto("/en/");
    const textarea = page.getByLabel("Sequence input in FASTA format");
    await textarea.fill(FASTA_SAMPLE);
    await page.getByRole("button", { name: /^Annotate$/ }).click();

    // Error message renders next to the submit button; the form leaves
    // the running stages so the user can retry.
    await expect(page.getByText(/fasta parse failed/)).toBeVisible({
      timeout: 10_000,
    });
    await expect(page.getByRole("button", { name: /^Annotate$/ })).toBeEnabled();
  });
});
