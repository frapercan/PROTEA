// Critical user flow: maintenance page loads orphan + unindexed stats,
// renders the two VacuumCards with status pills, and lets the user
// trigger vacuum POSTs that surface a success toast. Hermetic via the
// shared mock-api fixture; the destructive POSTs are intercepted by
// route mocks so no real data is touched.

import { test, expect } from "./fixtures/mock-api";

const SEQ_PREVIEW = {
  total_sequences: 12000,
  orphan_sequences: 42,
  referenced_sequences: 11958,
};

const EMB_PREVIEW_CLEAN = {
  total_embeddings: 9000,
  unindexed_embeddings: 0,
  indexed_embeddings: 9000,
};

test.describe("maintenance flow", () => {
  test("preview endpoints populate both vacuum cards", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/maintenance/vacuum-sequences/preview/", SEQ_PREVIEW);
    mockApi.override("/maintenance/vacuum-embeddings/preview/", EMB_PREVIEW_CLEAN);

    await page.goto("/en/maintenance/");

    await expect(
      page.getByRole("heading", { name: "Maintenance", level: 1 }),
    ).toBeVisible();
    // Orphan-sequences stat row reflects the mocked 42 figure.
    await expect(page.getByText("12,000").first()).toBeVisible();
    await expect(page.getByText("11,958")).toBeVisible();
    await expect(page.getByText("42").first()).toBeVisible();
  });

  test("orphan-pill flips to 'to clean' when orphans are present", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/maintenance/vacuum-sequences/preview/", SEQ_PREVIEW);
    mockApi.override("/maintenance/vacuum-embeddings/preview/", EMB_PREVIEW_CLEAN);

    await page.goto("/en/maintenance/");

    // Sequences card has orphans => amber "42 to clean" pill.
    await expect(page.getByText("42 to clean")).toBeVisible();
    // Embeddings card has none => green "Clean" pill.
    await expect(page.getByText("Clean", { exact: true })).toBeVisible();
  });

  test("vacuum button is disabled when there are no orphans", async ({
    page,
    mockApi,
  }) => {
    // Both endpoints report zero orphans.
    mockApi.override("/maintenance/vacuum-sequences/preview/", {
      total_sequences: 12000,
      orphan_sequences: 0,
      referenced_sequences: 12000,
    });
    mockApi.override("/maintenance/vacuum-embeddings/preview/", EMB_PREVIEW_CLEAN);

    await page.goto("/en/maintenance/");

    // Wait for stats to settle before asserting button state.
    await expect(page.getByText("12,000").first()).toBeVisible();
    const vacuumButtons = page.getByRole("button", { name: /Vacuum/ });
    await expect(vacuumButtons.first()).toBeDisabled();
    await expect(vacuumButtons.last()).toBeDisabled();
  });

  test("running the sequences vacuum POSTs and surfaces a success toast", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/maintenance/vacuum-sequences/preview/", SEQ_PREVIEW);
    mockApi.override("/maintenance/vacuum-embeddings/preview/", EMB_PREVIEW_CLEAN);
    // POST returns the deletion count; the handler also re-runs both
    // previews afterwards, so the override remains valid.
    mockApi.override("/maintenance/vacuum-sequences", { deleted_sequences: 42 });

    await page.goto("/en/maintenance/");

    // Wait for the orphan count to load so the Vacuum button enables.
    const vacuumSeq = page
      .getByRole("button", { name: /Vacuum \(42\)/ })
      .first();
    await expect(vacuumSeq).toBeEnabled();
    await vacuumSeq.click();

    // Toast renders the deleted-count message from the page handler.
    await expect(page.getByText(/Deleted 42 orphan sequence/)).toBeVisible();
  });
});
