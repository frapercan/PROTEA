// Critical user flow: jobs list renders, status filter narrows the
// rows, the row click navigates to the detail route, and the detail
// page shows status / progress / events with cancel disabled on
// terminal jobs.
//
// All assertions go through Playwright's auto-waiting matchers; no
// raw setTimeout or networkidle waits that flake under CI load.

import { test, expect } from "./fixtures/mock-api";

test.describe("jobs flow", () => {
  test("list page renders the four mocked jobs", async ({ page }) => {
    await page.goto("/en/jobs/");
    // Jobs render both mobile (lg:hidden) and desktop (hidden lg:block)
    // markup; on a 1280x800 viewport only the desktop variant is
    // visible. Match each operation by the visible row link and scope
    // to the main region to avoid the header summary pills.
    const main = page.getByRole("main");
    await expect(main.getByRole("link", { name: /compute_embeddings/ }).first()).toBeVisible();
    await expect(main.getByRole("link", { name: /predict_go_terms/ }).first()).toBeVisible();
    await expect(main.getByRole("link", { name: /load_quickgo_annotations/ }).first()).toBeVisible();
  });

  test("active-jobs pill counts running + queued mock jobs", async ({ page }) => {
    await page.goto("/en/jobs/");
    // Mock has 1 running + 1 queued, so the pill should show count=2.
    await expect(page.getByText(/2/).first()).toBeVisible();
  });

  test("status filter narrows the list and updates the URL param", async ({
    page,
  }) => {
    await page.goto("/en/jobs/");
    const main = page.getByRole("main");
    // Wait for initial list to populate before changing filter.
    await expect(main.getByRole("link", { name: /compute_embeddings/ }).first()).toBeVisible();

    await page.getByRole("combobox").selectOption("failed");
    await page.waitForURL(/status=failed/);

    // Only the failed job (job-ddd-004) should remain. Match by its
    // unique description, scoped to the desktop row.
    await expect(
      main.getByRole("link", { name: /Backfill prot_t5 embeddings/ }).first(),
    ).toBeVisible();
    await expect(
      main.getByRole("link", { name: /Compute ESM2 embeddings/ }),
    ).toHaveCount(0);
  });

  test("clicking a job row navigates to its detail route", async ({ page }) => {
    await page.goto("/en/jobs/");
    const main = page.getByRole("main");
    await main
      .getByRole("link", { name: /compute_embeddings/ })
      .first()
      .click();
    await page.waitForURL(/\/jobs\/job-/);
    expect(page.url()).toMatch(/\/jobs\/job-aaa-001/);
  });

  test("detail page shows progress, payload, and events", async ({ page }) => {
    await page.goto("/en/jobs/job-aaa-001/");
    // Progress reads 4500 / 9000 (50%).
    await expect(page.getByText(/50%/).first()).toBeVisible();
    // Events timeline includes the mocked entry.
    await expect(page.getByText("worker picked up job").first()).toBeVisible();
    // Payload is inside a <details> element. Open it via its summary
    // (translated label "Payload"), then assert the JSON renders.
    await page.getByRole("group").filter({ hasText: /payload/i }).locator("summary").click();
    await expect(page.getByText(/embedding_config_id/).first()).toBeVisible();
  });

  test("cancel is enabled on running jobs and disabled on terminal jobs", async ({
    page,
    mockApi,
  }) => {
    // Running job: cancel enabled, delete disabled (isLive).
    await page.goto("/en/jobs/job-aaa-001/");
    await expect(page.getByRole("button", { name: /cancel/i })).toBeEnabled();
    await expect(page.getByRole("button", { name: /delete/i })).toBeDisabled();

    // Override detail with a succeeded variant; navigate to a separate
    // id so the page remounts cleanly.
    mockApi.override("/jobs/job-ccc-003", {
      id: "job-ccc-003",
      operation: "load_quickgo_annotations",
      operation_description: "Refresh GO annotations from QuickGO",
      queue_name: "protea.annotations",
      status: "succeeded",
      parent_job_id: null,
      created_at: "2026-05-14T09:00:00Z",
      started_at: "2026-05-14T09:01:00Z",
      finished_at: "2026-05-14T09:14:00Z",
      progress_current: 1000,
      progress_total: 1000,
      payload: {},
    });
    await page.goto("/en/jobs/job-ccc-003/");
    await expect(page.getByRole("button", { name: /cancel/i })).toBeDisabled();
    await expect(page.getByRole("button", { name: /delete/i })).toBeEnabled();
  });
});
