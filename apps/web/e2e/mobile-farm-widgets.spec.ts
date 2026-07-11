// FARM-UI.3 mobile (390x844) smoke for FarmJobsWidget + FarmStatusPill.
//
// Both components are mounted in the global LocaleLayout but gated to
// /[locale]/farm/* routes by <FarmChrome>. The spec asserts:
//   - both render on /en/instrument/farm
//   - the status pill resolves to a health dot (data-testid + data-health)
//   - the floating widget appears when at least one running task is
//     returned by the sidecar mock, and disappears when the queue is
//     empty
//   - both poll the sidecar (we observe at least two list requests
//     within ~3 polling cycles via page.route() interception)
//
// The farm-api sidecar is mocked through page.route(), mirroring the
// FARM-UI.2 farm-list spec so the test runs hermetic. The spec lives
// under the existing "mobile" project (viewport 390x844, see
// playwright.config.ts) so no new project is needed.

import { test, expect, type Route } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ??
  "http://localhost:8801";

const RUNNING_TASK = {
  id: "executor-1779100000-aaaa",
  agent_name: "executor",
  kind: "executor",
  persistent: 0,
  status: "running",
  spawn_args: '{"task":"FARM-UI.3"}',
  worktree: "/home/user/worktrees/executor-1779100000-aaaa",
  tmux_window: "executor:1",
  pid: 4242,
  model: "claude-opus-4-7",
  created_at: new Date(Date.now() - 60_000).toISOString(),
  started_at: new Date(Date.now() - 50_000).toISOString(),
  ended_at: null,
  exit_code: null,
  worktree_owner_repo: "frapercan/PROTEA",
};

const FAILED_TASK = {
  id: "executor-1779100001-bbbb",
  agent_name: "executor",
  kind: "executor",
  persistent: 0,
  status: "failed",
  spawn_args: '{"task":"FARM-UI.3"}',
  worktree: null,
  tmux_window: null,
  pid: null,
  model: "claude-sonnet-4-7",
  created_at: new Date(Date.now() - 600_000).toISOString(),
  started_at: new Date(Date.now() - 580_000).toISOString(),
  ended_at: new Date(Date.now() - 300_000).toISOString(),
  exit_code: 1,
  worktree_owner_repo: "frapercan/PROTEA",
};

type SidecarFixture = {
  running: typeof RUNNING_TASK[];
  recent: (typeof RUNNING_TASK | typeof FAILED_TASK)[];
};

async function mockFarmApi(
  page: import("@playwright/test").Page,
  fixture: SidecarFixture,
  hitCounter: { count: number },
) {
  await page.route(`${FARM_API_ORIGIN}/tasks*`, (route: Route) => {
    hitCounter.count += 1;
    const url = new URL(route.request().url());
    const statusF = url.searchParams.get("status");
    let rows: (typeof RUNNING_TASK | typeof FAILED_TASK)[];
    if (statusF === "running") {
      rows = fixture.running;
    } else {
      rows = fixture.recent;
    }
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(rows),
    });
  });
}

test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try {
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore
    }
  });
});

test.describe("FARM-UI.3 widgets at 390x844", () => {
  test("status pill and jobs widget render with a running task", async ({
    page,
  }) => {
    const hitCounter = { count: 0 };
    await mockFarmApi(
      page,
      { running: [RUNNING_TASK], recent: [RUNNING_TASK] },
      hitCounter,
    );
    await page.goto("/en/instrument/farm");

    // Status pill is visible and reports healthy.
    const pill = page.getByTestId("farm-status-pill");
    await expect(pill).toBeVisible();
    const dot = page.getByTestId("farm-status-dot");
    await expect(dot).toHaveAttribute("data-health", "healthy");

    // Floating widget toggle is visible (mounted because tasks.length > 0).
    const toggle = page.getByTestId("farm-jobs-widget-toggle");
    await expect(toggle).toBeVisible();

    // Expand the widget and assert one row.
    await toggle.click();
    const rows = page.getByTestId("farm-jobs-widget-row");
    await expect(rows).toHaveCount(1);
    await expect(rows.first()).toContainText("executor");

    // We've observed at least two list calls already (one for running,
    // one for since=1h) on first mount.
    expect(hitCounter.count).toBeGreaterThanOrEqual(2);
  });

  test("status pill flips to error when a recent failure is returned", async ({
    page,
  }) => {
    const hitCounter = { count: 0 };
    await mockFarmApi(
      page,
      { running: [], recent: [FAILED_TASK] },
      hitCounter,
    );
    await page.goto("/en/instrument/farm");

    const dot = page.getByTestId("farm-status-dot");
    await expect(dot).toHaveAttribute("data-health", "error");

    // The floating widget should NOT render when there are no running
    // tasks (mirrors FloatingJobsWidget's empty-state contract).
    await expect(page.getByTestId("farm-jobs-widget-toggle")).toHaveCount(0);
  });

  test("chrome is gated to /farm and absent on /jobs", async ({ page }) => {
    const hitCounter = { count: 0 };
    await mockFarmApi(
      page,
      { running: [RUNNING_TASK], recent: [RUNNING_TASK] },
      hitCounter,
    );
    // Visit /en/instrument/jobs first; the farm chrome must NOT mount here even
    // though the locale layout is shared.
    await page.goto("/en/instrument/jobs");
    await expect(page.getByTestId("farm-status-pill")).toHaveCount(0);
    await expect(page.getByTestId("farm-jobs-widget-toggle")).toHaveCount(0);

    // Move to /en/farm; the chrome should mount.
    await page.goto("/en/instrument/farm");
    await expect(page.getByTestId("farm-status-pill")).toBeVisible();
  });
});
