// FARM-UI.8 — mobile (390x844) smoke for the /en/farm/ surface area.
//
// Covers the four farm routes shipped by FARM-UI.2..5:
//   - /en/farm/               list page (farm-list)
//   - /en/farm/<id>/          task detail
//   - /en/farm/plan/          slice DAG
//   - /en/farm/cost/          cost rollup
//
// Runs under the existing "mobile" Playwright project (viewport 390x844,
// see playwright.config.ts). The farm-api sidecar is mocked through
// page.route() so the spec is hermetic and can also run unmodified
// against the live deploy at https://protea.ngrok.app (the routes still
// intercept regardless of origin because the sidecar lives under a
// different hostname).
//
// Each route asserts the page loads, the mobile chrome (status pill
// from FARM-UI.3) mounts, and writes a fullPage screenshot to
// e2e/screenshots/farm-mobile-<route>.png so the playwright-platform
// agent can attach them to its summary.

import { test, expect, type Route } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ??
  "http://localhost:8801";

// Reused fixtures. We keep them shaped like the real sidecar response so
// the page does not have to special-case mock data.
const SAMPLE_TASKS = [
  {
    id: "executor-1779200000-aaaa",
    agent_name: "executor",
    kind: "executor",
    persistent: 0,
    status: "running",
    spawn_args: '{"task":"FARM-UI.8"}',
    worktree: "/home/user/worktrees/executor-1779200000-aaaa",
    tmux_window: "executor:1",
    pid: 4321,
    model: "claude-opus-4-7",
    created_at: new Date(Date.now() - 60_000).toISOString(),
    started_at: new Date(Date.now() - 50_000).toISOString(),
    ended_at: null,
    exit_code: null,
    worktree_owner_repo: "frapercan/PROTEA",
  },
  {
    id: "janitor-1779200001-bbbb",
    agent_name: "janitor",
    kind: "janitor",
    persistent: 0,
    status: "succeeded",
    spawn_args: '{"task":"trailer-sweep"}',
    worktree: "/home/user/worktrees/janitor-1779200001-bbbb",
    tmux_window: "janitor:1",
    pid: null,
    model: "claude-sonnet-4-7",
    created_at: new Date(Date.now() - 600_000).toISOString(),
    started_at: new Date(Date.now() - 580_000).toISOString(),
    ended_at: new Date(Date.now() - 300_000).toISOString(),
    exit_code: 0,
    worktree_owner_repo: "frapercan/agent-farm",
  },
];

const SAMPLE_HEARTBEATS = [
  {
    id: 1,
    ts: new Date(Date.now() - 45_000).toISOString(),
    level: "info",
    message: "started slice",
  },
  {
    id: 2,
    ts: new Date(Date.now() - 30_000).toISOString(),
    level: "info",
    message: "branch created",
  },
  {
    id: 3,
    ts: new Date(Date.now() - 15_000).toISOString(),
    level: "warning",
    message: "linter advisory",
  },
];

const SAMPLE_RESULT = {
  task_id: "executor-1779200000-aaaa",
  ts: new Date(Date.now() - 5_000).toISOString(),
  summary: "executor v2 — FARM-UI.8 mobile spec landed.",
  branches: "task/farm-ui-8",
  prs: "https://github.com/frapercan/PROTEA/pull/9999",
  sha_before: "abc1234abc1234abc1234abc1234abc1234abcd",
  sha_after: "def5678def5678def5678def5678def5678defa",
};

const SAMPLE_SLICES = [
  {
    id: "DEMO.A",
    title: "DEMO.A — root",
    loop: "demo-loop",
    phase: "F-DEMO",
    status: "pending",
    deps: [],
    priority: "P0",
    estimated_hours: 1,
    tags: ["demo"],
    requires_human: false,
    acceptance: "Root acceptance",
  },
  {
    id: "DEMO.B",
    title: "DEMO.B — leaf",
    loop: "demo-loop",
    phase: "F-DEMO",
    status: "pending",
    deps: ["DEMO.A"],
    priority: "P1",
    estimated_hours: 2,
    tags: ["demo"],
    requires_human: false,
    acceptance: "Leaf acceptance",
  },
];

const TODAY = new Date().toISOString().slice(0, 10);
const COST_BUCKETS = [
  {
    date: TODAY,
    agent: "executor",
    model: "opus-4.7",
    tasks: 2,
    input_tokens: 2000,
    output_tokens: 1000,
    cache_tokens: 0,
    usd: 5.0,
  },
];
const BUDGETS = [
  {
    name: "executor",
    kind: "subagent",
    model: "opus-4.7",
    expected_tokens: 120000,
    max_usd_per_day: 12.0,
  },
];

async function mockFarmApi(page: import("@playwright/test").Page) {
  // More specific routes are registered LAST so playwright's LIFO route
  // resolution prefers them over the broader /tasks* glob. The detail
  // page fetches /tasks/<id>, /tasks/<id>/heartbeats, /tasks/<id>/results
  // (note plural; matches lib/farmApi.ts getFarmResults).
  await page.route(`${FARM_API_ORIGIN}/tasks*`, (route: Route) => {
    const url = new URL(route.request().url());
    const statusF = url.searchParams.get("status");
    let rows = SAMPLE_TASKS.slice();
    if (statusF) rows = rows.filter((r) => r.status === statusF);
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(rows),
    });
  });
  await page.route(/\/tasks\/[^/]+$/, (route: Route) => {
    const url = new URL(route.request().url());
    const id = url.pathname.split("/").pop() ?? "";
    const row = SAMPLE_TASKS.find((t) => t.id === id) ?? SAMPLE_TASKS[0];
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(row),
    });
  });
  await page.route(/\/tasks\/[^/]+\/heartbeats/, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(SAMPLE_HEARTBEATS),
    }),
  );
  await page.route(/\/tasks\/[^/]+\/results/, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(SAMPLE_RESULT),
    }),
  );
  await page.route(`${FARM_API_ORIGIN}/plan*`, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(SAMPLE_SLICES),
    }),
  );
  await page.route(`${FARM_API_ORIGIN}/cost*`, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(COST_BUCKETS),
    }),
  );
  await page.route(`${FARM_API_ORIGIN}/agents/budgets*`, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(BUDGETS),
    }),
  );
  await page.route(`${FARM_API_ORIGIN}/healthz*`, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ok: true }),
    }),
  );
}

// Bypass the usage-policy modal. The other FARM-UI specs use this same
// pattern; documented in the FARM-UI.5 PR body as the clean way around
// the modal interception flakes called out in mobile-check.spec.ts.
test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try {
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore — only matters in non-localStorage contexts
    }
  });
});

test.describe("FARM-UI.8 mobile (390x844) — /en/farm/ surfaces", () => {
  test("list page renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/farm");

    await expect(
      page.getByRole("heading", { name: "Agent farm" }),
    ).toBeVisible();
    // Mobile card container visible.
    await expect(page.getByTestId("farm-list-mobile")).toBeVisible();
    // FarmChrome status pill mounts on /farm/*.
    await expect(page.getByTestId("farm-status-pill")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-mobile-list.png",
      fullPage: true,
    });
  });

  test("task detail renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto(`/en/farm/${SAMPLE_TASKS[0].id}`);

    await expect(page.getByTestId("farm-task-card")).toBeVisible();
    await expect(page.getByTestId("farm-summary")).toBeVisible();
    // Chrome from FARM-UI.3 stays mounted on the detail page.
    await expect(page.getByTestId("farm-status-pill")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-mobile-detail.png",
      fullPage: true,
    });
  });

  test("plan page renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/farm/plan");

    await expect(page.getByRole("heading", { name: "Slice DAG" })).toBeVisible();
    await expect(page.getByTestId("slice-dag-canvas")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-mobile-plan.png",
      fullPage: true,
    });
  });

  test("cost page renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/farm/cost");

    await expect(page.getByTestId("cost-summary-total")).toBeVisible();
    await expect(page.getByTestId("cost-chart-agent")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-mobile-cost.png",
      fullPage: true,
    });
  });

  // ── No horizontal overflow ────────────────────────────────────────────
  // Mirrors the legacy mobile-check.spec.ts overflow loop so the farm
  // routes never silently regress on a 390-wide viewport. Two-pixel
  // tolerance for sub-pixel rounding inside chrome.

  const ROUTES: { path: string; name: string }[] = [
    { path: "/en/farm", name: "list" },
    { path: `/en/farm/${SAMPLE_TASKS[0].id}`, name: "detail" },
    { path: "/en/farm/plan", name: "plan" },
    { path: "/en/farm/cost", name: "cost" },
  ];

  for (const { path, name } of ROUTES) {
    test(`no horizontal overflow on ${name}`, async ({ page }) => {
      await mockFarmApi(page);
      await page.goto(path);
      await page.waitForLoadState("networkidle");
      const bodyWidth = await page.evaluate(
        () => document.body.scrollWidth,
      );
      const viewportWidth = page.viewportSize()!.width;
      expect(
        bodyWidth,
        `${name}: scrollWidth=${bodyWidth} viewport=${viewportWidth}`,
      ).toBeLessThanOrEqual(viewportWidth + 2);
    });
  }
});
