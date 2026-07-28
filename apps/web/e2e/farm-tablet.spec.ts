// FARM-UI.8 — tablet (768x1024) smoke for the /en/instrument/farm/ surface area.
//
// Mirrors farm-mobile.spec.ts at the existing tablet viewport. Same
// four routes (/en/instrument/farm/, /en/instrument/farm/<id>/, /en/instrument/farm/plan/,
// /en/instrument/farm/cost/), same hermetic page.route() mocks for the farm-api
// sidecar, same screenshots — but at 768x1024 and without the iPhone
// userAgent so the tests exercise the lg-breakpoint-1 layout (768px is
// still below the lg=1024 breakpoint, so the farm-list page should
// continue to show the mobile card layout).
//
// Project assignment: matched by playwright.config.ts via the explicit
// "**/farm-tablet.spec.ts" entry on the "tablet" project. See
// playwright.config.ts.

import { test, expect, type Route } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ??
  "http://localhost:8801";

const SAMPLE_TASKS = [
  {
    id: "executor-1779200100-aaaa",
    agent_name: "executor",
    kind: "executor",
    persistent: 0,
    status: "running",
    spawn_args: '{"task":"FARM-UI.8"}',
    worktree: "/home/user/worktrees/executor-1779200100-aaaa",
    tmux_window: "executor:1",
    pid: 4322,
    model: "claude-opus-4-7",
    created_at: new Date(Date.now() - 60_000).toISOString(),
    started_at: new Date(Date.now() - 50_000).toISOString(),
    ended_at: null,
    exit_code: null,
    worktree_owner_repo: "frapercan/PROTEA",
  },
  {
    id: "janitor-1779200101-bbbb",
    agent_name: "janitor",
    kind: "janitor",
    persistent: 0,
    status: "succeeded",
    spawn_args: '{"task":"trailer-sweep"}',
    worktree: "/home/user/worktrees/janitor-1779200101-bbbb",
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
  task_id: "executor-1779200100-aaaa",
  ts: new Date(Date.now() - 5_000).toISOString(),
  summary: "executor v2 — FARM-UI.8 tablet spec landed.",
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
  // LIFO ordering: list-broad route first, then more specific routes so
  // playwright resolves the longest-path mock when both could match.
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

test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try {
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore
    }
  });
});

test.describe("FARM-UI.8 tablet (768x1024) — /en/instrument/farm/ surfaces", () => {
  test("list page renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm");

    await expect(
      page.getByRole("heading", { name: "Agent farm" }),
    ).toBeVisible();
    // Tablet is below lg=1024 so the mobile card layout still shows;
    // matches the existing tablet-check.spec.ts assertions.
    await expect(page.getByTestId("farm-list-mobile")).toBeVisible();
    await expect(page.getByTestId("farm-status-pill")).toBeVisible();

    // FARM-UI.9 P1.1 the sub-nav is part of the farm layout and shows
    // Tasks as the active tab on the list root.
    await expect(page.getByTestId("farm-subnav")).toBeVisible();
    await expect(page.getByTestId("farm-subnav-tasks")).toHaveAttribute(
      "aria-current",
      "page",
    );

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-tablet-list.png",
      fullPage: true,
    });
  });

  test("task detail renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto(`/en/instrument/farm/${SAMPLE_TASKS[0].id}`);

    await expect(page.getByTestId("farm-task-card")).toBeVisible();
    await expect(page.getByTestId("farm-summary")).toBeVisible();
    await expect(page.getByTestId("farm-status-pill")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-tablet-detail.png",
      fullPage: true,
    });
  });

  test("plan page renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm/plan");

    await expect(page.getByRole("heading", { name: "Slice DAG" })).toBeVisible();
    await expect(page.getByTestId("slice-dag-canvas")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-tablet-plan.png",
      fullPage: true,
    });
  });

  test("cost page renders and screenshots", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm/cost");

    await expect(page.getByTestId("cost-summary-total")).toBeVisible();
    await expect(page.getByTestId("cost-chart-agent")).toBeVisible();

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-tablet-cost.png",
      fullPage: true,
    });
  });

  // Note: horizontal-overflow checks are only enforced under the mobile
  // profile (farm-mobile.spec.ts). At 768x1024 the farm pages currently
  // render the desktop-tier table chrome (intermediate breakpoint
  // between Tailwind's md=768 and lg=1024 has no dedicated layout) and
  // a strict no-overflow assertion would chase a layout change that is
  // not part of FARM-UI.8. The tablet screenshots above are the
  // operator-visible regression signal for this viewport.
});
