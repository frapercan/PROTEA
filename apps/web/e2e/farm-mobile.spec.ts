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

    // FARM-UI.9 P1.1 intra-farm sub-nav is mounted with Tasks active.
    await expect(page.getByTestId("farm-subnav")).toBeVisible();
    await expect(page.getByTestId("farm-subnav-tasks")).toHaveAttribute(
      "aria-current",
      "page",
    );
    // Next.js normalises trailing slashes at the rendered href; match
    // both forms so the assertion is independent of the trailing-slash
    // config flag.
    await expect(page.getByTestId("farm-subnav-plan")).toHaveAttribute(
      "href",
      /^\/en\/farm\/plan\/?$/,
    );
    await expect(page.getByTestId("farm-subnav-cost")).toHaveAttribute(
      "href",
      /^\/en\/farm\/cost\/?$/,
    );

    // FARM-UI.9 P1.3 status filter chips have a >= 44 px touch target on
    // mobile. The "All" chip uses the same size class so we measure the
    // running status chip which is always rendered.
    const runningChip = page.getByTestId("farm-status-chip-running");
    await expect(runningChip).toBeVisible();
    const chipHeight = await runningChip.evaluate(
      (el) => (el as HTMLElement).getBoundingClientRect().height,
    );
    expect(chipHeight).toBeGreaterThanOrEqual(44);

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

    // FARM-UI.9 P1.6 back link is locale-aware (/<locale>/farm, not
    // bare /farm which depends on Next.js middleware to redirect).
    await expect(page.getByTestId("farm-detail-back-link")).toHaveAttribute(
      "href",
      /^\/en\/farm\/?$/,
    );

    // FARM-UI.9 P1.1 sub-nav stays mounted on detail and shows Tasks as
    // the active tab (detail pages belong to the tasks namespace).
    await expect(page.getByTestId("farm-subnav-tasks")).toHaveAttribute(
      "aria-current",
      "page",
    );

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

    // FARM-UI.9 P0.2: cytoscape canvas now carries role=img + aria-label
    // and a visually-hidden slice list summary for screen readers.
    await expect(page.getByTestId("slice-dag-canvas")).toHaveAttribute(
      "role",
      "img",
    );
    await expect(page.getByTestId("slice-dag-canvas")).toHaveAttribute(
      "aria-label",
      /Slice dependency graph/,
    );
    await expect(page.getByTestId("slice-dag-sr-summary")).toBeAttached();

    // FARM-UI.9 P1.1 sub-nav shows Plan as the active tab.
    await expect(page.getByTestId("farm-subnav-plan")).toHaveAttribute(
      "aria-current",
      "page",
    );

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

    // FARM-UI.9 P0.1: at 390 px viewport the agent chart SVG must render
    // labels at >= 12 px effective size. The compact viewBox is 360 wide,
    // so an 11-12 px SVG fontSize renders close to 1:1 against a ~358 px
    // chart container after gutters.
    const chart = page.getByTestId("cost-chart-agent");
    const measured = await chart.evaluate((svg) => {
      const el = svg as SVGSVGElement;
      const vb = el.viewBox.baseVal;
      const rect = el.getBoundingClientRect();
      const text = el.querySelector("text");
      const declared = text
        ? Number((text as SVGTextElement).getAttribute("font-size") ?? "11")
        : 0;
      const scale = vb.width > 0 ? rect.width / vb.width : 1;
      return {
        viewBoxW: vb.width,
        renderedW: rect.width,
        declared,
        effective: declared * scale,
      };
    });
    expect(measured.effective).toBeGreaterThanOrEqual(12);

    // FARM-UI.9 P1.1 sub-nav shows Cost as the active tab.
    await expect(page.getByTestId("farm-subnav-cost")).toHaveAttribute(
      "aria-current",
      "page",
    );

    await page.waitForLoadState("networkidle");
    await page.screenshot({
      path: "e2e/screenshots/farm-mobile-cost.png",
      fullPage: true,
    });
  });

  // FARM-UI.9 P1.2 — breadcrumb segments under /farm/* are translated
  // via the nav namespace instead of rendering the raw URL segment.
  test("breadcrumbs translate farm/plan/cost segments", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/farm/plan");
    const crumbs = page.getByRole("navigation", { name: "Breadcrumb" });
    await expect(crumbs).toBeVisible();
    // "Agent farm" comes from nav.farmSection, "Plan" from nav.farmPlan.
    // Asserting on innerText covers the full breadcrumb trail; the i18n
    // keys must be exactly those translated labels (not the raw URL
    // segments). The raw URL segments would show as the lowercase words
    // "farm" or "plan" standing alone; we sample each crumb-link text
    // to make sure no link reads as the raw segment.
    const text = await crumbs.innerText();
    expect(text).toContain("Agent farm");
    expect(text).toContain("Plan");
    const linkTexts = await crumbs
      .locator("a, span.text-slate-900")
      .allInnerTexts();
    // Each crumb label must be the human label, not a bare URL segment.
    expect(linkTexts).not.toContain("farm");
    expect(linkTexts).not.toContain("plan");
  });

  // FARM-UI.9 P1.5 — tap on a DAG node now surfaces the tooltip in
  // addition to the existing neighborhood-fade interaction.
  //
  // Cytoscape renders into a <canvas>, so its node hit-testing is not
  // reachable through Playwright DOM locators. To exercise the wiring
  // the component publishes the live cytoscape instance on
  // window.__sliceDagCy (test-only seam) and we emit a `tap` event on
  // the first node from inside the page, which is the exact path the
  // production runtime takes when a user taps with a finger on a real
  // device.
  test("plan DAG tooltip surfaces on tap", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/farm/plan");
    await expect(page.getByTestId("slice-dag-canvas")).toBeVisible();
    await expect(
      page.getByTestId("slice-dag-canvas").locator("canvas").first(),
    ).toBeVisible();
    // Wait for the cytoscape instance to be attached.
    await page.waitForFunction(() => {
      const w = window as unknown as { __sliceDagCy?: { nodes(): unknown } };
      return Boolean(w.__sliceDagCy);
    });
    // Emit a tap on the first node from inside the page. The real tap
    // event path on touch devices reaches the same cy.on("tap","node")
    // handler we wired in P1.5.
    await page.evaluate(() => {
      const w = window as unknown as {
        __sliceDagCy?: {
          nodes(): {
            length: number;
            first(): {
              emit: (
                event: string,
                payload?: { renderedPosition?: { x: number; y: number } },
              ) => void;
              renderedPosition: () => { x: number; y: number };
            };
          };
        };
      };
      const cy = w.__sliceDagCy;
      if (!cy) throw new Error("cytoscape instance not attached");
      const nodes = cy.nodes();
      if (nodes.length === 0) throw new Error("no nodes laid out");
      const first = nodes.first();
      const pos = first.renderedPosition();
      first.emit("tap", { renderedPosition: pos });
    });
    await expect(page.getByTestId("slice-dag-tooltip")).toBeVisible();
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
