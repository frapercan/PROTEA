// FARM-UI.5 mobile (390x844) smoke for the cost-rollup dashboard at
// /[locale]/farm/cost. Runs under the existing "mobile" Playwright
// project (viewport 390x844, see playwright.config.ts).
//
// The farm-api sidecar is mocked through page.route() so the spec is
// hermetic: it never opens the real sqlite under ~/Thesis2/agent-farm/
// state, and it deterministically seeds /cost and /agents/budgets.
//
// The spec verifies three things:
//   - by-agent view renders one bar per seeded agent, with the budget
//     overlay (cap line) visible when a budget is present
//   - the view tabs switch the chart between agent / model / day
//     without forcing a re-fetch (one cost+budget call total)
//   - over-budget agents are flagged (red bar + "over cap" label)

import { test, expect, type Route } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ??
  "http://localhost:8801";

// Anchor the fixture to "today" UTC so dateRange() in the page lines up
// regardless of when the suite runs.
const TODAY = new Date().toISOString().slice(0, 10);
const DAY_MINUS = (n: number) => {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n);
  return d.toISOString().slice(0, 10);
};

const COST_BUCKETS = [
  // executor spends $4/day on average over the last 4 days; cap is
  // $12/day so it stays well within budget.
  { date: TODAY, agent: "executor", model: "opus-4.7", tasks: 2, input_tokens: 2000, output_tokens: 1000, cache_tokens: 0, usd: 5.0 },
  { date: DAY_MINUS(1), agent: "executor", model: "opus-4.7", tasks: 1, input_tokens: 1000, output_tokens: 500, cache_tokens: 0, usd: 3.0 },
  { date: DAY_MINUS(2), agent: "executor", model: "sonnet-4.7", tasks: 1, input_tokens: 1000, output_tokens: 500, cache_tokens: 0, usd: 4.0 },
  { date: DAY_MINUS(3), agent: "executor", model: "opus-4.7", tasks: 1, input_tokens: 1000, output_tokens: 500, cache_tokens: 0, usd: 4.0 },
  // janitor spends $5/day over the same window; cap is $2/day so it
  // is reported as over-budget by the page.
  { date: TODAY, agent: "janitor", model: "haiku-4.5", tasks: 3, input_tokens: 6000, output_tokens: 1500, cache_tokens: 0, usd: 8.0 },
  { date: DAY_MINUS(1), agent: "janitor", model: "haiku-4.5", tasks: 1, input_tokens: 2000, output_tokens: 500, cache_tokens: 0, usd: 4.0 },
  { date: DAY_MINUS(2), agent: "janitor", model: "haiku-4.5", tasks: 1, input_tokens: 2000, output_tokens: 500, cache_tokens: 0, usd: 4.0 },
  { date: DAY_MINUS(3), agent: "janitor", model: "haiku-4.5", tasks: 1, input_tokens: 2000, output_tokens: 500, cache_tokens: 0, usd: 4.0 },
];

const BUDGETS = [
  { name: "executor", kind: "subagent", model: "opus-4.7", expected_tokens: 120000, max_usd_per_day: 12.0 },
  { name: "janitor", kind: "subagent", model: "haiku-4.5", expected_tokens: 30000, max_usd_per_day: 2.0 },
  // A zero-spend agent with a cap; should still appear in the bar
  // chart with a zero-length bar so the operator can see they are
  // configured but idle.
  { name: "shepherd", kind: "subagent", model: "haiku-4.5", expected_tokens: 20000, max_usd_per_day: 1.5 },
];

type FixtureCounters = { cost: number; budgets: number };

// Pre-accept the usage-policy modal so the overlay does not block clicks
// on the view / window controls. Mirrors the bypass used by the other
// farm-namespace specs.
async function bypassUsagePolicy(page: import("@playwright/test").Page) {
  await page.addInitScript(() => {
    try {
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // localStorage disabled in some test contexts; the spec will fail
      // loudly downstream if so.
    }
  });
}

async function mockFarmApi(
  page: import("@playwright/test").Page,
  counters: FixtureCounters,
) {
  await page.route(`${FARM_API_ORIGIN}/cost*`, (route: Route) => {
    counters.cost += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(COST_BUCKETS),
    });
  });
  await page.route(`${FARM_API_ORIGIN}/agents/budgets*`, (route: Route) => {
    counters.budgets += 1;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(BUDGETS),
    });
  });
  // Other farm-api endpoints (FarmStatusPill polls /tasks for the chrome
  // pill) are stubbed with empty arrays so the spec stays hermetic.
  await page.route(`${FARM_API_ORIGIN}/tasks*`, (route: Route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: "[]",
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

test.describe("FARM-UI.5 mobile cost page", () => {
  test("renders bar chart with budget overlay and over-budget flag", async ({ page }) => {
    const counters: FixtureCounters = { cost: 0, budgets: 0 };
    await bypassUsagePolicy(page);
    await mockFarmApi(page, counters);

    await page.goto("/en/farm/cost");

    // Summary cards appear before the chart so the operator gets a
    // window-total at a glance regardless of which view is active.
    await expect(page.getByTestId("cost-summary-total")).toBeVisible();
    await expect(page.getByTestId("cost-summary-tasks")).toBeVisible();

    // Default view is by-agent. One bar per seeded agent (zero-spend
    // shepherd still appears because it has a budget).
    await expect(page.getByTestId("cost-chart-agent")).toBeVisible();
    await expect(page.getByTestId("cost-bar-executor")).toBeVisible();
    await expect(page.getByTestId("cost-bar-janitor")).toBeVisible();
    await expect(page.getByTestId("cost-bar-shepherd")).toBeVisible();

    // Budget overlay is drawn for every agent that has a cap > 0.
    await expect(page.getByTestId("cost-budget-line-executor")).toBeAttached();
    await expect(page.getByTestId("cost-budget-line-janitor")).toBeAttached();

    // janitor's $5/day daily-average over the 7-day default window
    // exceeds its $2/day cap, so the row carries the "over cap" label.
    const janitorBar = page.getByTestId("cost-bar-janitor");
    await expect(janitorBar).toContainText("over cap");
    // executor stays within budget so it should NOT carry the flag.
    const executorBar = page.getByTestId("cost-bar-executor");
    await expect(executorBar).not.toContainText("over cap");

    // One initial fetch of each endpoint, no per-view refetch.
    expect(counters.cost).toBe(1);
    expect(counters.budgets).toBe(1);
  });

  test("view tabs switch the chart without refetching", async ({ page }) => {
    const counters: FixtureCounters = { cost: 0, budgets: 0 };
    await bypassUsagePolicy(page);
    await mockFarmApi(page, counters);

    await page.goto("/en/farm/cost");
    await expect(page.getByTestId("cost-chart-agent")).toBeVisible();

    await page.getByTestId("cost-view-model").click();
    await expect(page.getByTestId("cost-chart-model")).toBeVisible();
    await expect(page.getByTestId("cost-chart-model-legend")).toBeVisible();

    await page.getByTestId("cost-view-day").click();
    await expect(page.getByTestId("cost-chart-day")).toBeVisible();

    await page.getByTestId("cost-view-agent").click();
    await expect(page.getByTestId("cost-chart-agent")).toBeVisible();

    // Still one fetch each: the page slices the single fetch
    // client-side.
    expect(counters.cost).toBe(1);
    expect(counters.budgets).toBe(1);
  });

  test("window switcher is wired to URL", async ({ page }) => {
    const counters: FixtureCounters = { cost: 0, budgets: 0 };
    await bypassUsagePolicy(page);
    await mockFarmApi(page, counters);

    await page.goto("/en/farm/cost");
    await expect(page.getByTestId("cost-view-agent")).toHaveAttribute("aria-pressed", "true");

    await page.getByTestId("cost-window-30").click();
    await expect(page).toHaveURL(/[?&]days=30(&|$)/);

    // Resetting to default does not leave a stale URL parameter.
    await page.getByTestId("cost-window-7").click();
    await expect(page).not.toHaveURL(/[?&]days=7/);
  });
});
