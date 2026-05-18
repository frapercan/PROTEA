// FARM-UI.4 smoke test for /[locale]/farm/plan/. Runs under the
// existing "mobile" Playwright project (viewport 390x844, per
// playwright.config.ts). The /plan endpoint on the farm-api sidecar is
// mocked through page.route() so the spec stays hermetic.

import { test, expect, type Route } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ??
  "http://localhost:8801";

// Three slices: a chain A -> B -> C (all pending) and an unrelated
// done slice D. The longest pending chain is A -> B -> C, summing
// 1 + 2 + 3 = 6h.
const SAMPLE_SLICES = [
  {
    id: "DEMO.A",
    title: "DEMO.A — chain root",
    loop: "demo-loop",
    phase: "F-DEMO",
    status: "pending",
    deps: [],
    priority: "P0",
    estimated_hours: 1,
    tags: ["demo"],
    requires_human: false,
    acceptance: "Root slice acceptance",
  },
  {
    id: "DEMO.B",
    title: "DEMO.B — chain middle",
    loop: "demo-loop",
    phase: "F-DEMO",
    status: "pending",
    deps: ["DEMO.A"],
    priority: "P1",
    estimated_hours: 2,
    tags: ["demo"],
    requires_human: false,
    acceptance: "Middle slice acceptance",
  },
  {
    id: "DEMO.C",
    title: "DEMO.C — chain leaf",
    loop: "demo-loop",
    phase: "F-DEMO",
    status: "pending",
    deps: ["DEMO.B"],
    priority: "P2",
    estimated_hours: 3,
    tags: ["demo"],
    requires_human: false,
    acceptance: "Leaf slice acceptance",
  },
  {
    id: "DEMO.D",
    title: "DEMO.D — unrelated done",
    loop: "other-loop",
    phase: "F-OTHER",
    status: "done",
    deps: [],
    priority: "P2",
    estimated_hours: 4,
    tags: [],
    requires_human: false,
    acceptance: "Done acceptance",
  },
];

async function mockPlan(page: import("@playwright/test").Page) {
  await page.route(`${FARM_API_ORIGIN}/plan*`, (route: Route) => {
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(SAMPLE_SLICES),
    });
  });
}

test.beforeEach(async ({ context }) => {
  // Match the usage-policy bypass used by farm-list.spec.ts so the
  // modal does not block clicks on first visit.
  await context.addInitScript(() => {
    try {
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore on origins where localStorage is not yet available
    }
  });
});

test("plan page renders the slice DAG canvas at 390x844", async ({ page }) => {
  await mockPlan(page);
  await page.goto("/en/farm/plan");

  await expect(
    page.getByRole("heading", { name: "Slice DAG" }),
  ).toBeVisible();
  // Cytoscape mounts a <canvas> inside the data-testid container.
  const canvas = page.getByTestId("slice-dag-canvas");
  await expect(canvas).toBeVisible();
  await expect(canvas.locator("canvas").first()).toBeVisible();

  // Initial slice count rendered (all four).
  await expect(page.getByText("4 slices shown")).toBeVisible();
});

test("loop filter chip narrows the slice count", async ({ page }) => {
  await mockPlan(page);
  await page.goto("/en/farm/plan");

  await expect(page.getByText("4 slices shown")).toBeVisible();

  // Click the "demo-loop" chip; only the three demo-loop slices remain.
  await page
    .getByTestId("filter-loop-chip-demo-loop")
    .click();
  await expect(page).toHaveURL(/[?&]loop=demo-loop\b/);
  await expect(page.getByText("3 slices shown")).toBeVisible();
});

test("critical-path toggle adds the summary line", async ({ page }) => {
  await mockPlan(page);
  await page.goto("/en/farm/plan");

  const toggle = page.getByTestId("critical-toggle");
  await expect(toggle).toHaveAttribute("aria-pressed", "false");
  await toggle.click();
  await expect(page).toHaveURL(/[?&]critical=1\b/);
  await expect(toggle).toHaveAttribute("aria-pressed", "true");

  // Summary shows the 3-slice / 6h chain.
  const summary = page.getByTestId("critical-summary");
  await expect(summary).toBeVisible();
  await expect(summary).toContainText("3 slices");
  await expect(summary).toContainText("6h");
});
