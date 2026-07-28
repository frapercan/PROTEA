// FARM-UI.2 smoke test for /[locale]/farm/. The spec exercises the list
// page at two viewports (375x667 mobile + 1280x800 desktop) under the
// "flows" Playwright project, which is desktop-only by default. We
// re-use the project for both viewports by overriding viewport per
// describe block via test.use().
//
// The farm-api sidecar is mocked through page.route() so the spec runs
// hermetic: no live FastAPI process required. Routes match the sidecar
// origin (NEXT_PUBLIC_FARM_API_URL, default http://localhost:8801).
//
// Project assignment: this file matches the existing "flows" project
// via its location in apps/web/e2e/ (top-level). The playwright config
// has been extended with a "farm" project so the spec is picked up
// regardless of where the operator runs it from.

import { test, expect, type Route } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ?? "http://localhost:8801";

const SAMPLE_TASKS = [
  {
    id: "executor-1779000000-aaaa",
    agent_name: "executor",
    kind: "executor",
    persistent: 0,
    status: "running",
    spawn_args: '{"task":"FARM-UI.2"}',
    worktree: "/home/user/worktrees/executor-1779000000-aaaa",
    tmux_window: "executor:1",
    pid: 12345,
    model: "claude-opus-4-7",
    created_at: "2026-05-17T10:00:00Z",
    started_at: "2026-05-17T10:00:05Z",
    ended_at: null,
    exit_code: null,
    worktree_owner_repo: "frapercan/agent-farm",
  },
  {
    id: "janitor-1779000001-bbbb",
    agent_name: "janitor",
    kind: "janitor",
    persistent: 0,
    status: "succeeded",
    spawn_args: '{"task":"trailer-sweep"}',
    worktree: "/home/user/worktrees/janitor-1779000001-bbbb",
    tmux_window: "janitor:1",
    pid: null,
    model: "claude-sonnet-4-7",
    created_at: "2026-05-17T09:00:00Z",
    started_at: "2026-05-17T09:00:03Z",
    ended_at: "2026-05-17T09:14:22Z",
    exit_code: 0,
    worktree_owner_repo: "frapercan/PROTEA",
  },
  {
    id: "executor-1779000002-cccc",
    agent_name: "executor",
    kind: "executor",
    persistent: 0,
    status: "killed",
    spawn_args: '{"task":"FARM-1.3"}',
    worktree: null,
    tmux_window: null,
    pid: null,
    model: "claude-opus-4-7",
    created_at: "2026-05-17T08:00:00Z",
    started_at: "2026-05-17T08:00:01Z",
    ended_at: "2026-05-17T08:05:11Z",
    exit_code: 137,
    worktree_owner_repo: "frapercan/agent-farm",
  },
];

async function mockFarmApi(page: import("@playwright/test").Page) {
  // /tasks (list) with optional query string filters.
  await page.route(`${FARM_API_ORIGIN}/tasks*`, (route: Route) => {
    const url = new URL(route.request().url());
    const statusF = url.searchParams.get("status");
    const agentF = url.searchParams.get("agent");
    let rows = SAMPLE_TASKS.slice();
    if (statusF) rows = rows.filter((r) => r.status === statusF);
    if (agentF) rows = rows.filter((r) => r.agent_name === agentF);
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(rows),
    });
  });
}

// Pre-accept the usage-policy modal so the overlay does not block clicks
// on first visit. This matches the bypass used by e2e/flows/fixtures/
// mock-api.ts and runs in every fresh Document before the app scripts
// execute. The modal's useEffect only opens when the storage key is
// missing, so this single addInitScript is sufficient.
test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try {
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore on origins where localStorage is not yet available
    }
  });
});

test.describe("farm list — mobile (375x667)", () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test("renders mobile card list with at least one row", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm");

    await expect(page.getByRole("heading", { name: "Agent farm" })).toBeVisible();
    // Mobile container is visible, desktop hidden.
    await expect(page.getByTestId("farm-list-mobile")).toBeVisible();
    await expect(page.getByTestId("farm-list-desktop")).toBeHidden();

    // Scope to the mobile container; the desktop table also renders its
    // rows in the DOM (hidden via Tailwind's lg:hidden / hidden lg:block
    // pair) so a global query would double-count.
    const mobileRows = page
      .getByTestId("farm-list-mobile")
      .getByTestId("farm-row");
    await expect(mobileRows.first()).toBeVisible();
    await expect(mobileRows).toHaveCount(SAMPLE_TASKS.length);
  });

  test("status chip filters the list and persists in the URL", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm");

    const mobileRows = page
      .getByTestId("farm-list-mobile")
      .getByTestId("farm-row");
    await expect(mobileRows).toHaveCount(SAMPLE_TASKS.length);

    // Click "Running" chip.
    await page.getByRole("button", { name: "Running", exact: true }).click();
    await expect(page).toHaveURL(/[?&]status=running\b/);
    await expect(mobileRows).toHaveCount(1);
  });
});

test.describe("farm list — desktop (1280x800)", () => {
  test.use({ viewport: { width: 1280, height: 800 } });

  test("renders desktop table with at least one row", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm");

    await expect(page.getByRole("heading", { name: "Agent farm" })).toBeVisible();
    await expect(page.getByTestId("farm-list-desktop")).toBeVisible();
    await expect(page.getByTestId("farm-list-mobile")).toBeHidden();

    const desktopRows = page
      .getByTestId("farm-list-desktop")
      .getByTestId("farm-row");
    await expect(desktopRows.first()).toBeVisible();
    await expect(desktopRows).toHaveCount(SAMPLE_TASKS.length);

    // Table headers visible.
    await expect(page.getByText("Task ID", { exact: true })).toBeVisible();
    await expect(
      page.getByTestId("farm-list-desktop").getByText("Agent", { exact: true }),
    ).toBeVisible();
  });

  test("window radio updates the URL parameter", async ({ page }) => {
    await mockFarmApi(page);
    await page.goto("/en/instrument/farm");

    await page.getByRole("button", { name: "Last 7d", exact: true }).click();
    await expect(page).toHaveURL(/[?&]window=7d\b/);
  });
});
