// FARM-UI.6: CommandPalette "farm:" namespace e2e.
//
// The spec exercises the four verbs added to the global palette:
//   farm: spawn next pickable    -> POST /spawn   {next_pickable: true}
//   farm: kill <task_id>         -> POST /tasks/<id>/kill
//   farm: cleanup --apply        -> POST /cleanup {apply: true}
//   farm: view <task_id>         -> client navigation only
//
// Auth gating: write verbs are disabled with an "auth required" tooltip
// unless the AuthChip is in bearer / apiKey mode. We simulate signed-in
// state by setting localStorage["protea.bearer"] before navigation, the
// same shape AuthChip.readMode() reads.
//
// The farm-api sidecar is mocked via page.route(); a successful spawn
// also primes the /tasks list so the post-spawn navigation lands on a
// detail page that has at least the task row visible.

import { test, expect, type Route, type Page } from "@playwright/test";

const FARM_API_ORIGIN =
  process.env.NEXT_PUBLIC_FARM_API_URL?.replace(/\/+$/, "") ?? "http://localhost:8801";

type FarmTaskFixture = {
  id: string;
  agent_name: string;
  kind: string;
  persistent: number;
  status: string;
  spawn_args: string;
  worktree: string | null;
  tmux_window: string | null;
  pid: number | null;
  model: string;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
  exit_code: number | null;
  worktree_owner_repo: string;
};

const SPAWNED_TASK: FarmTaskFixture = {
  id: "executor-1779999999-aaaa",
  agent_name: "executor",
  kind: "subagent",
  persistent: 0,
  status: "running",
  spawn_args: '{"task":"FARM-UI.6"}',
  worktree: "/home/test/worktrees/executor-1779999999-aaaa",
  tmux_window: null,
  pid: null,
  model: "opus",
  created_at: "2026-05-18T12:00:00Z",
  started_at: "2026-05-18T12:00:01Z",
  ended_at: null,
  exit_code: null,
  worktree_owner_repo: "frapercan/agent-farm",
};

const EXISTING_TASKS: FarmTaskFixture[] = [
  {
    id: "executor-1779000000-bbbb",
    agent_name: "executor",
    kind: "subagent",
    persistent: 0,
    status: "succeeded",
    spawn_args: "{}",
    worktree: null,
    tmux_window: null,
    pid: null,
    model: "opus",
    created_at: "2026-05-17T12:00:00Z",
    started_at: "2026-05-17T12:00:01Z",
    ended_at: "2026-05-17T12:10:00Z",
    exit_code: 0,
    worktree_owner_repo: "frapercan/PROTEA",
  },
];

type SpawnPayload = { next_pickable?: boolean; agent?: string; task?: string };

async function mockFarmApi(
  page: Page,
  opts?: {
    spawnHandler?: (body: SpawnPayload) => unknown;
    killHandler?: (taskId: string) => unknown;
    cleanupHandler?: (body: { apply?: boolean }) => unknown;
    extraTasks?: FarmTaskFixture[];
  },
) {
  // Tasks list: starts with EXISTING_TASKS; the spawn handler prepends
  // SPAWNED_TASK so the "task row appears" assertion has something
  // observable post-spawn.
  let tasks: FarmTaskFixture[] = [
    ...EXISTING_TASKS,
    ...(opts?.extraTasks ?? []),
  ];

  // Order matters: more specific patterns first so kill / heartbeats /
  // results never get swallowed by the catch-all /tasks/* handler.
  await page.route(/\/tasks\/[^/]+\/kill$/, (route: Route) => {
    const url = new URL(route.request().url());
    const tid = decodeURIComponent(url.pathname.split("/")[2]);
    const out = opts?.killHandler?.(tid) ?? {
      task_id: tid,
      killed: true,
      stdout: `killed: ${tid}\n`,
    };
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(out),
    });
  });
  await page.route(/\/tasks\/[^/]+\/heartbeats(\?.*)?$/, (route: Route) => {
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: "[]",
    });
  });
  await page.route(/\/tasks\/[^/]+\/results$/, (route: Route) => {
    return route.fulfill({ status: 404, body: "{}" });
  });
  await page.route(/\/tasks\/[^/]+$/, (route: Route) => {
    const url = new URL(route.request().url());
    const id = decodeURIComponent(url.pathname.split("/").pop() ?? "");
    const found = tasks.find((t) => t.id === id);
    if (!found) {
      return route.fulfill({ status: 404, body: "{}" });
    }
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(found),
    });
  });
  await page.route(/\/tasks(\?.*)?$/, (route: Route) => {
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(tasks),
    });
  });
  await page.route(`${FARM_API_ORIGIN}/spawn`, async (route: Route) => {
    const body = (await route.request().postDataJSON()) as SpawnPayload;
    const out = opts?.spawnHandler?.(body) ?? {
      task_id: SPAWNED_TASK.id,
      worktree: SPAWNED_TASK.worktree,
      repo: "/home/test/agent-farm",
      model: "opus",
      composed_prompt: null,
      results_dir: null,
      slice_id: body.next_pickable ? "FARM-UI.99" : null,
      loop: body.next_pickable ? "farm-platform" : null,
      agent: body.agent ?? "executor",
    };
    // Make the spawned task visible in subsequent /tasks list calls.
    tasks = [SPAWNED_TASK, ...tasks];
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(out),
    });
  });
  await page.route(`${FARM_API_ORIGIN}/cleanup`, async (route: Route) => {
    const body = (await route.request().postDataJSON()) as { apply?: boolean };
    const out = opts?.cleanupHandler?.(body) ?? {
      apply: body.apply ?? false,
      stdout: `cleanup pass (apply=${body.apply ? 1 : 0})\n`,
    };
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(out),
    });
  });
}

async function signInWithBearer(context: import("@playwright/test").BrowserContext) {
  // AuthChip + readFarmToken both read protea.bearer first; > 16 chars
  // is the threshold that lifts the value out of anonymous mode.
  await context.addInitScript(() => {
    try {
      window.localStorage.setItem("protea.bearer", "test-bearer-token-1234567890abcdef");
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore
    }
  });
}

async function signOut(context: import("@playwright/test").BrowserContext) {
  await context.addInitScript(() => {
    try {
      window.localStorage.removeItem("protea.bearer");
      window.localStorage.removeItem("protea.apiKey");
      window.localStorage.setItem("protea_policy_accepted_v1", "1");
    } catch {
      // ignore
    }
  });
}

async function openPaletteAndType(page: Page, query: string) {
  // The trigger button is hidden below the sm breakpoint; dispatch the
  // same window event the trigger fires so the test works at every
  // viewport without relying on header layout.
  await page.evaluate(() => {
    window.dispatchEvent(new CustomEvent("protea:cmdk:toggle"));
  });
  const input = page.getByRole("dialog", { name: "Command palette" }).getByRole("textbox");
  await expect(input).toBeVisible();
  await input.fill(query);
  return input;
}

test.describe("farm palette (authed user)", () => {
  test.beforeEach(async ({ context }) => {
    await signInWithBearer(context);
  });

  test("spawn next pickable inserts a task row and navigates", async ({
    page,
  }) => {
    await mockFarmApi(page);

    await page.goto("/en/farm");
    // Baseline: the desktop list table shows the EXISTING_TASKS row(s)
    // but not the about-to-spawn task id. We scope to the desktop
    // container because the farm project runs at 1280x800 where the
    // mobile card list is hidden by `lg:hidden`.
    const desktop = page.getByTestId("farm-list-desktop");
    await expect(desktop.getByTestId("farm-row").first()).toBeVisible();
    await expect(desktop.getByText(SPAWNED_TASK.id)).toHaveCount(0);

    await openPaletteAndType(page, "farm: spawn");
    const spawn = page.getByTestId("cmdk-farm-spawn-next");
    await expect(spawn).toBeVisible();
    // Authed: aria-disabled is omitted entirely (React drops the attr
    // when the value is undefined).
    await expect(spawn).not.toHaveAttribute("aria-disabled", "true");
    await spawn.click();

    // The palette navigates to the detail page; the heading uses the
    // task id which is what we just minted.
    await expect(page).toHaveURL(new RegExp(`/en/farm/${SPAWNED_TASK.id}/?$`));

    // Bounce back to the list and assert the spawned task is visible.
    await page.goto("/en/farm");
    await expect(
      page.getByTestId("farm-list-desktop").getByText(SPAWNED_TASK.id),
    ).toBeVisible();
  });

  test("kill <task_id> POSTs to /tasks/<id>/kill", async ({ page }) => {
    let killedId: string | null = null;
    await mockFarmApi(page, {
      killHandler: (tid: string) => {
        killedId = tid;
        return { task_id: tid, killed: true, stdout: `killed: ${tid}\n` };
      },
    });

    await page.goto("/en/farm");
    await openPaletteAndType(page, `farm: kill ${EXISTING_TASKS[0].id}`);
    const kill = page.getByTestId("cmdk-farm-kill");
    await expect(kill).toBeVisible();
    await kill.click();
    await expect.poll(() => killedId).toBe(EXISTING_TASKS[0].id);
  });

  test("cleanup posts apply=true", async ({ page }) => {
    let receivedApply: boolean | null = null;
    await mockFarmApi(page, {
      cleanupHandler: (body) => {
        receivedApply = body.apply ?? false;
        return { apply: receivedApply, stdout: "cleanup pass (apply=1)\n" };
      },
    });

    await page.goto("/en/farm");
    await openPaletteAndType(page, "farm: cleanup");
    const cleanup = page.getByTestId("cmdk-farm-cleanup");
    await expect(cleanup).toBeVisible();
    await cleanup.click();
    await expect.poll(() => receivedApply).toBe(true);
  });

  test("view <task_id> navigates without hitting a write endpoint", async ({
    page,
  }) => {
    let spawnHits = 0;
    await mockFarmApi(page, {
      spawnHandler: () => {
        spawnHits++;
        return { task_id: "should-not-spawn", agent: "executor" };
      },
    });

    await page.goto("/en/farm");
    await openPaletteAndType(page, `farm: view ${EXISTING_TASKS[0].id}`);
    const view = page.getByTestId("cmdk-farm-view");
    await expect(view).toBeVisible();
    await view.click();
    await expect(page).toHaveURL(
      new RegExp(`/en/farm/${EXISTING_TASKS[0].id}/?$`),
    );
    expect(spawnHits).toBe(0);
  });
});

test.describe("farm palette (anonymous user)", () => {
  test.beforeEach(async ({ context }) => {
    await signOut(context);
  });

  test("write verbs are disabled with auth tooltip", async ({ page }) => {
    let spawnHits = 0;
    await mockFarmApi(page, {
      spawnHandler: () => {
        spawnHits++;
        return { task_id: "leaked", agent: "executor" };
      },
    });

    await page.goto("/en/farm");
    await openPaletteAndType(page, "farm: spawn");
    const spawn = page.getByTestId("cmdk-farm-spawn-next");
    await expect(spawn).toBeVisible();
    // Disabled affordance: aria-disabled true + auth chip text.
    await expect(spawn).toHaveAttribute("aria-disabled", "true");
    await expect(spawn).toHaveText(/farm: spawn next pickable/);
    await expect(spawn).toHaveAttribute("title", /sign in/i);

    // Clicking surfaces the error toast but never hits the sidecar.
    // force=true bypasses Playwright's aria-disabled actionability gate
    // (the gate is itself proof the verb is disabled, but we still
    // want to assert the click handler refuses to spawn).
    await spawn.click({ force: true });
    expect(spawnHits).toBe(0);
  });
});
