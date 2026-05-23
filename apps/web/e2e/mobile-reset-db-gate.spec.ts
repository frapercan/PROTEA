import { test, expect } from "@playwright/test";

// Two-part regression for NEXT_PUBLIC_ENABLE_DB_RESET:
//   * default (env unset / "0") => button absent from sidebar footer
//   * env set to "1" via dev-server start => button present
//
// The dev server is started by the harness with NEXT_PUBLIC_ENABLE_DB_RESET=1
// for the second scenario; the first scenario simply asserts the gate path
// is wired by checking the rendered DOM under the current env. We pick the
// scenario via the GATE env passed to the test runner.

test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try { localStorage.setItem("protea_policy_accepted_v1", "1"); } catch {}
  });
});

const expectGateOn = process.env.GATE === "on";

test("reset-db button respects NEXT_PUBLIC_ENABLE_DB_RESET", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/jobs");
  await page.waitForLoadState("networkidle");
  await page.locator('button[aria-controls="protea-mobile-nav"]').click();
  const drawer = page.locator('#protea-mobile-nav[role="dialog"]');
  await expect(drawer).toBeVisible();
  const resetBtn = drawer.getByRole("button", { name: /Reset DB|Reiniciar BD/i });
  if (expectGateOn) {
    await expect(resetBtn).toBeVisible();
  } else {
    await expect(resetBtn).toHaveCount(0);
  }
});
