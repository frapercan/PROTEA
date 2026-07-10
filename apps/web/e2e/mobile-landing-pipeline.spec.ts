// Regression spec for the landing page pipeline section at mobile viewport.
// Bug: the 5-stage pipeline row rendered as a single overflowing horizontal
// flex row on narrow screens, clipping the first card at the left edge and
// causing horizontal page scroll.
//
// Fix (HomeShowcase.tsx): on mobile (< sm = 640px) the stages stack
// vertically (flex-col, full-width cards, arrows rotate 90deg to point
// down). On sm+ the row becomes a horizontal scroll container
// (overflow-x-auto) with no justify-center, preventing center-anchored
// overflow that clips the leftmost card.
//
// This spec runs under the "mobile" Playwright project (390x844px,
// isMobile: true) via the "mobile*.spec.ts" testMatch glob.

import { test, expect } from "@playwright/test";

// Pre-accept the usage-policy modal so it does not block page content.
test.beforeEach(async ({ context }) => {
  await context.addInitScript(() => {
    try {
      localStorage.setItem("protea_policy_accepted_v1", "1");
      localStorage.setItem("protea_policy_accepted_v2", "1");
    } catch {}
  });
});

test("pipeline section has no horizontal page overflow at 390px", async ({ page }) => {
  await page.goto("/en/annotate");
  await page.waitForLoadState("networkidle");

  const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
  const viewportWidth = page.viewportSize()!.width;
  expect(
    bodyScrollWidth,
    `page scrollWidth=${bodyScrollWidth} exceeds viewport=${viewportWidth}`,
  ).toBeLessThanOrEqual(viewportWidth + 2);
});

test("all 5 pipeline stage cards are reachable at 390px", async ({ page }) => {
  await page.goto("/en/annotate");
  await page.waitForLoadState("networkidle");

  // Locate by the stage anchor labels present in the mock payload.
  // Each card has an aria-label of "StageName StageDesc". We check
  // visibility (in-viewport) rather than just existence.
  const sequencesLink = page.getByRole("link", { name: /sequences/i }).first();
  const embeddingsLink = page.getByRole("link", { name: /embeddings/i }).first();

  await expect(sequencesLink).toBeVisible();
  await expect(embeddingsLink).toBeVisible();

  // Scroll to end of page to ensure all pipeline cards rendered.
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await page.waitForTimeout(200);

  // After scrolling all stages should be accessible (not horizontally clipped).
  // We verify by checking that the bounding box x of every pipeline link is >= 0.
  const pipelineLinks = page.locator("section").filter({ hasText: /From sequence input/ }).getByRole("link");
  const count = await pipelineLinks.count();
  expect(count, "expected 5 pipeline stage links").toBe(5);

  for (let idx = 0; idx < count; idx++) {
    const box = await pipelineLinks.nth(idx).boundingBox();
    expect(box, `pipeline card ${idx} has no bounding box`).not.toBeNull();
    expect(
      box!.x,
      `pipeline card ${idx} is clipped at x=${box!.x} (should be >= 0)`,
    ).toBeGreaterThanOrEqual(0);
  }
});

test("desktop pipeline (1280px) is unchanged: horizontal row visible", async ({ page }) => {
  // Switch to desktop viewport within the test to verify desktop layout.
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto("/en/annotate");
  await page.waitForLoadState("networkidle");

  // On desktop the pipeline cards should still be in a row. We verify
  // this by checking that all 5 pipeline links have the same approximate
  // y-coordinate (horizontal row means same row, similar y).
  const pipelineLinks = page.locator("section").filter({ hasText: /From sequence input/ }).getByRole("link");
  const count = await pipelineLinks.count();
  expect(count, "expected 5 pipeline stage links on desktop").toBe(5);

  const yCoords: number[] = [];
  for (let idx = 0; idx < count; idx++) {
    const box = await pipelineLinks.nth(idx).boundingBox();
    expect(box).not.toBeNull();
    yCoords.push(box!.y);
  }

  // All cards should have roughly the same y (within 20px tolerance for
  // minor height differences). A vertical stack would spread them 100px+ apart.
  const minY = Math.min(...yCoords);
  const maxY = Math.max(...yCoords);
  expect(
    maxY - minY,
    `pipeline cards spread over ${maxY - minY}px vertically — expected horizontal row on desktop`,
  ).toBeLessThan(20);

  // No horizontal overflow on desktop either.
  const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
  const viewportWidth = page.viewportSize()!.width;
  expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 2);
});

test("screenshot — mobile landing pipeline", async ({ page }) => {
  await page.goto("/en/annotate");
  await page.waitForLoadState("networkidle");
  await page.screenshot({
    path: "e2e/screenshots/mobile-landing-pipeline.png",
    fullPage: true,
  });
});
