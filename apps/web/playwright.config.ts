import { defineConfig, devices } from "@playwright/test";

// Playwright projects:
//   mobile  / tablet  : viewport regression (existing)
//   flows             : critical user-flow suite (F6.5). Desktop viewport,
//                       hermetic via per-test page.route() API mocks so
//                       specs do not require the backend bundle. The CI
//                       workflow may later swap baseURL to the bundle.
//
// baseURL prefers PLAYWRIGHT_BASE_URL (CI sets it when pointing at a
// real bundle), falls back to the local dev server on port 3000.

const BASE_URL = process.env.PLAYWRIGHT_BASE_URL ?? "http://localhost:3000";
const MOCK_API_PORT = Number(process.env.MOCK_API_PORT ?? 8000);

export default defineConfig({
  testDir: "./e2e",
  use: {
    baseURL: BASE_URL,
    browserName: "chromium",
    trace: "retain-on-failure",
  },
  workers: process.env.CI ? 1 : undefined,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI
    ? [["list"], ["html", { open: "never" }], ["github"]]
    : "list",
  // Boot the showcase mock so the Next.js server (running in
  // production mode with NEXT_PUBLIC_API_URL=http://localhost:8000)
  // gets a real backend on the other end of every server-side fetch
  // issued by RSC components (e.g. HomeShowcase). page.route() does
  // NOT intercept Node-side fetches; this real socket does.
  // See e2e/mock-server/showcase-server.mjs + e2e/flows/fixtures/
  // showcase-mock.ts for the test-side control endpoints.
  webServer: {
    command: `node ${__dirname}/e2e/mock-server/showcase-server.mjs`,
    port: MOCK_API_PORT,
    reuseExistingServer: !process.env.CI,
    timeout: 10_000,
    env: { MOCK_API_PORT: String(MOCK_API_PORT) },
  },
  projects: [
    {
      name: "mobile",
      use: {
        browserName: "chromium",
        viewport: { width: 390, height: 844 },
        deviceScaleFactor: 3,
        isMobile: true,
        hasTouch: true,
        userAgent: devices["iPhone 12"].userAgent,
      },
      // Match both the legacy "mobile-*.spec.ts" prefix and the FARM-UI.8
      // "farm-mobile.spec.ts" file. The farm-mobile spec lives next to
      // the farm-list / farm-palette specs (which are desktop) so the
      // pattern is the more explicit option rather than a prefix.
      testMatch: ["**/mobile*.spec.ts", "**/farm-mobile.spec.ts"],
    },
    {
      name: "tablet",
      use: {
        browserName: "chromium",
        viewport: { width: 768, height: 1024 },
        deviceScaleFactor: 2,
        isMobile: true,
        hasTouch: true,
      },
      // Same dual-pattern: legacy tablet-*.spec.ts plus the FARM-UI.8
      // farm-tablet.spec.ts.
      testMatch: ["**/tablet*.spec.ts", "**/farm-tablet.spec.ts"],
    },
    {
      name: "flows",
      use: {
        browserName: "chromium",
        viewport: { width: 1280, height: 800 },
      },
      testMatch: "**/flows/**/*.spec.ts",
    },
    {
      // FARM-UI.2 smoke. The spec ships its own per-describe viewport
      // override (375x667 + 1280x800) so the project-level viewport
      // here is only the bootstrap default. Excludes farm-mobile /
      // farm-tablet which run under the dedicated viewport projects.
      name: "farm",
      use: {
        browserName: "chromium",
        viewport: { width: 1280, height: 800 },
      },
      testMatch: "**/farm-*.spec.ts",
      testIgnore: ["**/farm-mobile.spec.ts", "**/farm-tablet.spec.ts"],
    },
  ],
});
