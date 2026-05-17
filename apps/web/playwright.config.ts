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
      testMatch: "**/mobile*.spec.ts",
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
      testMatch: "**/tablet*.spec.ts",
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
      // here is only the bootstrap default.
      name: "farm",
      use: {
        browserName: "chromium",
        viewport: { width: 1280, height: 800 },
      },
      testMatch: "**/farm-*.spec.ts",
    },
  ],
});
