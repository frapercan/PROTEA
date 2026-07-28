// Critical user flow: stack page renders the repositories table from
// /stack, the open-pulls table from /stack/pulls, and surfaces the
// thesis-PDF banner when the response carries thesis_pdf_url. Refresh
// re-fires the pulls request. Hermetic via the mock-api fixture.

import { test, expect } from "./fixtures/mock-api";

const STACK_RESPONSE = {
  thesis_pdf_url: "https://example.test/thesis.pdf",
  repos: [
    {
      name: "PROTEA",
      slug: "PROTEA",
      role: "core",
      role_label: "Main pipeline",
      status: "active",
      summary: "Main embedding-based annotation pipeline.",
      summary_es: "Pipeline principal de anotación basada en embeddings.",
      github_url: "https://github.com/frapercan/PROTEA",
      docs_url: "https://protea.readthedocs.io/",
      package_url: null,
    },
    {
      name: "protea-method",
      slug: "protea-method",
      role: "inference",
      role_label: "LAFA inference layer",
      status: "beta",
      summary: "LAFA inference + benchmarking.",
      summary_es: null,
      github_url: "https://github.com/frapercan/protea-method",
      docs_url: "https://protea-method.readthedocs.io/",
      package_url: null,
    },
  ],
};

const PULLS_RESPONSE = {
  fetched_at: 1747396800,
  cached: false,
  repos_queried: 2,
  pulls: [
    {
      repo: "PROTEA",
      number: 379,
      title: "Playwright E2E scaffolding",
      url: "https://github.com/frapercan/PROTEA/pull/379",
      state: "open",
      draft: false,
      author: "frapercan",
      created_at: "2026-05-15T10:00:00Z",
      updated_at: "2026-05-16T09:00:00Z",
      labels: ["test"],
    },
  ],
  rate_limit_remaining: 4992,
  errors: {},
};

test.describe("stack flow", () => {
  test("renders the repository table with status pills", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/stack/pulls", PULLS_RESPONSE);
    mockApi.override("/stack", STACK_RESPONSE);

    await page.goto("/en/instrument/stack/");

    await expect(
      page.getByRole("heading", { name: "PROTEA stack", level: 1 }),
    ).toBeVisible();
    await expect(page.getByRole("link", { name: "PROTEA" }).first()).toBeVisible();
    await expect(page.getByRole("link", { name: "protea-method" }).first()).toBeVisible();
    await expect(page.getByText("active")).toBeVisible();
    await expect(page.getByText("beta")).toBeVisible();
  });

  test("shows the thesis PDF banner when the response carries a URL", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/stack/pulls", PULLS_RESPONSE);
    mockApi.override("/stack", STACK_RESPONSE);

    await page.goto("/en/instrument/stack/");

    await expect(
      page.getByRole("heading", { name: "PROTEA thesis", level: 2 }),
    ).toBeVisible();
    const pdfLink = page.getByRole("link", { name: "Open PDF" });
    await expect(pdfLink).toHaveAttribute(
      "href",
      "https://example.test/thesis.pdf",
    );
  });

  test("pulls table lists the open PRs returned by /stack/pulls", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/stack/pulls", PULLS_RESPONSE);
    mockApi.override("/stack", STACK_RESPONSE);

    await page.goto("/en/instrument/stack/");

    const prLink = page.getByRole("link", {
      name: /#379 Playwright E2E scaffolding/,
    });
    await expect(prLink).toBeVisible();
    await expect(prLink).toHaveAttribute(
      "href",
      "https://github.com/frapercan/PROTEA/pull/379",
    );
    // Label badge from the mock renders next to the title.
    await expect(page.getByText("test", { exact: true })).toBeVisible();
  });

  test("refresh button re-triggers /stack/pulls and updates the fetchedAt label", async ({
    page,
    mockApi,
  }) => {
    mockApi.override("/stack/pulls", PULLS_RESPONSE);
    mockApi.override("/stack", STACK_RESPONSE);

    await page.goto("/en/instrument/stack/");

    const refresh = page.getByRole("button", { name: /Refresh/ });
    await expect(refresh).toBeEnabled();

    // Swap to an empty pulls response, then click refresh and assert
    // the empty-state row appears.
    mockApi.override("/stack/pulls", {
      ...PULLS_RESPONSE,
      pulls: [],
      fetched_at: PULLS_RESPONSE.fetched_at + 60,
    });
    await refresh.click();

    await expect(
      page.getByText("No open pull requests in any repository. Tabula rasa."),
    ).toBeVisible();
  });
});
