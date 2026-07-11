// Hermetic Playwright fixture that mocks the PROTEA API surface used by
// the critical landing + jobs flows. Each test extends `test` from here
// and gets a `mockApi` handle on the page (already wired). Tests can
// override individual endpoints by calling `mockApi.override(path, body)`
// before navigating, which keeps the default catalog small and lets each
// spec express only the divergences it needs.
//
// Why mock instead of pointing at the bundle:
//   - F6.5a is scaffolding. We want fast, deterministic specs that
//     prove the navigation surface works without booting postgres /
//     rabbitmq / a worker every CI run.
//   - F6.5b (annotate submission + polling) WILL need the bundle and
//     a real worker. That slice can flip baseURL to the live stack.

import { test as base, expect, type Page, type Route } from "@playwright/test";

type Json = Record<string, unknown> | unknown[];

// Context passed to a function-body override so callers can vary the
// response per call. F6.5b uses this for the embedding/prediction
// polling lifecycle (call 1 returns "running", call 2 returns
// "succeeded") without leaking state into the fixture defaults.
export interface OverrideContext {
  call: number;
  method: string;
  url: URL;
}
export type OverrideBody = Json | ((ctx: OverrideContext) => Json | { body: Json; status?: number });

// The web app reads NEXT_PUBLIC_API_URL at page-load time and issues
// raw fetch() calls to that origin (see lib/api.ts). Route matchers
// must be origin-scoped so they do not accidentally intercept the
// Next.js page routes (/en/instrument/jobs/, /en/instrument/jobs/[id]/) which share suffix
// shapes with API paths.
//
// We accept both the dev origin (localhost:8000) and the prod-style
// /api-proxy rewrite path (see next.config.ts).
const API_ORIGIN = process.env.NEXT_PUBLIC_API_URL?.replace(/\/+$/, "") ?? "http://localhost:8000";

export interface MockApi {
  override(suffix: string, body: OverrideBody, status?: number): void;
  page: Page;
}

const DEFAULT_SHOWCASE = {
  protein_stats: { total: 12345, canonical: 9876 },
  best: {
    evaluation_result_id: "er-1",
    evaluation_set_id: "es-1",
    stage: "reranker",
    avg_primary: 0.612,
    primary_metric: "f_micro_w",
    embedding: {
      id: "emb-esm2",
      model_name: "facebook/esm2_t33_650M_UR50D",
      model_backend: "esm2",
      family: "ESM2",
      display_name: "ESM2 650M",
      param_count: 650_000_000,
    },
    per_cell: [
      { category: "NK", aspect: "MFO", primary: 0.71, primary_metric: "f_micro_w", f_micro_w: 0.71, fmax: 0.69, precision: 0.7, recall: 0.72 },
      { category: "NK", aspect: "BPO", primary: 0.55, primary_metric: "f_micro_w", f_micro_w: 0.55, fmax: 0.53, precision: 0.5, recall: 0.6 },
      { category: "NK", aspect: "CCO", primary: 0.60, primary_metric: "f_micro_w", f_micro_w: 0.60, fmax: 0.58, precision: 0.58, recall: 0.62 },
    ],
  },
  primary_metric: "f_micro_w",
  per_task: [
    { category: "NK", aspect: "MFO", metric: "f_micro_w", mean: 0.58, ci95: 0.04, max: 0.71, min: 0.49, n_models: 8 },
    { category: "NK", aspect: "BPO", metric: "f_micro_w", mean: 0.46, ci95: 0.05, max: 0.55, min: 0.38, n_models: 8 },
    { category: "NK", aspect: "CCO", metric: "f_micro_w", mean: 0.51, ci95: 0.03, max: 0.60, min: 0.44, n_models: 8 },
  ],
  counts: {
    proteins: 12345,
    sequences: 12000,
    embeddings: 9000,
    prediction_sets: 14,
    predictions: 250_000,
    reranker_models: 3,
    evaluations: 12,
  },
  pipeline_stages: [
    { name: "sequences", count: 12000, href: "/proteins" },
    { name: "embeddings", count: 9000, href: "/embeddings" },
    { name: "predictions", count: 250000, href: "/functional-annotation" },
    { name: "reranker_models", count: 3, href: "/reranker" },
    { name: "evaluations", count: 12, href: "/evaluation" },
  ],
};

const DEFAULT_JOBS = [
  {
    id: "job-aaa-001",
    operation: "compute_embeddings",
    operation_description: "Compute ESM2 embeddings for the canonical proteome",
    operation_summary: "esm2_t33_650M_UR50D | 9000 proteins",
    queue_name: "protea.embeddings",
    status: "running",
    parent_job_id: null,
    created_at: "2026-05-15T10:00:00Z",
    started_at: "2026-05-15T10:01:00Z",
    finished_at: null,
    progress_current: 4500,
    progress_total: 9000,
    error_code: null,
    error_message: null,
  },
  {
    id: "job-bbb-002",
    operation: "predict_go_terms",
    operation_description: "Run KNN prediction on query set qs-42",
    operation_summary: "qs-42 | esm2_t33_650M_UR50D",
    queue_name: "protea.predictions",
    status: "queued",
    parent_job_id: null,
    created_at: "2026-05-15T10:15:00Z",
    started_at: null,
    finished_at: null,
    progress_current: null,
    progress_total: null,
    error_code: null,
    error_message: null,
  },
  {
    id: "job-ccc-003",
    operation: "load_quickgo_annotations",
    operation_description: "Refresh GO annotations from QuickGO",
    operation_summary: "swissprot batch",
    queue_name: "protea.annotations",
    status: "succeeded",
    parent_job_id: null,
    created_at: "2026-05-14T09:00:00Z",
    started_at: "2026-05-14T09:01:00Z",
    finished_at: "2026-05-14T09:14:00Z",
    progress_current: 1000,
    progress_total: 1000,
    error_code: null,
    error_message: null,
  },
  {
    id: "job-ddd-004",
    operation: "compute_embeddings",
    operation_description: "Backfill prot_t5 embeddings",
    operation_summary: "prot_t5_xl_uniref50 | 500 proteins",
    queue_name: "protea.embeddings",
    status: "failed",
    parent_job_id: null,
    created_at: "2026-05-13T08:00:00Z",
    started_at: "2026-05-13T08:01:00Z",
    finished_at: "2026-05-13T08:03:30Z",
    progress_current: 120,
    progress_total: 500,
    error_code: "EMBEDDING_OOM",
    error_message: "CUDA out of memory on shard 2",
  },
];

const DEFAULT_JOB_DETAIL = {
  ...DEFAULT_JOBS[0],
  payload: { embedding_config_id: "emb-esm2", batch_size: 32 },
};

// Truthful GPU-availability signal consumed by the AnnotateForm banner.
// Default mirrors DEFAULT_JOBS: a live compute_embeddings run + a queued
// predict, so the queue-busy banner renders by default. Tests that need a
// free pipeline override "/jobs/gpu-availability" with { busy: false }.
const DEFAULT_GPU_AVAILABILITY = {
  busy: true,
  running_fresh: 1,
  queued: 1,
  running_stale: 0,
  active_operation: "compute_embeddings",
  progress_current: 4500,
  progress_total: 9000,
};

const DEFAULT_JOB_EVENTS = [
  {
    id: 1,
    ts: "2026-05-15T10:01:00Z",
    level: "info",
    event: "job_started",
    message: "worker picked up job",
    fields: { worker_id: "worker-1" },
  },
  {
    id: 2,
    ts: "2026-05-15T10:02:30Z",
    level: "info",
    event: "progress",
    message: "shard 1 of 2 complete",
    fields: { shard: 1 },
  },
];

export const test = base.extend<{ mockApi: MockApi }>({
  // auto: true so every test inheriting this `test` gets the modal
  // pre-acceptance + API route mocks applied without having to add
  // `mockApi` to its arg list. Tests that need to customise responses
  // can still destructure `mockApi` and call `override(...)`.
  mockApi: [async ({ page, context }, run) => {
    // Pre-accept the usage-policy modal so it does not gate the page on
    // first visit. addInitScript runs in every fresh Document before the
    // app scripts execute, which is enough for the modal's useEffect
    // (which only opens when the storage key is missing).
    //
    // Also inject a test session JWT so mutations (POSTs to maintenance,
    // etc.) that require auth do not fail. The JWT payload is cosmetic
    // in the mock fixtures; the backend rejects the signature anyway.
    // We use a far-future expiry (year 2050) so it never expires during tests.
    const testJwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJleHAiOjI1MjQ2MDgwMDAsImlhdCI6MTcxNjU0MTUwMCwicm9sZSI6ImFkbWluIn0.test";
    // Set the session cookie at the context level BEFORE any navigation
    // so the middleware sees it on the very first request. addInitScript
    // runs inside the page after navigation starts; by then the middleware
    // has already redirected role-gated routes (/maintenance, /admin/*).
    // Covers both localhost and 127.0.0.1 because CI and local dev use
    // each interchangeably depending on the runner.
    await context.addCookies([
      { name: "protea_session", value: testJwt, domain: "localhost", path: "/" },
      { name: "protea_session", value: testJwt, domain: "127.0.0.1", path: "/" },
    ]);
    await context.addInitScript(({ jwt }: { jwt: string }) => {
      try {
        window.localStorage.setItem("protea_policy_accepted_v1", "1");
        window.localStorage.setItem("protea_policy_accepted_v2", "1");
        // Mirror into document.cookie in case any client-side code reads it
        // before the context cookie shows up via document.cookie API.
        document.cookie = `protea_session=${jwt}; path=/`;
      } catch {
        // ignore on origins where localStorage is not yet available
      }
    }, { jwt: testJwt });

    const overrides = new Map<string, { body: OverrideBody; status: number }>();
    const callCounts = new Map<string, number>();

    const handle = async (route: Route) => {
      const url = new URL(route.request().url());
      const path = url.pathname.replace(/^\/api-proxy/, "");

      // Caller overrides win first. Match by suffix so callers can pass
      // either "/jobs" or the fully-qualified path.
      for (const [suffix, payload] of overrides) {
        if (path === suffix || path.endsWith(suffix)) {
          const nextCall = (callCounts.get(suffix) ?? 0) + 1;
          callCounts.set(suffix, nextCall);
          let body: Json;
          let status = payload.status;
          if (typeof payload.body === "function") {
            const result = payload.body({
              call: nextCall,
              method: route.request().method(),
              url,
            });
            if (
              result &&
              typeof result === "object" &&
              !Array.isArray(result) &&
              "body" in (result as Record<string, unknown>)
            ) {
              const wrapped = result as { body: Json; status?: number };
              body = wrapped.body;
              if (typeof wrapped.status === "number") status = wrapped.status;
            } else {
              body = result as Json;
            }
          } else {
            body = payload.body;
          }
          return route.fulfill({
            status,
            contentType: "application/json",
            body: JSON.stringify(body),
          });
        }
      }

      if (path.match(/\/showcase\/?$/)) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(DEFAULT_SHOWCASE),
        });
      }

      if (path.match(/\/jobs\/gpu-availability\/?$/)) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(DEFAULT_GPU_AVAILABILITY),
        });
      }

      if (path.match(/\/jobs\/[^/]+\/events/)) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(DEFAULT_JOB_EVENTS),
        });
      }

      if (path.match(/\/jobs\/[^/]+\/?$/) && !path.match(/\/jobs\/?$/)) {
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(DEFAULT_JOB_DETAIL),
        });
      }

      if (path.match(/\/jobs\/?$/)) {
        const status = url.searchParams.get("status");
        const parent = url.searchParams.get("parent_job_id");
        let filtered = DEFAULT_JOBS;
        if (parent) filtered = [];
        if (status) filtered = filtered.filter((j) => j.status === status);
        return route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(filtered),
        });
      }

      // Anything we did not specifically wire returns an empty list.
      // Keeps tests from hanging on background polls (e.g. AnnotateForm
      // queue probe) when they navigate off-path.
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([]),
      });
    };

    // Scope route mocks to the API origin (or the /api-proxy rewrite)
    // so they do not intercept Next.js page routes that share suffix
    // shapes with API endpoints (e.g. /en/instrument/jobs/ vs /jobs).
    await page.route(`${API_ORIGIN}/**`, handle);
    await page.route("**/api-proxy/**", handle);

    await run({
      page,
      override(suffix, body, status = 200) {
        overrides.set(suffix, { body, status });
      },
    });
  }, { auto: true }],
});

export { expect };
export { DEFAULT_JOBS, DEFAULT_SHOWCASE };
