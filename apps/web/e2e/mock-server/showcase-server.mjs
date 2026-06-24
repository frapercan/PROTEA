// Tiny stateful HTTP mock for the PROTEA API surface that the homepage
// RSC reaches at render time. Playwright `page.route()` cannot
// intercept fetches that originate inside the Next.js server (the
// HomeShowcase server component calls `getShowcase()` via `fetch` from
// the Node runtime, not from the browser), so we need a real socket
// listener on the URL `NEXT_PUBLIC_API_URL` resolves to.
//
// Surface:
//   GET  /showcase/                  -> current seed (default payload)
//   POST /__mock__/showcase          -> swap the seed { status, body }
//   POST /__mock__/reset             -> restore defaults
//   anything else                    -> 200 with `[]`
//
// The control endpoints are namespaced under /__mock__ so they cannot
// collide with real PROTEA routes. The fixture writes to them over
// loopback before each test navigates.

import http from "node:http";

const PORT = Number(process.env.MOCK_API_PORT ?? 8000);

const DEFAULT_SHOWCASE = {
  protein_stats: { total: 12345, canonical: 9876 },
  best: {
    evaluation_result_id: "er-fixture-001",
    evaluation_set_id: "es-fixture-001",
    stage: "reranker",
    avg_primary: 0.612,
    primary_metric: "f_micro_w",
    embedding: {
      id: "emb-esm2-fixture",
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
    { category: "NK", aspect: "MFO", metric: "f_micro_w", mean: 0.612, ci95: 0.04, max: 0.71, min: 0.49, n_models: 8 },
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
    { name: "predictions", count: 250_000, href: "/functional-annotation" },
    { name: "reranker_models", count: 3, href: "/reranker" },
    { name: "evaluations", count: 12, href: "/evaluation" },
  ],
};

let showcaseStatus = 200;
let showcaseBody = DEFAULT_SHOWCASE;

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const raw = Buffer.concat(chunks).toString("utf8");
      if (!raw) return resolve({});
      try {
        resolve(JSON.parse(raw));
      } catch {
        resolve({});
      }
    });
  });
}

function send(res, status, body) {
  res.writeHead(status, {
    "content-type": "application/json",
    "access-control-allow-origin": "*",
  });
  res.end(typeof body === "string" ? body : JSON.stringify(body));
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);
  const path = url.pathname.replace(/\/+$/, "") || "/";

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,PUT,DELETE,OPTIONS",
      "access-control-allow-headers": "*",
    });
    return res.end();
  }

  // Control plane.
  if (path === "/__mock__/showcase" && req.method === "POST") {
    const body = await readBody(req);
    showcaseStatus = typeof body.status === "number" ? body.status : 200;
    showcaseBody = body.body !== undefined ? body.body : DEFAULT_SHOWCASE;
    return send(res, 200, { ok: true });
  }
  if (path === "/__mock__/reset" && req.method === "POST") {
    showcaseStatus = 200;
    showcaseBody = DEFAULT_SHOWCASE;
    return send(res, 200, { ok: true });
  }
  if (path === "/__mock__/health" && req.method === "GET") {
    return send(res, 200, { ok: true });
  }

  // Data plane.
  if (path === "/showcase" && req.method === "GET") {
    return send(res, showcaseStatus, showcaseBody);
  }

  // Catch-all: respond OK with an empty list so background polls do
  // not flake other flows that share the mock origin.
  return send(res, 200, []);
});

server.listen(PORT, "127.0.0.1", () => {
  // eslint-disable-next-line no-console
  console.log(`[mock-api] listening on http://127.0.0.1:${PORT}`);
});

function shutdown() {
  server.close(() => process.exit(0));
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
