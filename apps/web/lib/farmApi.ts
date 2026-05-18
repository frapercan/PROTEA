// Thin client for the agent-farm FastAPI sidecar (apps/farm-api, shipped
// in FARM-UI.1). The dashboard pages under /[locale]/farm/ call this
// module exclusively, so the rest of the app stays decoupled from the
// sidecar's origin and OpenAPI shape.
//
// The base URL is read from NEXT_PUBLIC_FARM_API_URL with a localhost
// fallback so a `npm run dev` against a running sidecar Just Works. The
// fallback intentionally matches the runbook default (port 8801).

export type FarmTask = {
  id: string;
  agent_name: string;
  kind: string;
  persistent: number;
  status: string;
  spawn_args?: string | null;
  worktree?: string | null;
  tmux_window?: string | null;
  pid?: number | null;
  model?: string | null;
  created_at: string;
  started_at?: string | null;
  ended_at?: string | null;
  exit_code?: number | null;
  worktree_owner_repo?: string | null;
};

export type FarmHeartbeat = {
  id: number;
  task_id: string;
  ts: string;
  level: string;
  message: string;
};

export type FarmResult = {
  task_id: string;
  summary?: string | null;
  sha_before?: string | null;
  sha_after?: string | null;
  artifacts_dir?: string | null;
  metrics_json?: string | null;
};

export function farmApiBaseUrl(): string {
  const u = process.env.NEXT_PUBLIC_FARM_API_URL ?? "http://localhost:8801";
  return u.replace(/\/+$/, "");
}

async function get<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${farmApiBaseUrl()}${path}`, {
      cache: "no-store",
      ...init,
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : "Network error";
    throw new Error(msg);
  }
  if (!res.ok) {
    const body = await res.text();
    // The sidecar returns plain JSON {detail: "..."} on 4xx/5xx; if it
    // happens to return HTML (proxy error pages), keep the status line
    // instead of dumping markup into the toast.
    const msg = body.trimStart().startsWith("<")
      ? `HTTP ${res.status} ${res.statusText}`
      : body;
    throw new Error(msg);
  }
  return (await res.json()) as T;
}

export function listFarmTasks(params?: {
  status?: string;
  agent?: string;
  since?: string;
  limit?: number;
}) {
  const q = new URLSearchParams();
  if (params?.status) q.set("status", params.status);
  if (params?.agent) q.set("agent", params.agent);
  if (params?.since) q.set("since", params.since);
  q.set("limit", String(params?.limit ?? 200));
  return get<FarmTask[]>(`/tasks?${q.toString()}`);
}

export function getFarmTask(taskId: string) {
  return get<FarmTask>(`/tasks/${encodeURIComponent(taskId)}`);
}

export function getFarmHeartbeats(taskId: string, limit = 50) {
  return get<FarmHeartbeat[]>(
    `/tasks/${encodeURIComponent(taskId)}/heartbeats?limit=${limit}`,
  );
}

export function getFarmResults(taskId: string) {
  return get<FarmResult>(`/tasks/${encodeURIComponent(taskId)}/results`);
}

// ── plan slices ──────────────────────────────────────────────────────────────
//
// FARM-UI.4 consumes /plan from the farm-api sidecar (see
// apps/farm-api/farm_api/routes/plan.py). The response is a flat list of
// PlanSlice rows from every loop directory; the slice-DAG page rebuilds
// edges client-side off the deps[] field. Status vocabulary matches
// scripts/lib/plan_parser.STATUS_GLYPH (pending, in_progress, blocked,
// done, deferred). The /plan response stays small (sub-200 slices) so we
// page everything in one request and rebuild the graph on render.

export type FarmPlanSlice = {
  id: string;
  title: string;
  loop: string;
  phase: string;
  status: string;
  deps: string[];
  priority?: string | null;
  estimated_hours?: number | null;
  tags: string[];
  requires_human: boolean;
  acceptance?: string | null;
};

export function listFarmPlan(params?: { loop?: string; status?: string }) {
  const q = new URLSearchParams();
  if (params?.loop) q.set("loop", params.loop);
  if (params?.status) q.set("status", params.status);
  const qs = q.toString();
  return get<FarmPlanSlice[]>(qs ? `/plan?${qs}` : "/plan");
}
