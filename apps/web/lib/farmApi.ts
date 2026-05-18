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

// plan slices
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

// ---------------------------------------------------------------------------
// Write surface (FARM-UI.6). The sidecar gates these behind FARM_API_WRITE=1
// + FARM_API_AUTH_TOKEN; the operator provisions the same shared secret as
// the local protea.bearer (or protea.apiKey) so the AuthChip's notion of
// "signed in" is what unlocks the verbs here.
//
// readFarmToken() is the single source of truth for which localStorage key
// becomes the X-Farm-Token header. Bearer wins over API key for parity with
// AuthChip's display logic. Returns null when neither is present (verbs are
// then surfaced as disabled in the palette).
// ---------------------------------------------------------------------------

export type FarmSpawnRequest = {
  next_pickable?: boolean;
  agent?: string;
  task?: string;
  args?: Record<string, unknown>;
};

export type FarmSpawnResponse = {
  task_id: string;
  worktree?: string | null;
  repo?: string | null;
  model?: string | null;
  composed_prompt?: string | null;
  results_dir?: string | null;
  slice_id?: string | null;
  loop?: string | null;
  agent: string;
};

export type FarmKillResponse = {
  task_id: string;
  killed: boolean;
  stdout: string;
};

export type FarmCleanupResponse = {
  apply: boolean;
  stdout: string;
};

export function readFarmToken(): string | null {
  if (typeof window === "undefined") return null;
  try {
    const bearer = window.localStorage.getItem("protea.bearer");
    if (bearer && bearer.length > 16) return bearer;
    const apiKey = window.localStorage.getItem("protea.apiKey");
    if (apiKey && apiKey.length > 8) return apiKey;
  } catch {
    // Private mode / disabled storage falls through to null.
  }
  return null;
}

async function post<T>(
  path: string,
  body: unknown,
  token: string,
): Promise<T> {
  let res: Response;
  try {
    res = await fetch(`${farmApiBaseUrl()}${path}`, {
      method: "POST",
      cache: "no-store",
      headers: {
        "content-type": "application/json",
        "x-farm-token": token,
      },
      body: JSON.stringify(body ?? {}),
    });
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : "Network error";
    throw new Error(msg);
  }
  if (!res.ok) {
    const text = await res.text();
    const msg = text.trimStart().startsWith("<")
      ? `HTTP ${res.status} ${res.statusText}`
      : text;
    throw new Error(msg);
  }
  return (await res.json()) as T;
}

export function spawnFarmTask(
  body: FarmSpawnRequest,
  token: string,
): Promise<FarmSpawnResponse> {
  return post<FarmSpawnResponse>("/spawn", body, token);
}

export function killFarmTask(
  taskId: string,
  token: string,
): Promise<FarmKillResponse> {
  return post<FarmKillResponse>(
    `/tasks/${encodeURIComponent(taskId)}/kill`,
    {},
    token,
  );
}

export function cleanupFarm(
  apply: boolean,
  token: string,
): Promise<FarmCleanupResponse> {
  return post<FarmCleanupResponse>("/cleanup", { apply }, token);
}
