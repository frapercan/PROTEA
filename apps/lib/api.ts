export type Job = {
  id: string;
  operation: string;
  queue_name: string;
  status: string;
  created_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  progress_current?: number | null;
  progress_total?: number | null;
  error_code?: string | null;
  error_message?: string | null;
};

export type JobEvent = {
  id: number;
  ts: string;
  level: "info" | "warning" | "error";
  event: string;
  message: string | null;
  fields: Record<string, any>;
};

function baseUrl(): string {
  const u = process.env.NEXT_PUBLIC_API_URL;
  if (!u) throw new Error("NEXT_PUBLIC_API_URL is not set");
  return u.replace(/\/+$/, "");
}

async function http<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${baseUrl()}${path}`, { cache: "no-store", ...init });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

export function listJobs(params?: { limit?: number; status?: string; operation?: string }) {
  const q = new URLSearchParams();
  q.set("limit", String(params?.limit ?? 50));
  if (params?.status) q.set("status", params.status);
  if (params?.operation) q.set("operation", params.operation);
  return http<Job[]>(`/jobs?${q.toString()}`);
}

export function getJob(id: string) {
  return http<any>(`/jobs/${id}`);
}

export function getJobEvents(id: string, limit = 200) {
  return http<JobEvent[]>(`/jobs/${id}/events?limit=${limit}`);
}

export function createJob(body: {
  operation: string;
  queue_name: string;
  payload?: Record<string, any>;
  meta?: Record<string, any>;
}) {
  return http<{ id: string; status: string }>(`/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function cancelJob(id: string) {
  return http<{ id: string; status: string }>(`/jobs/${id}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
}