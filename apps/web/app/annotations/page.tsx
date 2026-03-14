"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  listAnnotationSets,
  listOntologySnapshots,
  deleteAnnotationSet,
  createJob,
  AnnotationSet,
  OntologySnapshot,
} from "@/lib/api";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";

type Tab = "sets" | "snapshots" | "load-snapshot" | "load-goa" | "load-quickgo";

const inputClass = "w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const labelClass = "block text-sm font-medium text-gray-700 mb-1";

function formatDate(iso?: string | null) {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "medium" });
}

function shortId(id: string) {
  return id.slice(0, 8);
}

export default function AnnotationsPage() {
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("sets");

  const [sets, setSets] = useState<AnnotationSet[]>([]);
  const [snapshots, setSnapshots] = useState<OntologySnapshot[]>([]);
  const [loadingSets, setLoadingSets] = useState(true);
  const [loadingSnaps, setLoadingSnaps] = useState(true);

  // Load Snapshot form
  const [oboUrl, setOboUrl] = useState("http://purl.obolibrary.org/obo/go/go-basic.obo");
  const [snapResult, setSnapResult] = useState<{ id: string } | null>(null);
  const [snapSubmitting, setSnapSubmitting] = useState(false);

  // Load GOA form
  const [goaSnapshotId, setGoaSnapshotId] = useState("");
  const [goaUrl, setGoaUrl] = useState("");
  const [goaVersion, setGoaVersion] = useState("");
  const [goaResult, setGoaResult] = useState<{ id: string } | null>(null);
  const [goaSubmitting, setGoaSubmitting] = useState(false);

  // Load QuickGO form
  const [qgoSnapshotId, setQgoSnapshotId] = useState("");
  const [qgoVersion, setQgoVersion] = useState("2025-03");
  const [qgoResult, setQgoResult] = useState<{ id: string } | null>(null);
  const [qgoSubmitting, setQgoSubmitting] = useState(false);

  async function loadSets() {
    setLoadingSets(true);
    try {
      setSets(await listAnnotationSets());
    } catch (e: any) {
      toast(e.message ?? "Failed to load annotation sets", "error");
    } finally {
      setLoadingSets(false);
    }
  }

  async function loadSnapshots() {
    setLoadingSnaps(true);
    try {
      const snaps = await listOntologySnapshots();
      setSnapshots(snaps);
      if (snaps.length > 0 && !goaSnapshotId) {
        setGoaSnapshotId(snaps[0].id);
        setQgoSnapshotId(snaps[0].id);
      }
    } catch (e: any) {
      toast(e.message ?? "Failed to load snapshots", "error");
    } finally {
      setLoadingSnaps(false);
    }
  }

  useEffect(() => {
    loadSets();
    loadSnapshots();
  }, []);

  async function handleDeleteSet(id: string) {
    const s = sets.find((a) => a.id === id);
    const count = s?.annotation_count ?? 0;
    const msg = count > 0
      ? `Delete this annotation set and its ${count.toLocaleString()} GO annotations? This cannot be undone.`
      : "Delete this annotation set?";
    if (!confirm(msg)) return;
    try {
      const r = await deleteAnnotationSet(id);
      setSets((prev) => prev.filter((a) => a.id !== id));
      toast(`Deleted (${r.annotations_deleted.toLocaleString()} annotations removed)`, "info");
    } catch (err: any) {
      toast(String(err), "error");
    }
  }

  async function handleLoadSnapshot(e: React.FormEvent) {
    e.preventDefault();
    setSnapSubmitting(true);
    setSnapResult(null);
    try {
      const res = await createJob({
        operation: "load_ontology_snapshot",
        queue_name: "protea.jobs",
        payload: { obo_url: oboUrl },
      });
      setSnapResult(res);
      toast("Job queued", "success");
    } catch (err: any) {
      toast(String(err), "error");
    } finally {
      setSnapSubmitting(false);
    }
  }

  async function handleLoadGoa(e: React.FormEvent) {
    e.preventDefault();
    setGoaSubmitting(true);
    setGoaResult(null);
    try {
      const res = await createJob({
        operation: "load_goa_annotations",
        queue_name: "protea.jobs",
        payload: {
          ontology_snapshot_id: goaSnapshotId,
          gaf_url: goaUrl,
          source_version: goaVersion,
        },
      });
      setGoaResult(res);
      toast("Job queued", "success");
    } catch (err: any) {
      toast(String(err), "error");
    } finally {
      setGoaSubmitting(false);
    }
  }

  async function handleLoadQuickgo(e: React.FormEvent) {
    e.preventDefault();
    setQgoSubmitting(true);
    setQgoResult(null);
    try {
      const payload = {
        ontology_snapshot_id: qgoSnapshotId,
        source_version: qgoVersion,
      };
      const res = await createJob({ operation: "load_quickgo_annotations", queue_name: "protea.jobs", payload });
      setQgoResult(res);
      toast("Job queued", "success");
    } catch (err: any) {
      toast(String(err), "error");
    } finally {
      setQgoSubmitting(false);
    }
  }

  const tabs: { key: Tab; label: string }[] = [
    { key: "sets", label: "Annotation Sets" },
    { key: "snapshots", label: "Ontology Snapshots" },
    { key: "load-snapshot", label: "Load Snapshot" },
    { key: "load-goa", label: "Load GOA" },
    { key: "load-quickgo", label: "Load QuickGO" },
  ];

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">Annotations</h1>
      </div>

      <div className="flex gap-1 border-b mb-6">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === t.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ── Annotation Sets ── */}
      {activeTab === "sets" && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-sm text-gray-500">{sets.length} annotation set{sets.length !== 1 ? "s" : ""}</p>
            <button onClick={loadSets} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
              Refresh
            </button>
          </div>
          <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[80px_100px_140px_100px_1fr_160px_60px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>ID</div><div>Source</div><div>Version</div><div>Annotations</div><div>Meta</div><div>Created</div><div></div>
            </div>
            {loadingSets && Array.from({ length: 3 }).map((_, i) => <SkeletonTableRow key={i} cols={7} />)}
            {!loadingSets && sets.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-400">
                No annotation sets yet. Load GO annotations from the Load GOA or Load QuickGO tabs.
              </div>
            )}
            {sets.map((a) => (
              <div key={a.id} className="grid grid-cols-[80px_100px_140px_100px_1fr_160px_60px] gap-2 border-b px-4 py-3 text-sm last:border-0 items-center">
                <div className="font-mono text-xs text-gray-400" title={a.id}>{shortId(a.id)}</div>
                <div className="font-medium text-gray-800">{a.source}</div>
                <div className="text-xs text-gray-500">{a.source_version ?? "—"}</div>
                <div className="text-gray-700">{(a.annotation_count ?? 0).toLocaleString()}</div>
                <div className="flex flex-wrap gap-1">
                  {a.meta && Object.entries(a.meta).map(([k, v]) => (
                    <span key={k} className="rounded bg-gray-100 px-1.5 py-0.5 text-xs text-gray-600">
                      {k}: {Array.isArray(v) ? v.join(", ") : String(v)}
                    </span>
                  ))}
                </div>
                <div className="flex items-center gap-2 text-xs text-gray-400">
                  {formatDate(a.created_at)}
                  {a.job_id && (
                    <Link href={`/jobs/${a.job_id}`} className="text-blue-400 hover:text-blue-600" title="View job">↗</Link>
                  )}
                </div>
                <div className="flex justify-end">
                  <button
                    onClick={() => handleDeleteSet(a.id)}
                    className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Ontology Snapshots ── */}
      {activeTab === "snapshots" && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-sm text-gray-500">{snapshots.length} snapshot{snapshots.length !== 1 ? "s" : ""}</p>
            <button onClick={loadSnapshots} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
              Refresh
            </button>
          </div>
          <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[80px_1fr_120px_200px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>ID</div><div>Version</div><div>GO Terms</div><div>Loaded</div>
            </div>
            {loadingSnaps && Array.from({ length: 2 }).map((_, i) => <SkeletonTableRow key={i} cols={4} />)}
            {!loadingSnaps && snapshots.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-400">
                No ontology snapshots yet. Use the Load Snapshot tab.
              </div>
            )}
            {snapshots.map((s) => (
              <div key={s.id} className="grid grid-cols-[80px_1fr_120px_200px] gap-2 border-b px-4 py-3 text-sm last:border-0 items-center">
                <div className="font-mono text-xs text-gray-400" title={s.id}>{shortId(s.id)}</div>
                <div className="font-medium text-gray-800">{s.obo_version}</div>
                <div className="text-gray-700">{(s.go_term_count ?? 0).toLocaleString()}</div>
                <div className="text-xs text-gray-400">{formatDate(s.loaded_at)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Load Ontology Snapshot ── */}
      {activeTab === "load-snapshot" && (
        <div className="max-w-lg">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">Load Ontology Snapshot</h2>
            <p className="text-sm text-gray-500 mb-4">Downloads a GO OBO file and populates GOTerm rows.</p>
            <form onSubmit={handleLoadSnapshot} className="space-y-4">
              <div>
                <label className={labelClass}>OBO URL</label>
                <input
                  type="text"
                  value={oboUrl}
                  onChange={(e) => setOboUrl(e.target.value)}
                  required
                  className={inputClass}
                />
              </div>
              {snapResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  Job queued:{" "}
                  <Link href={`/jobs/${snapResult.id}`} className="font-mono underline hover:text-green-900">
                    {snapResult.id}
                  </Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={snapSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {snapSubmitting ? "Launching…" : "Launch Job"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Load GOA Annotations ── */}
      {activeTab === "load-goa" && (
        <div className="max-w-lg">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">Load GOA Annotations</h2>
            <p className="text-sm text-gray-500 mb-4">Bulk-loads GO annotations from a GAF file.</p>
            <form onSubmit={handleLoadGoa} className="space-y-4">
              <div>
                <label className={labelClass}>Ontology Snapshot</label>
                <select value={goaSnapshotId} onChange={(e) => setGoaSnapshotId(e.target.value)} required className={inputClass}>
                  <option value="">— select snapshot —</option>
                  {snapshots.map((s) => (
                    <option key={s.id} value={s.id}>{s.obo_version} · {shortId(s.id)}…</option>
                  ))}
                </select>
                {snapshots.length === 0 && (
                  <p className="mt-1 text-xs text-amber-600">No snapshots — run Load Snapshot first.</p>
                )}
              </div>
              <div>
                <label className={labelClass}>GAF URL</label>
                <input
                  type="text"
                  value={goaUrl}
                  onChange={(e) => setGoaUrl(e.target.value)}
                  required
                  placeholder="https://current.geneontology.org/annotations/goa_human.gaf.gz"
                  className={inputClass}
                />
              </div>
              <div>
                <label className={labelClass}>Source version</label>
                <input
                  type="text"
                  value={goaVersion}
                  onChange={(e) => setGoaVersion(e.target.value)}
                  required
                  placeholder="2025-03"
                  className={inputClass}
                />
              </div>
              {goaResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  Job queued:{" "}
                  <Link href={`/jobs/${goaResult.id}`} className="font-mono underline hover:text-green-900">
                    {goaResult.id}
                  </Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={goaSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {goaSubmitting ? "Launching…" : "Launch Job"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Load QuickGO Annotations ── */}
      {activeTab === "load-quickgo" && (
        <div className="max-w-lg">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">Load QuickGO Annotations</h2>
            <p className="text-sm text-gray-500 mb-4">Streams GO annotations from the QuickGO bulk download API.</p>
            <form onSubmit={handleLoadQuickgo} className="space-y-4">
              <div>
                <label className={labelClass}>Ontology Snapshot</label>
                <select value={qgoSnapshotId} onChange={(e) => setQgoSnapshotId(e.target.value)} required className={inputClass}>
                  <option value="">— select snapshot —</option>
                  {snapshots.map((s) => (
                    <option key={s.id} value={s.id}>{s.obo_version} · {shortId(s.id)}…</option>
                  ))}
                </select>
                {snapshots.length === 0 && (
                  <p className="mt-1 text-xs text-amber-600">No snapshots — run Load Snapshot first.</p>
                )}
              </div>
              <div>
                <label className={labelClass}>Source version</label>
                <input
                  type="text"
                  value={qgoVersion}
                  onChange={(e) => setQgoVersion(e.target.value)}
                  required
                  placeholder="2025-03"
                  className={inputClass}
                />
              </div>
              {qgoResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  Job queued:{" "}
                  <Link href={`/jobs/${qgoResult.id}`} className="font-mono underline hover:text-green-900">
                    {qgoResult.id}
                  </Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={qgoSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {qgoSubmitting ? "Launching…" : "Launch Job"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </>
  );
}
