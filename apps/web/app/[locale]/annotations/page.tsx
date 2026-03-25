"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  listAnnotationSets,
  listOntologySnapshots,
  setSnapshotIaUrl,
  deleteAnnotationSet,
  createJob,
  AnnotationSet,
  OntologySnapshot,
} from "@/lib/api";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useTranslations } from "next-intl";

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
  const t = useTranslations("annotations");
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("sets");

  const [sets, setSets] = useState<AnnotationSet[]>([]);
  const [snapshots, setSnapshots] = useState<OntologySnapshot[]>([]);
  const [loadingSets, setLoadingSets] = useState(true);
  const [loadingSnaps, setLoadingSnaps] = useState(true);

  // IA URL inline edit state: snapshotId → current input value (undefined = not editing)
  const [iaEditId, setIaEditId] = useState<string | null>(null);
  const [iaEditValue, setIaEditValue] = useState("");
  const [iaSaving, setIaSaving] = useState(false);

  async function handleSaveIa(snapshotId: string) {
    setIaSaving(true);
    try {
      const result = await setSnapshotIaUrl(snapshotId, iaEditValue.trim() || null);
      setSnapshots((prev) =>
        prev.map((s) => (s.id === snapshotId ? { ...s, ia_url: result.ia_url } : s))
      );
      setIaEditId(null);
      toast("IA URL saved", "success");
    } catch (err: any) {
      toast(String(err), "error");
    } finally {
      setIaSaving(false);
    }
  }

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
      ? t("setsTab.deleteConfirm", { count: count.toLocaleString() })
      : t("setsTab.deleteConfirmNoAnnotations");
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
    { key: "sets", label: t("tabs.sets") },
    { key: "snapshots", label: t("tabs.snapshots") },
    { key: "load-snapshot", label: t("tabs.loadSnapshot") },
    { key: "load-goa", label: t("tabs.loadGoa") },
    { key: "load-quickgo", label: t("tabs.loadQuickgo") },
  ];

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
      </div>

      <div className="border-b mb-6 overflow-hidden"><div className="flex gap-1 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-3 sm:px-4 py-2 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
              activeTab === tab.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div></div>

      {/* ── Annotation Sets ── */}
      {activeTab === "sets" && (
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-sm text-gray-500">{t("setsTab.annotationSets", { count: sets.length })}</p>
            <button onClick={loadSets} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
              {t("setsTab.refresh")}
            </button>
          </div>
          {/* Mobile card list */}
          <div className="lg:hidden space-y-2">
            {loadingSets && Array.from({ length: 3 }).map((_, i) => (
              <div key={i} className="rounded-lg border bg-white p-4 shadow-sm animate-pulse">
                <div className="h-4 bg-gray-200 rounded w-1/3 mb-2" />
                <div className="h-3 bg-gray-100 rounded w-2/3" />
              </div>
            ))}
            {!loadingSets && sets.length === 0 && (
              <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-gray-400 shadow-sm">
                {t("setsTab.noSetsFound")}
              </div>
            )}
            {sets.map((a) => (
              <div key={a.id} className="rounded-lg border bg-white p-4 shadow-sm">
                <div className="flex items-center justify-between mb-1">
                  <span className="font-medium text-gray-800">{a.source}</span>
                  <button
                    onClick={() => handleDeleteSet(a.id)}
                    className="rounded border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 transition-colors"
                  >
                    {t("setsTab.delete")}
                  </button>
                </div>
                <p className="text-xs text-gray-500">{a.source_version ?? "—"} · {(a.annotation_count ?? 0).toLocaleString()} annotations</p>
                <div className="mt-1 flex flex-wrap gap-1">
                  {a.meta && Object.entries(a.meta).map(([k, v]) => (
                    <span key={k} className="rounded bg-gray-100 px-1.5 py-0.5 text-xs text-gray-600">
                      {k}: {Array.isArray(v) ? v.join(", ") : String(v)}
                    </span>
                  ))}
                </div>
                <div className="mt-1 flex items-center gap-2 text-xs text-gray-400">
                  <span className="font-mono">{shortId(a.id)}</span>
                  <span>{formatDate(a.created_at)}</span>
                  {a.job_id && (
                    <Link href={`/jobs/${a.job_id}`} className="text-blue-400 hover:text-blue-600">↗</Link>
                  )}
                </div>
              </div>
            ))}
          </div>

          {/* Desktop table */}
          <div className="hidden lg:block overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[80px_100px_140px_100px_1fr_160px_60px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>{t("setsTab.tableHeaders.id")}</div><div>{t("setsTab.tableHeaders.source")}</div><div>{t("setsTab.tableHeaders.version")}</div><div>{t("setsTab.tableHeaders.annotations")}</div><div>{t("setsTab.tableHeaders.meta")}</div><div>{t("setsTab.tableHeaders.created")}</div><div></div>
            </div>
            {loadingSets && Array.from({ length: 3 }).map((_, i) => <SkeletonTableRow key={i} cols={7} />)}
            {!loadingSets && sets.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-400">
                {t("setsTab.noSetsFound")}
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
                    {t("setsTab.delete")}
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
            <p className="text-sm text-gray-500">{t("snapshotsTab.snapshots", { count: snapshots.length })}</p>
            <button onClick={loadSnapshots} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
              {t("snapshotsTab.refresh")}
            </button>
          </div>
          {/* Mobile card list */}
          <div className="lg:hidden space-y-2">
            {loadingSnaps && Array.from({ length: 2 }).map((_, i) => (
              <div key={i} className="rounded-lg border bg-white p-4 shadow-sm animate-pulse">
                <div className="h-4 bg-gray-200 rounded w-1/3 mb-2" />
                <div className="h-3 bg-gray-100 rounded w-2/3" />
              </div>
            ))}
            {!loadingSnaps && snapshots.length === 0 && (
              <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-gray-400 shadow-sm">
                {t("snapshotsTab.noSnapshotsFound")}
              </div>
            )}
            {snapshots.map((s) => (
              <div key={s.id} className="rounded-lg border bg-white p-4 shadow-sm space-y-2">
                <div className="flex items-center justify-between">
                  <span className="font-medium text-gray-800">{s.obo_version}</span>
                  <span className="text-xs text-gray-400">{(s.go_term_count ?? 0).toLocaleString()} terms</span>
                </div>
                <div className="min-w-0">
                  {iaEditId === s.id ? (
                    <div className="flex flex-col gap-1">
                      <input
                        autoFocus
                        type="text"
                        value={iaEditValue}
                        onChange={(e) => setIaEditValue(e.target.value)}
                        placeholder="https://…/IA_cafa6.tsv or file path"
                        className="w-full rounded border px-2 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
                        onKeyDown={(e) => {
                          if (e.key === "Enter") handleSaveIa(s.id);
                          if (e.key === "Escape") setIaEditId(null);
                        }}
                      />
                      <div className="flex gap-1">
                        <button onClick={() => handleSaveIa(s.id)} disabled={iaSaving} className="rounded bg-blue-600 px-2 py-1 text-xs text-white hover:bg-blue-700 disabled:opacity-50">{t("snapshotsTab.save")}</button>
                        <button onClick={() => setIaEditId(null)} className="rounded border px-2 py-1 text-xs text-gray-500 hover:bg-gray-50">{t("snapshotsTab.cancel")}</button>
                      </div>
                    </div>
                  ) : (
                    <button
                      onClick={() => { setIaEditId(s.id); setIaEditValue(s.ia_url ?? ""); }}
                      className="w-full text-left flex items-center gap-2 rounded px-1 py-0.5 hover:bg-gray-50 active:bg-gray-100 transition-colors"
                      title={t("snapshotsTab.editTooltip")}
                    >
                      {s.ia_url ? (
                        <span className="truncate text-xs text-gray-500 font-mono flex-1">{s.ia_url}</span>
                      ) : (
                        <span className="text-xs text-amber-500 italic flex-1">{t("snapshotsTab.notSet")}</span>
                      )}
                      <span className="shrink-0 text-gray-400 text-xs">✎</span>
                    </button>
                  )}
                </div>
                <div className="flex items-center gap-2 text-xs text-gray-400">
                  <span className="font-mono">{shortId(s.id)}</span>
                  <span>{formatDate(s.loaded_at)}</span>
                </div>
              </div>
            ))}
          </div>

          {/* Desktop table */}
          <div className="hidden lg:block overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[80px_160px_100px_minmax(160px,1fr)_160px] min-w-[700px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>{t("snapshotsTab.tableHeaders.id")}</div><div>{t("snapshotsTab.tableHeaders.version")}</div><div>{t("snapshotsTab.tableHeaders.goTerms")}</div><div>{t("snapshotsTab.tableHeaders.iaUrl")}</div><div>{t("snapshotsTab.tableHeaders.loaded")}</div>
            </div>
            {loadingSnaps && Array.from({ length: 2 }).map((_, i) => <SkeletonTableRow key={i} cols={5} />)}
            {!loadingSnaps && snapshots.length === 0 && (
              <div className="px-4 py-8 text-center text-sm text-gray-400">
                {t("snapshotsTab.noSnapshotsFound")}
              </div>
            )}
            {snapshots.map((s) => (
              <div key={s.id} className="grid grid-cols-[80px_160px_100px_minmax(160px,1fr)_160px] min-w-[700px] gap-2 border-b px-4 py-3 text-sm last:border-0 items-center">
                <div className="font-mono text-xs text-gray-400" title={s.id}>{shortId(s.id)}</div>
                <div className="font-medium text-gray-800">{s.obo_version}</div>
                <div className="text-gray-700">{(s.go_term_count ?? 0).toLocaleString()}</div>
                <div className="min-w-0">
                  {iaEditId === s.id ? (
                    <div className="flex items-center gap-1">
                      <input
                        autoFocus
                        type="text"
                        value={iaEditValue}
                        onChange={(e) => setIaEditValue(e.target.value)}
                        placeholder="https://…/IA_cafa6.tsv or file path"
                        className="flex-1 min-w-0 rounded border px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-blue-500"
                        onKeyDown={(e) => {
                          if (e.key === "Enter") handleSaveIa(s.id);
                          if (e.key === "Escape") setIaEditId(null);
                        }}
                      />
                      <button
                        onClick={() => handleSaveIa(s.id)}
                        disabled={iaSaving}
                        className="rounded bg-blue-600 px-2 py-1 text-xs text-white hover:bg-blue-700 disabled:opacity-50"
                      >
                        {t("snapshotsTab.save")}
                      </button>
                      <button
                        onClick={() => setIaEditId(null)}
                        className="rounded border px-2 py-1 text-xs text-gray-500 hover:bg-gray-50"
                      >
                        {t("snapshotsTab.cancel")}
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={() => { setIaEditId(s.id); setIaEditValue(s.ia_url ?? ""); }}
                      className="w-full text-left flex items-center gap-2 rounded px-1 py-0.5 hover:bg-gray-50 active:bg-gray-100 transition-colors"
                      title={t("snapshotsTab.editTooltip")}
                    >
                      {s.ia_url ? (
                        <span className="truncate text-xs text-gray-500 font-mono flex-1">{s.ia_url}</span>
                      ) : (
                        <span className="text-xs text-amber-500 italic flex-1">{t("snapshotsTab.notSet")}</span>
                      )}
                      <span className="shrink-0 text-gray-400 text-xs">✎</span>
                    </button>
                  )}
                </div>
                <div className="text-xs text-gray-400">{formatDate(s.loaded_at)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Load Ontology Snapshot ── */}
      {activeTab === "load-snapshot" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">{t("loadSnapshotTab.title")}</h2>
            <p className="text-sm text-gray-500 mb-4">{t("loadSnapshotTab.description")}</p>
            <form onSubmit={handleLoadSnapshot} className="space-y-4">
              <div>
                <label className={labelClass}>{t("loadSnapshotTab.oboUrlLabel")}</label>
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
                  {snapSubmitting ? t("loadSnapshotTab.launching") : t("loadSnapshotTab.launchJob")}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Load GOA Annotations ── */}
      {activeTab === "load-goa" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">{t("loadGoaTab.title")}</h2>
            <p className="text-sm text-gray-500 mb-4">{t("loadGoaTab.description")}</p>
            <form onSubmit={handleLoadGoa} className="space-y-4">
              <div>
                <label className={labelClass}>{t("loadGoaTab.snapshotLabel")}</label>
                <select value={goaSnapshotId} onChange={(e) => setGoaSnapshotId(e.target.value)} required className={inputClass}>
                  <option value="">{t("loadGoaTab.selectSnapshot")}</option>
                  {snapshots.map((s) => (
                    <option key={s.id} value={s.id}>{s.obo_version} · {shortId(s.id)}…</option>
                  ))}
                </select>
                {snapshots.length === 0 && (
                  <p className="mt-1 text-xs text-amber-600">{t("loadGoaTab.noSnapshots")}</p>
                )}
              </div>
              <div>
                <label className={labelClass}>{t("loadGoaTab.gafUrlLabel")}</label>
                <input
                  type="text"
                  value={goaUrl}
                  onChange={(e) => setGoaUrl(e.target.value)}
                  required
                  placeholder={t("loadGoaTab.gafUrlPlaceholder")}
                  className={inputClass}
                />
              </div>
              <div>
                <label className={labelClass}>{t("loadGoaTab.sourceVersionLabel")}</label>
                <input
                  type="text"
                  value={goaVersion}
                  onChange={(e) => setGoaVersion(e.target.value)}
                  required
                  placeholder={t("loadGoaTab.sourceVersionPlaceholder")}
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
                  {goaSubmitting ? t("loadGoaTab.launching") : t("loadGoaTab.launchJob")}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Load QuickGO Annotations ── */}
      {activeTab === "load-quickgo" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">{t("loadQuickgoTab.title")}</h2>
            <p className="text-sm text-gray-500 mb-4">{t("loadQuickgoTab.description")}</p>
            <form onSubmit={handleLoadQuickgo} className="space-y-4">
              <div>
                <label className={labelClass}>{t("loadQuickgoTab.snapshotLabel")}</label>
                <select value={qgoSnapshotId} onChange={(e) => setQgoSnapshotId(e.target.value)} required className={inputClass}>
                  <option value="">{t("loadQuickgoTab.selectSnapshot")}</option>
                  {snapshots.map((s) => (
                    <option key={s.id} value={s.id}>{s.obo_version} · {shortId(s.id)}…</option>
                  ))}
                </select>
                {snapshots.length === 0 && (
                  <p className="mt-1 text-xs text-amber-600">{t("loadQuickgoTab.noSnapshots")}</p>
                )}
              </div>
              <div>
                <label className={labelClass}>{t("loadQuickgoTab.sourceVersionLabel")}</label>
                <input
                  type="text"
                  value={qgoVersion}
                  onChange={(e) => setQgoVersion(e.target.value)}
                  required
                  placeholder={t("loadQuickgoTab.sourceVersionPlaceholder")}
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
                  {qgoSubmitting ? t("loadQuickgoTab.launching") : t("loadQuickgoTab.launchJob")}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </>
  );
}
