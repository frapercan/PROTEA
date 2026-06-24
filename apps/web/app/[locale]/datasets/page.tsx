"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useLocale, useTranslations } from "next-intl";
import {
  listDatasets,
  listEmbeddingConfigs,
  type Dataset,
  type EmbeddingConfig,
} from "@/lib/api";
import { Breadcrumbs } from "@/components/Breadcrumbs";
import { SkeletonTableRow } from "@/components/Skeleton";
import { NewDatasetDialog } from "@/components/NewDatasetDialog";
import { useToast } from "@/components/Toast";
import { useHasRole } from "@/lib/useRole";
import { useUrlParam } from "@/lib/useUrlParam";

/**
 * Registry view of frozen reranker datasets (``Dataset`` rows produced
 * by ``export_research_dataset`` or by ``POST /datasets/import-by-reference``).
 *
 * Closes the parity gap noted by ux-reviewer 2026-05-23: the conductor
 * was dispatching exports via raw POST /v1/datasets curl because no UI
 * existed. The page lists every registered dump newest-first with the
 * provenance columns operators actually grep for (PLM family, K, train
 * window, schema_sha / manifest_sha shortids, row counts) and offers a
 * single ``Dispatch new export`` CTA that hands off to a generic
 * NewDatasetDialog.
 */

function shortSha(s: string | null): string {
  if (!s) return "—";
  return s.length > 12 ? s.slice(0, 12) : s;
}

function formatDate(iso?: string | null): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString([], { dateStyle: "short", timeStyle: "short" });
}

function formatInt(n: number | null | undefined): string {
  if (n == null) return "—";
  return n.toLocaleString();
}

function trainWindow(pairs: string[]): string {
  if (!pairs || pairs.length === 0) return "—";
  if (pairs.length === 1) return pairs[0];
  // Pairs are "v{old}-v{new}" strings, one per training shard. Render
  // the contiguous range as ``v{firstOld}…v{lastNew}`` plus a count
  // chip so operators can tell at a glance how many shards rolled in.
  return `${pairs[0]} … ${pairs[pairs.length - 1]}`;
}

function familyOf(emb: EmbeddingConfig | undefined): string {
  if (!emb) return "—";
  // The DB EmbeddingConfig row doesn't carry an explicit `family` field
  // (that's a BenchmarkEmbedding concept). Derive a stable family slug
  // from the model_name prefix so the filter chip groups by PLM family.
  const m = emb.model_name.toLowerCase();
  if (m.startsWith("esm2_") || m.startsWith("esm-2")) return "esm2";
  if (m.startsWith("esmc")) return "esmc";
  if (m.startsWith("prot_t5") || m.startsWith("prott5")) return "prot_t5";
  if (m.startsWith("prostt5")) return "prostt5";
  if (m.startsWith("ankh")) return "ankh";
  if (m.startsWith("esm3")) return "esm3";
  // Fallback: first token before underscore or hyphen.
  return m.split(/[_\-]/)[0] ?? m;
}

function copyToClipboard(text: string, onDone: () => void): void {
  if (!text) return;
  void navigator.clipboard.writeText(text).then(onDone, onDone);
}

type SortKey = "created_at" | "name" | "k" | "n_train_rows" | "n_eval_rows";
type SortDir = "asc" | "desc";

export default function DatasetsPage() {
  const t = useTranslations("datasets");
  const locale = useLocale();
  const toast = useToast();

  const [datasets, setDatasets] = useState<Dataset[] | null>(null);
  const [embConfigs, setEmbConfigs] = useState<EmbeddingConfig[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  // URL-synced filters so the page is shareable.
  const [familyRaw, setFamilyRaw] = useUrlParam("family", null);
  const family = familyRaw ?? "all";
  const setFamily = (v: string) => setFamilyRaw(v === "all" ? null : v);
  const [kRaw, setKRaw] = useUrlParam("k", null);
  const kFilter = kRaw ?? "all";
  const setKFilter = (v: string) => setKRaw(v === "all" ? null : v);
  const [search, setSearch] = useState("");

  const [sortKey, setSortKey] = useState<SortKey>("created_at");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const [showDialog, setShowDialog] = useState(false);
  // FEAT-AUTH: dispatch is operator+; viewers see the registry but the
  // CTA is hidden + the empty-state hint switches to a read-only line.
  const canDispatch = useHasRole("operator");

  async function load() {
    setError(null);
    try {
      const [rows, configs] = await Promise.all([listDatasets({ limit: 500 }), listEmbeddingConfigs()]);
      setDatasets(rows);
      setEmbConfigs(configs);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    }
  }

  useEffect(() => {
    // Initial fetch on mount only; user triggers reloads via Refresh button.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void load();
  }, []);

  const embById = useMemo(() => {
    const m = new Map<string, EmbeddingConfig>();
    for (const c of embConfigs ?? []) m.set(c.id, c);
    return m;
  }, [embConfigs]);

  // Available filter chips derived from the visible rows.
  const families = useMemo(() => {
    if (!datasets) return [];
    const s = new Set<string>();
    for (const d of datasets) {
      if (d.embedding_config_id) {
        const fam = familyOf(embById.get(d.embedding_config_id));
        if (fam && fam !== "—") s.add(fam);
      }
    }
    return Array.from(s).sort();
  }, [datasets, embById]);

  const ks = useMemo(() => {
    if (!datasets) return [];
    const s = new Set<number>();
    for (const d of datasets) s.add(d.k);
    return Array.from(s).sort((a, b) => a - b);
  }, [datasets]);

  const filtered = useMemo(() => {
    if (!datasets) return [];
    let rows = datasets;
    if (family !== "all") {
      rows = rows.filter((d) => familyOf(embById.get(d.embedding_config_id ?? "")) === family);
    }
    if (kFilter !== "all") {
      const n = Number(kFilter);
      rows = rows.filter((d) => d.k === n);
    }
    if (search.trim()) {
      const q = search.trim().toLowerCase();
      rows = rows.filter((d) => d.name.toLowerCase().includes(q));
    }
    rows = [...rows].sort((a, b) => {
      const ka = a[sortKey];
      const kb = b[sortKey];
      let cmp = 0;
      if (ka == null && kb == null) cmp = 0;
      else if (ka == null) cmp = -1;
      else if (kb == null) cmp = 1;
      else if (typeof ka === "number" && typeof kb === "number") cmp = ka - kb;
      else cmp = String(ka).localeCompare(String(kb));
      return sortDir === "asc" ? cmp : -cmp;
    });
    return rows;
  }, [datasets, family, kFilter, search, sortKey, sortDir, embById]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortKey(key);
      setSortDir(key === "name" ? "asc" : "desc");
    }
  }

  function onCreated(jobId: string, name: string) {
    toast(t("dispatched", { name, jobId: jobId.slice(0, 8) }), "success");
    // Refresh once the user navigates back; meanwhile point them at the job.
  }

  function onImported(datasetId: string, name: string) {
    toast(t("imported", { name, datasetId: datasetId.slice(0, 8) }), "success");
    // Reload the list so the freshly-registered row appears immediately.
    void load();
  }

  const loading = datasets === null;

  return (
    <>
      <Breadcrumbs />
      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {/* Header */}
        <header className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
          <div>
            <h1 className="text-2xl font-bold text-slate-900">{t("title")}</h1>
            <p className="text-sm text-slate-500 mt-1 max-w-3xl">{t("subtitle")}</p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={load}
              className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
            >
              {t("refresh")}
            </button>
            {canDispatch && (
              <button
                onClick={() => setShowDialog(true)}
                className="rounded-md bg-blue-700 px-3 py-1.5 text-sm font-semibold text-white hover:bg-blue-800"
              >
                {t("newExport")}
              </button>
            )}
          </div>
        </header>

        {/* Filters */}
        <div className="flex flex-wrap items-end gap-3">
          {families.length > 0 && (
            <div>
              <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
                {t("filters.family")}
              </label>
              <div role="group" aria-label={t("filters.family")} className="flex flex-wrap gap-1 rounded-lg bg-slate-100 p-0.5">
                <FilterChip active={family === "all"} onClick={() => setFamily("all")} label={t("filters.all")} />
                {families.map((f) => (
                  <FilterChip key={f} active={family === f} onClick={() => setFamily(f)} label={f} />
                ))}
              </div>
            </div>
          )}

          {ks.length > 0 && (
            <div>
              <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
                {t("filters.k")}
              </label>
              <div role="group" aria-label={t("filters.k")} className="flex flex-wrap gap-1 rounded-lg bg-slate-100 p-0.5">
                <FilterChip active={kFilter === "all"} onClick={() => setKFilter("all")} label={t("filters.all")} />
                {ks.map((n) => (
                  <FilterChip
                    key={n}
                    active={kFilter === String(n)}
                    onClick={() => setKFilter(String(n))}
                    label={`K=${n}`}
                  />
                ))}
              </div>
            </div>
          )}

          <div className="flex-1 min-w-[180px]">
            <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1" htmlFor="ds-search">
              {t("filters.search")}
            </label>
            <input
              id="ds-search"
              type="search"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder={t("filters.searchPlaceholder")}
              className="w-full rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <div className="text-xs text-slate-600 ml-auto self-end">
            {t("count", { shown: filtered.length, total: datasets?.length ?? 0 })}
          </div>
        </div>

        {error && (
          <div
            role="alert"
            className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
          >
            {t("loadError")}: {error}
          </div>
        )}

        {/* Table — desktop */}
        <div className="hidden lg:block overflow-x-auto rounded-lg border bg-white shadow-sm">
          <table className="w-full text-sm">
            <thead className="protea-thead-sticky bg-slate-50 text-left">
              <tr>
                <Th label={t("cols.name")} sortKey="name" current={sortKey} dir={sortDir} onClick={toggleSort} />
                <Th label={t("cols.family")} />
                <Th label={t("cols.k")} sortKey="k" current={sortKey} dir={sortDir} onClick={toggleSort} className="w-20" />
                <Th label={t("cols.trainWindow")} />
                <Th label={t("cols.test")} />
                <Th label={t("cols.schemaSha")} className="w-32" />
                <Th label={t("cols.manifestSha")} className="w-32" />
                <Th label={t("cols.nTrain")} sortKey="n_train_rows" current={sortKey} dir={sortDir} onClick={toggleSort} className="text-right" />
                <Th label={t("cols.nEval")} sortKey="n_eval_rows" current={sortKey} dir={sortDir} onClick={toggleSort} className="text-right" />
                <Th label={t("cols.created")} sortKey="created_at" current={sortKey} dir={sortDir} onClick={toggleSort} />
              </tr>
            </thead>
            <tbody>
              {loading && Array.from({ length: 5 }).map((_, i) => <SkeletonTableRow key={i} cols={10} />)}
              {!loading && filtered.length === 0 && (
                <tr>
                  <td colSpan={10} className="px-4 py-10 text-center text-sm text-slate-600">
                    {datasets!.length === 0 ? (
                      <>
                        <p>{t("emptyAll")}</p>
                        {canDispatch && (
                          <button
                            onClick={() => setShowDialog(true)}
                            className="mt-2 text-sm text-blue-700 underline"
                          >
                            {t("dispatchFirst")}
                          </button>
                        )}
                      </>
                    ) : (
                      t("emptyFiltered")
                    )}
                  </td>
                </tr>
              )}
              {!loading && filtered.map((d) => {
                const emb = embById.get(d.embedding_config_id ?? "");
                const fam = familyOf(emb);
                const href = `/${locale}/datasets/${encodeURIComponent(d.id)}`;
                return (
                  <tr key={d.id} className="border-t hover:bg-blue-50/60 transition-colors">
                    <td className="px-3 py-2.5 max-w-[280px]">
                      <Link href={href} className="font-medium text-slate-900 hover:text-blue-800" title={d.name}>
                        <span className="block font-mono text-[13px] truncate">{d.name}</span>
                      </Link>
                      {emb && (
                        <div className="text-[11px] text-slate-500 mt-0.5 font-mono truncate max-w-[280px]">
                          {emb.model_name} · {emb.pooling}/{emb.layer_agg}
                        </div>
                      )}
                    </td>
                    <td className="px-3 py-2.5">
                      <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-700">
                        {fam}
                      </span>
                    </td>
                    <td className="px-3 py-2.5 text-slate-700 tabular-nums">{d.k}</td>
                    <td className="px-3 py-2.5 text-xs text-slate-700 font-mono">{trainWindow(d.train_snapshot_pairs)}</td>
                    <td className="px-3 py-2.5 text-xs text-slate-700 font-mono">{d.eval_snapshot_pair ?? "—"}</td>
                    <td className="px-3 py-2.5">
                      <CopyableSha sha={d.schema_sha} ariaLabel={t("copy.schemaSha")} toast={toast} t={t} />
                    </td>
                    <td className="px-3 py-2.5">
                      <CopyableSha sha={d.manifest_sha} ariaLabel={t("copy.manifestSha")} toast={toast} t={t} />
                    </td>
                    <td className="px-3 py-2.5 text-right text-slate-700 tabular-nums">{formatInt(d.n_train_rows)}</td>
                    <td className="px-3 py-2.5 text-right text-slate-700 tabular-nums">{formatInt(d.n_eval_rows)}</td>
                    <td className="px-3 py-2.5 text-xs text-slate-500">{formatDate(d.created_at)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Mobile card list */}
        <div className="lg:hidden space-y-2">
          {loading && Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="rounded-lg border bg-white p-3 shadow-sm animate-pulse space-y-2">
              <div className="h-4 bg-slate-200 rounded w-2/3" />
              <div className="h-3 bg-slate-100 rounded w-1/2" />
            </div>
          ))}
          {!loading && filtered.length === 0 && (
            <div className="rounded-lg border bg-white px-4 py-8 text-center text-sm text-slate-600 shadow-sm">
              {datasets!.length === 0 ? t("emptyAll") : t("emptyFiltered")}
            </div>
          )}
          {!loading && filtered.map((d) => {
            const emb = embById.get(d.embedding_config_id ?? "");
            const fam = familyOf(emb);
            const href = `/${locale}/datasets/${encodeURIComponent(d.id)}`;
            return (
              <Link
                key={d.id}
                href={href}
                className="block rounded-lg border bg-white p-3 shadow-sm hover:border-blue-200 hover:bg-blue-50 transition-colors"
              >
                <div className="flex items-start justify-between gap-2">
                  <span className="font-mono text-[13px] font-medium text-slate-900 break-all">{d.name}</span>
                  <span className="shrink-0 inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-700">
                    K={d.k}
                  </span>
                </div>
                <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-slate-600">
                  <span className="inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 font-medium text-slate-700">
                    {fam}
                  </span>
                  {emb && <span className="font-mono">{emb.model_name}</span>}
                </div>
                <div className="mt-1.5 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[11px] text-slate-600">
                  <span><span className="text-slate-400">{t("cols.trainWindow")}:</span> <span className="font-mono">{trainWindow(d.train_snapshot_pairs)}</span></span>
                  <span><span className="text-slate-400">{t("cols.test")}:</span> <span className="font-mono">{d.eval_snapshot_pair ?? "—"}</span></span>
                  <span><span className="text-slate-400">{t("cols.nTrain")}:</span> {formatInt(d.n_train_rows)}</span>
                  <span><span className="text-slate-400">{t("cols.nEval")}:</span> {formatInt(d.n_eval_rows)}</span>
                </div>
                <div className="mt-1.5 flex items-center justify-between text-[11px] text-slate-500">
                  <span className="font-mono">{shortSha(d.schema_sha)}</span>
                  <span>{formatDate(d.created_at)}</span>
                </div>
              </Link>
            );
          })}
        </div>

        <p className="text-xs text-slate-500">
          {t("footnote")}
        </p>
      </div>

      <NewDatasetDialog
        open={showDialog}
        onClose={() => setShowDialog(false)}
        onCreated={onCreated}
        onImported={onImported}
      />
    </>
  );
}

function FilterChip({ active, onClick, label }: { active: boolean; onClick: () => void; label: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
        active ? "bg-white text-slate-900 shadow-sm" : "text-slate-600 hover:text-slate-900"
      }`}
    >
      {label}
    </button>
  );
}

function Th({
  label,
  sortKey,
  current,
  dir,
  onClick,
  className = "",
}: {
  label: string;
  sortKey?: SortKey;
  current?: SortKey;
  dir?: SortDir;
  onClick?: (k: SortKey) => void;
  className?: string;
}) {
  if (!sortKey || !onClick) {
    return (
      <th scope="col" className={`px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500 ${className}`}>
        {label}
      </th>
    );
  }
  const active = current === sortKey;
  return (
    <th
      scope="col"
      aria-sort={active ? (dir === "asc" ? "ascending" : "descending") : "none"}
      className={`px-3 py-2 text-xs font-semibold uppercase tracking-wide text-slate-500 ${className}`}
    >
      <button
        type="button"
        onClick={() => onClick(sortKey)}
        className={`inline-flex items-center gap-1 hover:text-slate-900 transition-colors ${active ? "text-slate-900" : ""}`}
      >
        {label}
        <span aria-hidden className="text-[9px] leading-none">
          {active ? (dir === "asc" ? "▲" : "▼") : "↕"}
        </span>
      </button>
    </th>
  );
}

function CopyableSha({
  sha,
  ariaLabel,
  toast,
  t,
}: {
  sha: string | null;
  ariaLabel: string;
  toast: ReturnType<typeof useToast>;
  t: ReturnType<typeof useTranslations>;
}) {
  if (!sha) return <span className="text-slate-300 text-xs">—</span>;
  return (
    <button
      type="button"
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        copyToClipboard(sha, () => toast(t("copy.copied"), "info"));
      }}
      title={sha}
      aria-label={ariaLabel}
      className="font-mono text-[11px] text-slate-700 hover:text-blue-700"
    >
      {shortSha(sha)}
    </button>
  );
}
