"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import {
  getProteinStats,
  listProteins,
  createJob,
  ProteinItem,
  ProteinStats,
} from "@/lib/api";

type Tab = "browse" | "stats" | "insert" | "metadata";

const inputClass = "w-full rounded-md border px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
const labelClass = "block text-sm font-medium text-gray-700 mb-1";
const PAGE_SIZE = 50;

function ReviewedBadge({ reviewed }: { reviewed?: boolean | null }) {
  if (reviewed === true)
    return <span className="rounded-full bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700 border border-blue-100">Swiss-Prot</span>;
  if (reviewed === false)
    return <span className="rounded-full bg-gray-50 px-2 py-0.5 text-xs font-medium text-gray-500 border border-gray-200">TrEMBL</span>;
  return <span className="text-gray-300 text-xs">—</span>;
}

function StatCard({ label, value, sub }: { label: string; value: number; sub?: string }) {
  return (
    <div className="rounded-lg border bg-white p-4 shadow-sm">
      <p className="text-xs font-medium uppercase tracking-wide text-gray-500">{label}</p>
      <p className="mt-1 text-2xl font-bold text-gray-900">{value.toLocaleString()}</p>
      {sub && <p className="mt-0.5 text-xs text-gray-400">{sub}</p>}
    </div>
  );
}

export default function ProteinsPage() {
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("browse");

  // Browse state
  const [proteins, setProteins] = useState<ProteinItem[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [search, setSearch] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [reviewedFilter, setReviewedFilter] = useState<"all" | "reviewed" | "unreviewed">("all");
  const [canonicalOnly, setCanonicalOnly] = useState(true);
  const [loadingBrowse, setLoadingBrowse] = useState(false);

  // Stats state
  const [stats, setStats] = useState<ProteinStats | null>(null);
  const [loadingStats, setLoadingStats] = useState(false);

  // Insert proteins form
  const [searchCriteria, setSearchCriteria] = useState("organism_id:9606 AND reviewed:true");
  const [pageSize, setPageSize] = useState(500);
  const [totalLimit, setTotalLimit] = useState("");
  const [includeIsoforms, setIncludeIsoforms] = useState(true);
  const [insertResult, setInsertResult] = useState<{ id: string } | null>(null);
  const [insertSubmitting, setInsertSubmitting] = useState(false);

  // Fetch metadata form
  const [metaCriteria, setMetaCriteria] = useState("organism_id:9606 AND reviewed:true");
  const [metaPageSize, setMetaPageSize] = useState(500);
  const [metaLimit, setMetaLimit] = useState("");
  const [metaResult, setMetaResult] = useState<{ id: string } | null>(null);
  const [metaSubmitting, setMetaSubmitting] = useState(false);

  const loadProteins = useCallback(async (currentOffset = 0, currentSearch = search, reviewed = reviewedFilter, canonical = canonicalOnly) => {
    setLoadingBrowse(true);
    try {
      const res = await listProteins({
        search: currentSearch || undefined,
        reviewed: reviewed === "reviewed" ? true : reviewed === "unreviewed" ? false : undefined,
        canonical_only: canonical,
        limit: PAGE_SIZE,
        offset: currentOffset,
      });
      setProteins(res.items);
      setTotal(res.total);
      setOffset(currentOffset);
    } catch (e: any) {
      toast(e.message ?? "Failed to load proteins", "error");
    } finally {
      setLoadingBrowse(false);
    }
  }, [search, reviewedFilter, canonicalOnly]);

  const loadStats = async () => {
    setLoadingStats(true);
    try {
      setStats(await getProteinStats());
    } catch (e: any) {
      toast(e.message ?? "Failed to load stats", "error");
    } finally {
      setLoadingStats(false);
    }
  };

  useEffect(() => {
    if (activeTab === "browse") loadProteins(0, search, reviewedFilter, canonicalOnly);
    if (activeTab === "stats") loadStats();
  }, [activeTab]);

  function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    setSearch(searchInput);
    loadProteins(0, searchInput, reviewedFilter, canonicalOnly);
  }

  function handleFilterChange(reviewed: typeof reviewedFilter, canonical: boolean) {
    setReviewedFilter(reviewed);
    setCanonicalOnly(canonical);
    loadProteins(0, search, reviewed, canonical);
  }

  async function handleInsertSubmit(e: React.FormEvent) {
    e.preventDefault();
    setInsertSubmitting(true);
    setInsertResult(null);
    try {
      const payload: Record<string, any> = { search_criteria: searchCriteria, page_size: pageSize, include_isoforms: includeIsoforms };
      if (totalLimit) payload.total_limit = parseInt(totalLimit, 10);
      const res = await createJob({ operation: "insert_proteins", queue_name: "protea.jobs", payload });
      setInsertResult(res);
      toast("Job queued", "success");
    } catch (err: any) {
      toast(String(err), "error");
    } finally {
      setInsertSubmitting(false);
    }
  }

  async function handleMetaSubmit(e: React.FormEvent) {
    e.preventDefault();
    setMetaSubmitting(true);
    setMetaResult(null);
    try {
      const payload: Record<string, any> = { search_criteria: metaCriteria, page_size: metaPageSize };
      if (metaLimit) payload.total_limit = parseInt(metaLimit, 10);
      const res = await createJob({ operation: "fetch_uniprot_metadata", queue_name: "protea.jobs", payload });
      setMetaResult(res);
      toast("Job queued", "success");
    } catch (err: any) {
      toast(String(err), "error");
    } finally {
      setMetaSubmitting(false);
    }
  }

  const totalPages = Math.ceil(total / PAGE_SIZE);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  const tabs: { key: Tab; label: string }[] = [
    { key: "browse", label: "Browse" },
    { key: "stats", label: "Stats" },
    { key: "insert", label: "Insert Proteins" },
    { key: "metadata", label: "Fetch Metadata" },
  ];

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">Proteins</h1>
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

      {/* ── Browse ── */}
      {activeTab === "browse" && (
        <div>
          {/* Filters */}
          <div className="flex flex-wrap items-center gap-3 mb-4">
            <form onSubmit={handleSearch} className="flex gap-2">
              <input
                type="text"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                placeholder="accession, gene, organism…"
                className="rounded-md border px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-64"
              />
              <button type="submit" className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
                Search
              </button>
              {search && (
                <button
                  type="button"
                  onClick={() => { setSearchInput(""); setSearch(""); loadProteins(0, "", reviewedFilter, canonicalOnly); }}
                  className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50 text-gray-500"
                >
                  Clear
                </button>
              )}
            </form>

            <select
              value={reviewedFilter}
              onChange={(e) => handleFilterChange(e.target.value as typeof reviewedFilter, canonicalOnly)}
              className="rounded-md border bg-white px-3 py-1.5 text-sm focus:outline-none"
            >
              <option value="all">All proteins</option>
              <option value="reviewed">Swiss-Prot only</option>
              <option value="unreviewed">TrEMBL only</option>
            </select>

            <label className="flex items-center gap-1.5 text-sm text-gray-600 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={canonicalOnly}
                onChange={(e) => handleFilterChange(reviewedFilter, e.target.checked)}
                className="rounded"
              />
              Canonical only
            </label>

            <span className="ml-auto text-sm text-gray-400">{total.toLocaleString()} proteins</span>
          </div>

          {/* Table */}
          <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[130px_140px_120px_1fr_80px_110px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>Accession</div>
              <div>Entry Name</div>
              <div>Gene</div>
              <div>Organism</div>
              <div>Length</div>
              <div>Source</div>
            </div>

            {loadingBrowse && Array.from({ length: 8 }).map((_, i) => <SkeletonTableRow key={i} cols={6} />)}

            {!loadingBrowse && proteins.length === 0 && (
              <div className="px-4 py-12 text-center text-sm text-gray-400">
                No proteins found. Use the Insert Proteins tab to import from UniProt.
              </div>
            )}

            {!loadingBrowse && proteins.map((p) => (
              <Link
                key={p.accession}
                href={`/proteins/${p.accession}`}
                className="grid grid-cols-[130px_140px_120px_1fr_80px_110px] gap-2 border-b px-4 py-3 text-sm hover:bg-blue-50 transition-colors last:border-0 items-center"
              >
                <div className="font-mono text-xs text-blue-600">{p.accession}</div>
                <div className="text-gray-700 truncate text-xs">{p.entry_name ?? "—"}</div>
                <div className="font-medium text-gray-800 truncate">{p.gene_name ?? "—"}</div>
                <div className="text-xs text-gray-500 truncate">{p.organism ?? "—"}</div>
                <div className="text-xs text-gray-600">{p.length?.toLocaleString() ?? "—"}</div>
                <div><ReviewedBadge reviewed={p.reviewed} /></div>
              </Link>
            ))}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="mt-4 flex items-center justify-between text-sm text-gray-500">
              <span>Page {currentPage} of {totalPages}</span>
              <div className="flex gap-2">
                <button
                  onClick={() => loadProteins(offset - PAGE_SIZE)}
                  disabled={offset === 0}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-gray-50 disabled:opacity-40"
                >
                  Previous
                </button>
                <button
                  onClick={() => loadProteins(offset + PAGE_SIZE)}
                  disabled={offset + PAGE_SIZE >= total}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-gray-50 disabled:opacity-40"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Stats ── */}
      {activeTab === "stats" && (
        <div>
          <div className="flex justify-end mb-4">
            <button onClick={loadStats} className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
              Refresh
            </button>
          </div>
          {loadingStats && <p className="text-sm text-gray-400">Loading…</p>}
          {stats && (
            <div className="space-y-6">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">Overview</p>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  <StatCard label="Total Proteins" value={stats.total} />
                  <StatCard label="Canonical" value={stats.canonical} sub={`${stats.isoforms.toLocaleString()} isoforms`} />
                  <StatCard label="Swiss-Prot" value={stats.reviewed} sub="reviewed" />
                  <StatCard label="TrEMBL" value={stats.unreviewed} sub="unreviewed" />
                </div>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">Coverage</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                  <StatCard
                    label="With Metadata"
                    value={stats.with_metadata}
                    sub={stats.canonical > 0 ? `${Math.round((stats.with_metadata / stats.canonical) * 100)}% of canonical` : undefined}
                  />
                  <StatCard
                    label="With Embeddings"
                    value={stats.with_embeddings}
                    sub={stats.total > 0 ? `${Math.round((stats.with_embeddings / stats.total) * 100)}% of total` : undefined}
                  />
                  <StatCard
                    label="With GO Annotations"
                    value={stats.with_go_annotations}
                    sub={stats.total > 0 ? `${Math.round((stats.with_go_annotations / stats.total) * 100)}% of total` : undefined}
                  />
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Insert Proteins ── */}
      {activeTab === "insert" && (
        <div className="max-w-lg">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">Insert Proteins from UniProt</h2>
            <p className="text-sm text-gray-500 mb-4">Downloads FASTA sequences and upserts Protein + Sequence rows.</p>
            <form onSubmit={handleInsertSubmit} className="space-y-4">
              <div>
                <label className={labelClass}>Search criteria</label>
                <input type="text" value={searchCriteria} onChange={(e) => setSearchCriteria(e.target.value)} required className={inputClass} placeholder="organism_id:9606 AND reviewed:true" />
                <p className="mt-1 text-xs text-gray-400">UniProt query — <code>reviewed:true</code> = Swiss-Prot only</p>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className={labelClass}>Page size</label>
                  <input type="number" value={pageSize} onChange={(e) => setPageSize(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                </div>
                <div>
                  <label className={labelClass}>Total limit <span className="font-normal text-gray-400">(optional)</span></label>
                  <input type="number" value={totalLimit} onChange={(e) => setTotalLimit(e.target.value)} placeholder="all" className={inputClass} />
                </div>
              </div>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input type="checkbox" checked={includeIsoforms} onChange={(e) => setIncludeIsoforms(e.target.checked)} className="rounded" />
                Include isoforms
              </label>
              {insertResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  Job queued:{" "}
                  <Link href={`/jobs/${insertResult.id}`} className="font-mono underline hover:text-green-900">{insertResult.id}</Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={insertSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {insertSubmitting ? "Launching…" : "Launch Job"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Fetch Metadata ── */}
      {activeTab === "metadata" && (
        <div className="max-w-lg">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">Fetch UniProt Metadata</h2>
            <p className="text-sm text-gray-500 mb-4">Downloads TSV annotations and upserts ProteinUniProtMetadata rows.</p>
            <form onSubmit={handleMetaSubmit} className="space-y-4">
              <div>
                <label className={labelClass}>Search criteria</label>
                <input type="text" value={metaCriteria} onChange={(e) => setMetaCriteria(e.target.value)} required className={inputClass} placeholder="organism_id:9606 AND reviewed:true" />
                <p className="mt-1 text-xs text-gray-400">UniProt query — <code>reviewed:true</code> = Swiss-Prot only</p>
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className={labelClass}>Page size</label>
                  <input type="number" value={metaPageSize} onChange={(e) => setMetaPageSize(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                </div>
                <div>
                  <label className={labelClass}>Total limit <span className="font-normal text-gray-400">(optional)</span></label>
                  <input type="number" value={metaLimit} onChange={(e) => setMetaLimit(e.target.value)} placeholder="all" className={inputClass} />
                </div>
              </div>
              {metaResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  Job queued:{" "}
                  <Link href={`/jobs/${metaResult.id}`} className="font-mono underline hover:text-green-900">{metaResult.id}</Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={metaSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {metaSubmitting ? "Launching…" : "Launch Job"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </>
  );
}
