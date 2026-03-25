"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import { useToast } from "@/components/Toast";
import { SkeletonTableRow } from "@/components/Skeleton";
import { useTranslations } from "next-intl";
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
  const t = useTranslations("proteins");
  if (reviewed === true)
    return <span className="rounded-full bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700 border border-blue-100">{t("sourceSwissProt")}</span>;
  if (reviewed === false)
    return <span className="rounded-full bg-gray-50 px-2 py-0.5 text-xs font-medium text-gray-500 border border-gray-200">{t("sourceTrembl")}</span>;
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
  const t = useTranslations("proteins");
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
    { key: "browse", label: t("tabs.browse") },
    { key: "stats", label: t("tabs.stats") },
    { key: "insert", label: t("tabs.insert") },
    { key: "metadata", label: t("tabs.metadata") },
  ];

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-xl font-semibold">{t("title")}</h1>
      </div>

      <div className="flex gap-1 border-b mb-6 overflow-x-auto">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveTab(tab.key)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
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
                placeholder={t("browseTab.searchPlaceholder")}
                className="rounded-md border px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-full sm:w-64"
              />
              <button type="submit" className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50">
                {t("browseTab.search")}
              </button>
              {search && (
                <button
                  type="button"
                  onClick={() => { setSearchInput(""); setSearch(""); loadProteins(0, "", reviewedFilter, canonicalOnly); }}
                  className="rounded-md border bg-white px-3 py-1.5 text-sm hover:bg-gray-50 text-gray-500"
                >
                  {t("browseTab.clear")}
                </button>
              )}
            </form>

            <select
              value={reviewedFilter}
              onChange={(e) => handleFilterChange(e.target.value as typeof reviewedFilter, canonicalOnly)}
              className="rounded-md border bg-white px-3 py-1.5 text-sm focus:outline-none"
            >
              <option value="all">{t("browseTab.allProteins")}</option>
              <option value="reviewed">{t("browseTab.swissProt")}</option>
              <option value="unreviewed">{t("browseTab.trembl")}</option>
            </select>

            <label className="flex items-center gap-1.5 text-sm text-gray-600 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={canonicalOnly}
                onChange={(e) => handleFilterChange(reviewedFilter, e.target.checked)}
                className="rounded"
              />
              {t("browseTab.canonicalOnly")}
            </label>

            <span className="ml-auto text-sm text-gray-400">{t("browseTab.totalProteins", { count: total.toLocaleString() })}</span>
          </div>

          {/* Mobile card list */}
          <div className="lg:hidden space-y-2">
            {loadingBrowse && Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="rounded-lg border bg-white p-4 shadow-sm animate-pulse">
                <div className="h-4 bg-gray-200 rounded w-1/3 mb-2" />
                <div className="h-3 bg-gray-100 rounded w-2/3" />
              </div>
            ))}
            {!loadingBrowse && proteins.length === 0 && (
              <div className="rounded-lg border bg-white px-4 py-12 text-center text-sm text-gray-400 shadow-sm">
                {t("browseTab.noProteinsCta")}
              </div>
            )}
            {!loadingBrowse && proteins.map((p) => (
              <Link
                key={p.accession}
                href={`/proteins/${p.accession}`}
                className="block rounded-lg border bg-white p-4 shadow-sm hover:bg-blue-50 transition-colors"
              >
                <div className="flex items-center justify-between mb-1">
                  <span className="font-mono text-sm text-blue-600">{p.accession}</span>
                  <ReviewedBadge reviewed={p.reviewed} />
                </div>
                <p className="text-sm font-medium text-gray-800 truncate">{p.gene_name ?? "—"}</p>
                <p className="text-xs text-gray-500 truncate">{p.organism ?? "—"}</p>
                <div className="mt-1 flex gap-3 text-xs text-gray-400">
                  <span>{p.entry_name ?? "—"}</span>
                  {p.length != null && <span>{p.length.toLocaleString()} aa</span>}
                </div>
              </Link>
            ))}
          </div>

          {/* Desktop table */}
          <div className="hidden lg:block overflow-x-auto rounded-lg border bg-white shadow-sm">
            <div className="grid grid-cols-[130px_140px_120px_1fr_80px_110px] gap-2 border-b bg-gray-50 px-4 py-2.5 text-xs font-semibold uppercase tracking-wide text-gray-500">
              <div>{t("browseTab.tableHeaders.accession")}</div>
              <div>{t("browseTab.tableHeaders.entryName")}</div>
              <div>{t("browseTab.tableHeaders.gene")}</div>
              <div>{t("browseTab.tableHeaders.organism")}</div>
              <div>{t("browseTab.tableHeaders.length")}</div>
              <div>{t("browseTab.tableHeaders.source")}</div>
            </div>

            {loadingBrowse && Array.from({ length: 8 }).map((_, i) => <SkeletonTableRow key={i} cols={6} />)}

            {!loadingBrowse && proteins.length === 0 && (
              <div className="px-4 py-12 text-center text-sm text-gray-400">
                {t("browseTab.noProteinsCta")}
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
              <span>{t("browseTab.pagination.page", { current: currentPage, total: totalPages })}</span>
              <div className="flex gap-2">
                <button
                  onClick={() => loadProteins(offset - PAGE_SIZE)}
                  disabled={offset === 0}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-gray-50 disabled:opacity-40"
                >
                  {t("browseTab.pagination.previous")}
                </button>
                <button
                  onClick={() => loadProteins(offset + PAGE_SIZE)}
                  disabled={offset + PAGE_SIZE >= total}
                  className="rounded-md border bg-white px-3 py-1.5 hover:bg-gray-50 disabled:opacity-40"
                >
                  {t("browseTab.pagination.next")}
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
              {t("statsTab.refresh")}
            </button>
          </div>
          {loadingStats && <p className="text-sm text-gray-400">{t("statsTab.loading")}</p>}
          {stats && (
            <div className="space-y-6">
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">{t("statsTab.overview")}</p>
                <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                  <StatCard label={t("statsTab.totalProteins")} value={stats.total} />
                  <StatCard label={t("statsTab.canonical")} value={stats.canonical} sub={t("statsTab.isoforms", { count: stats.isoforms.toLocaleString() })} />
                  <StatCard label={t("statsTab.reviewed")} value={stats.reviewed} sub={t("statsTab.reviewedSub")} />
                  <StatCard label={t("statsTab.unreviewed")} value={stats.unreviewed} sub={t("statsTab.unreviewedSub")} />
                </div>
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 mb-3">{t("statsTab.coverage")}</p>
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                  <StatCard
                    label={t("statsTab.withMetadata")}
                    value={stats.with_metadata}
                    sub={stats.canonical > 0 ? t("statsTab.metadataSub", { percent: Math.round((stats.with_metadata / stats.canonical) * 100) }) : undefined}
                  />
                  <StatCard
                    label={t("statsTab.withEmbeddings")}
                    value={stats.with_embeddings}
                    sub={stats.total > 0 ? t("statsTab.embeddingsSub", { percent: Math.round((stats.with_embeddings / stats.total) * 100) }) : undefined}
                  />
                  <StatCard
                    label={t("statsTab.withGoAnnotations")}
                    value={stats.with_go_annotations}
                    sub={stats.total > 0 ? t("statsTab.goAnnotationsSub", { percent: Math.round((stats.with_go_annotations / stats.total) * 100) }) : undefined}
                  />
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* ── Insert Proteins ── */}
      {activeTab === "insert" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">{t("insertTab.title")}</h2>
            <p className="text-sm text-gray-500 mb-4">{t("insertTab.description")}</p>
            <form onSubmit={handleInsertSubmit} className="space-y-4">
              <div>
                <label className={labelClass}>{t("insertTab.searchCriteriaLabel")}</label>
                <input type="text" value={searchCriteria} onChange={(e) => setSearchCriteria(e.target.value)} required className={inputClass} placeholder="organism_id:9606 AND reviewed:true" />
                <p className="mt-1 text-xs text-gray-400">{t("insertTab.searchCriteriaHelper")}</p>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div>
                  <label className={labelClass}>{t("insertTab.pageSizeLabel")}</label>
                  <input type="number" value={pageSize} onChange={(e) => setPageSize(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                </div>
                <div>
                  <label className={labelClass}>{t("insertTab.totalLimitLabel")} <span className="font-normal text-gray-400">{t("insertTab.totalLimitOptional")}</span></label>
                  <input type="number" value={totalLimit} onChange={(e) => setTotalLimit(e.target.value)} placeholder="all" className={inputClass} />
                </div>
              </div>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input type="checkbox" checked={includeIsoforms} onChange={(e) => setIncludeIsoforms(e.target.checked)} className="rounded" />
                {t("insertTab.includeIsoforms")}
              </label>
              {insertResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  {t("insertTab.jobQueuedPrefix")}
                  <Link href={`/jobs/${insertResult.id}`} className="font-mono underline hover:text-green-900">{insertResult.id}</Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={insertSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {insertSubmitting ? t("insertTab.launching") : t("insertTab.launchJob")}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* ── Fetch Metadata ── */}
      {activeTab === "metadata" && (
        <div className="max-w-2xl">
          <div className="rounded-lg border bg-white p-6 shadow-sm">
            <h2 className="text-base font-semibold mb-1">{t("metadataTab.title")}</h2>
            <p className="text-sm text-gray-500 mb-4">{t("metadataTab.description")}</p>
            <form onSubmit={handleMetaSubmit} className="space-y-4">
              <div>
                <label className={labelClass}>{t("metadataTab.searchCriteriaLabel")}</label>
                <input type="text" value={metaCriteria} onChange={(e) => setMetaCriteria(e.target.value)} required className={inputClass} placeholder="organism_id:9606 AND reviewed:true" />
                <p className="mt-1 text-xs text-gray-400">{t("metadataTab.searchCriteriaHelper")}</p>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                <div>
                  <label className={labelClass}>{t("metadataTab.pageSizeLabel")}</label>
                  <input type="number" value={metaPageSize} onChange={(e) => setMetaPageSize(parseInt(e.target.value, 10))} min={1} className={inputClass} />
                </div>
                <div>
                  <label className={labelClass}>{t("metadataTab.totalLimitLabel")} <span className="font-normal text-gray-400">{t("metadataTab.totalLimitOptional")}</span></label>
                  <input type="number" value={metaLimit} onChange={(e) => setMetaLimit(e.target.value)} placeholder="all" className={inputClass} />
                </div>
              </div>
              {metaResult && (
                <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-700">
                  {t("insertTab.jobQueuedPrefix")}
                  <Link href={`/jobs/${metaResult.id}`} className="font-mono underline hover:text-green-900">{metaResult.id}</Link>
                </div>
              )}
              <div className="flex justify-end">
                <button type="submit" disabled={metaSubmitting} className="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50">
                  {metaSubmitting ? t("metadataTab.launching") : t("metadataTab.launchJob")}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </>
  );
}
