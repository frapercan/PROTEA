"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { Skeleton } from "@/components/Skeleton";
import {
  getBenchmarkEmbeddings,
  getBenchmarkMatrix,
  type BenchmarkBestCell,
  type BenchmarkEmbedding,
  type BenchmarkEvalSet,
  type BenchmarkMatrixResponse,
  type BenchmarkRow,
  type BenchmarkStage,
} from "../../../lib/api";

// ── Helpers ──────────────────────────────────────────────────────────────

function formatParams(n: number | null): string {
  if (n == null) return "";
  if (n >= 1_000_000_000) {
    const v = n / 1_000_000_000;
    return v >= 10 ? `${Math.round(v)}B` : `${v.toFixed(1)}B`;
  }
  if (n >= 1_000_000) return `${Math.round(n / 1_000_000)}M`;
  return `${n}`;
}

function formatProteins(n: number | undefined): string {
  if (n == null) return "";
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function cellKey(eid: string, cat: string, asp: string): string {
  return `${eid}|${cat}|${asp}`;
}

/** Index rows by (embedding, cat, asp) for O(1) cell lookup. The matrix
 *  endpoint already dedupes to a single best row per tuple. */
function indexRows(rows: BenchmarkRow[]): Map<string, BenchmarkRow> {
  const out = new Map<string, BenchmarkRow>();
  for (const r of rows) {
    out.set(cellKey(r.embedding_config_id, r.category, r.aspect), r);
  }
  return out;
}

/** Index the leaderboard by (cat, asp) so the table can highlight winners. */
function indexBestPerCell(cells: BenchmarkBestCell[]): Map<string, BenchmarkBestCell> {
  const out = new Map<string, BenchmarkBestCell>();
  for (const c of cells) {
    out.set(`${c.category}|${c.aspect}`, c);
  }
  return out;
}

function stageLabel(stages: BenchmarkStage[], name: string): string {
  return stages.find((s) => s.name === name)?.label ?? name;
}

function evalSetLabel(evalSets: BenchmarkEvalSet[], id: string): string {
  return evalSets.find((e) => e.id === id)?.label ?? `${id.slice(0, 8)}…`;
}

/** Pick the initial stage once the catalog is loaded. Backend already
 *  returns stages sorted by YAML preferred_default_stages, so the first
 *  entry IS the preferred one if it has data. */
function pickDefaultStage(stages: BenchmarkStage[]): string | null {
  return stages.length > 0 ? stages[0].name : null;
}

/** CSV export of the currently filtered rows — one line per cell. */
function rowsToCsv(
  embeddings: BenchmarkEmbedding[],
  rows: BenchmarkRow[],
  stage: string,
): string {
  const embById = new Map(embeddings.map((e) => [e.id, e]));
  const header = [
    "display_name",
    "family",
    "param_count",
    "model_name",
    "stage",
    "category",
    "aspect",
    "fmax",
    "precision",
    "recall",
    "coverage",
    "n_proteins",
    "evaluation_set_id",
    "evaluation_result_id",
  ].join(",");
  const lines = [header];
  for (const r of rows) {
    if (r.stage !== stage) continue;
    const e = embById.get(r.embedding_config_id);
    lines.push(
      [
        e?.display_name ?? "",
        e?.family ?? "",
        e?.param_count ?? "",
        e?.model_name ?? "",
        r.stage,
        r.category,
        r.aspect,
        r.fmax,
        r.precision ?? "",
        r.recall ?? "",
        r.coverage ?? "",
        r.n_proteins ?? "",
        r.evaluation_set_id,
        r.evaluation_result_id,
      ]
        .map((v) => {
          const s = String(v);
          if (/[,"\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
          return s;
        })
        .join(","),
    );
  }
  return lines.join("\n");
}

function downloadCsv(filename: string, content: string): void {
  const blob = new Blob([content], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Page ─────────────────────────────────────────────────────────────────

export default function BenchmarkPage() {
  const [embeddings, setEmbeddings] = useState<BenchmarkEmbedding[] | null>(null);
  const [matrix, setMatrix] = useState<BenchmarkMatrixResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [stage, setStage] = useState<string | null>(null);
  const [evalSetId, setEvalSetId] = useState<string | "all">("all");
  const [selectedK, setSelectedK] = useState<number | null>(null);

  // Unfiltered catalog fetch — populates the full set of known stages and
  // eval sets, so selector chips don't disappear when a filtered query
  // returns zero rows.
  const [catalog, setCatalog] = useState<{
    stages: BenchmarkStage[];
    evalSets: BenchmarkEvalSet[];
    categories: string[];
    aspects: string[];
    ks: number[];
  }>({ stages: [], evalSets: [], categories: [], aspects: [], ks: [] });

  useEffect(() => {
    getBenchmarkMatrix()
      .then((m) => {
        setCatalog({
          stages: m.stages,
          evalSets: m.evaluation_sets,
          categories: m.categories,
          aspects: m.aspects,
          ks: m.ks ?? [],
        });
        setStage((prev) => prev ?? pickDefaultStage(m.stages));
        setSelectedK((prev) => prev ?? (m.ks?.[0] ?? null));
      })
      .catch((e) => setError(e.message));
  }, []);

  useEffect(() => {
    if (stage === null) return;
    setError(null);
    Promise.all([
      getBenchmarkEmbeddings(),
      getBenchmarkMatrix({
        stage,
        evaluation_set_id: evalSetId === "all" ? undefined : evalSetId,
        k: selectedK ?? undefined,
      }),
    ])
      .then(([e, m]) => {
        setEmbeddings(e.embeddings);
        setMatrix(m);
      })
      .catch((e) => setError(e.message));
  }, [stage, evalSetId, selectedK]);

  const rowIndex = useMemo(
    () => (matrix ? indexRows(matrix.rows) : new Map<string, BenchmarkRow>()),
    [matrix],
  );

  const bestPerCell = useMemo(
    () => (matrix ? indexBestPerCell(matrix.best_per_cell) : new Map<string, BenchmarkBestCell>()),
    [matrix],
  );

  const bestPerCellGlobal = useMemo(
    () =>
      matrix
        ? indexBestPerCell(matrix.best_per_cell_global ?? [])
        : new Map<string, BenchmarkBestCell>(),
    [matrix],
  );

  const embeddingsWithData = useMemo(() => {
    if (!embeddings || !matrix) return new Set<string>();
    return new Set(matrix.embedding_config_ids);
  }, [embeddings, matrix]);

  if (error) {
    return (
      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-12">
        <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center">
          <p className="text-red-800 text-sm">{error}</p>
        </div>
      </div>
    );
  }

  if (!embeddings || !matrix || stage === null) {
    return (
      <div className="max-w-6xl mx-auto px-4 sm:px-6 py-12 space-y-8">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-96 rounded-lg" />
      </div>
    );
  }

  const hasData = matrix.rows.length > 0;
  const stageList = catalog.stages.length > 0 ? catalog.stages : matrix.stages;
  const evalSetList = catalog.evalSets.length > 0 ? catalog.evalSets : matrix.evaluation_sets;
  const categories = catalog.categories.length > 0 ? catalog.categories : matrix.categories;
  const aspects = catalog.aspects.length > 0 ? catalog.aspects : matrix.aspects;
  const currentStageLabel = stageLabel(stageList, stage);

  // Active eval set banner: when "all" is selected and there's only one set,
  // show that one; when a specific one is selected, show its full metadata.
  const activeEvalSet =
    evalSetId !== "all"
      ? evalSetList.find((e) => e.id === evalSetId) ?? null
      : evalSetList.length === 1
        ? evalSetList[0]
        : null;

  return (
    <div className="max-w-6xl mx-auto px-4 sm:px-6 py-8 space-y-6">
      {/* Header */}
      <header className="flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Benchmark matrix</h1>
          <p className="text-sm text-slate-500 mt-1">
            Per-embedding Fmax across categories and aspects for every evaluation
            run in the database.{" "}
            <Link href="/" className="text-blue-600 hover:underline">
              Back to home
            </Link>
          </p>
        </div>
        <div className="flex gap-2">
          <button
            disabled={!hasData}
            onClick={() =>
              downloadCsv(
                `benchmark_${stage}.csv`,
                rowsToCsv(embeddings, matrix.rows, stage),
              )
            }
            className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            Download CSV
          </button>
        </div>
      </header>

      {/* Eval set context banner */}
      {activeEvalSet && (
        <section className="rounded-lg border border-blue-100 bg-blue-50 px-4 py-3">
          <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
            <div>
              <span className="text-xs font-semibold text-blue-700 uppercase tracking-wider">
                Evaluation split
              </span>
              <div className="text-sm font-semibold text-blue-900 mt-0.5">
                {activeEvalSet.label}
              </div>
            </div>
            <div className="flex gap-3 text-xs text-blue-800">
              {activeEvalSet.stats.delta_proteins != null && (
                <span>
                  <span className="text-blue-500">Δ</span>{" "}
                  <span className="font-mono font-semibold">
                    {activeEvalSet.stats.delta_proteins.toLocaleString()}
                  </span>{" "}
                  proteins
                </span>
              )}
              {activeEvalSet.stats.nk_proteins != null && (
                <span>
                  <span className="text-blue-500">NK</span>{" "}
                  <span className="font-mono font-semibold">
                    {formatProteins(activeEvalSet.stats.nk_proteins)}
                  </span>
                </span>
              )}
              {activeEvalSet.stats.lk_proteins != null && (
                <span>
                  <span className="text-blue-500">LK</span>{" "}
                  <span className="font-mono font-semibold">
                    {formatProteins(activeEvalSet.stats.lk_proteins)}
                  </span>
                </span>
              )}
              {activeEvalSet.stats.pk_proteins != null && (
                <span>
                  <span className="text-blue-500">PK</span>{" "}
                  <span className="font-mono font-semibold">
                    {formatProteins(activeEvalSet.stats.pk_proteins)}
                  </span>
                </span>
              )}
              {activeEvalSet.new_obo_version && (
                <span>
                  <span className="text-blue-500">OBO</span>{" "}
                  <span className="font-mono">{activeEvalSet.new_obo_version}</span>
                </span>
              )}
            </div>
          </div>
        </section>
      )}

      {/* Filters */}
      <div className="flex flex-wrap gap-3 items-end">
        <div>
          <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
            Pipeline stage
          </label>
          <div className="flex flex-wrap gap-1 rounded-lg bg-slate-100 p-0.5">
            {stageList.map((s) => (
              <button
                key={s.name}
                onClick={() => setStage(s.name)}
                title={`${s.kind}${s.is_baseline ? " · baseline" : ""}`}
                className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                  stage === s.name
                    ? "bg-white text-slate-900 shadow-sm"
                    : "text-slate-500 hover:text-slate-700"
                }`}
              >
                {s.label}
                {s.is_baseline && (
                  <span className="ml-1 text-[9px] text-slate-400 uppercase">base</span>
                )}
              </button>
            ))}
          </div>
        </div>

        {catalog.ks.length > 0 && (
          <div>
            <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Neighbours (K)
            </label>
            <div className="flex gap-1 rounded-lg bg-slate-100 p-0.5">
              {catalog.ks.map((n) => (
                <button
                  key={n}
                  onClick={() => setSelectedK(n)}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                    selectedK === n
                      ? "bg-white text-slate-900 shadow-sm"
                      : "text-slate-500 hover:text-slate-700"
                  }`}
                >
                  K={n}
                </button>
              ))}
            </div>
          </div>
        )}

        {evalSetList.length > 1 && (
          <div>
            <label className="block text-xs font-medium text-slate-500 uppercase tracking-wider mb-1">
              Evaluation set
            </label>
            <select
              value={evalSetId}
              onChange={(e) => setEvalSetId(e.target.value)}
              className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700"
            >
              <option value="all">All splits</option>
              {evalSetList.map((es) => (
                <option key={es.id} value={es.id}>
                  {es.label}
                </option>
              ))}
            </select>
          </div>
        )}

        <div className="text-xs text-slate-400 ml-auto self-end">
          {matrix.total} cells · {matrix.embedding_config_ids.length} embeddings ·{" "}
          {matrix.evaluation_sets.length} eval set
          {matrix.evaluation_sets.length === 1 ? "" : "s"}
        </div>
      </div>

      {/* Global champions: best Fmax per (cat, asp) ignoring stage/K filters.
          Stable across filter changes — anchors the absolute-best read. */}
      {matrix.best_per_cell_global && matrix.best_per_cell_global.length > 0 && (
        <section className="rounded-2xl border border-blue-100 bg-gradient-to-br from-blue-50/60 via-white to-violet-50/40 shadow-sm p-4 sm:p-5">
          <div className="flex items-baseline justify-between mb-3 flex-wrap gap-2">
            <h2 className="text-sm font-semibold text-slate-800 flex items-center gap-2">
              <span aria-hidden className="text-base">🏆</span>
              Global champions per cell
              <span className="ml-1 text-[11px] font-normal text-slate-500">
                across every stage and K · current evaluation set
              </span>
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th className="px-2 py-1 text-left font-medium text-slate-500"></th>
                  {aspects.map((asp) => (
                    <th
                      key={asp}
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide"
                    >
                      {asp}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {categories.map((cat) => (
                  <tr key={cat} className="border-t border-slate-200/60">
                    <td className="px-2 py-2.5 font-semibold text-slate-700">{cat}</td>
                    {aspects.map((asp) => {
                      const best = bestPerCellGlobal.get(`${cat}|${asp}`);
                      if (!best) {
                        return (
                          <td key={asp} className="px-2 py-2.5 text-center text-slate-300">
                            —
                          </td>
                        );
                      }
                      const emb = embeddings.find((e) => e.id === best.embedding_config_id);
                      return (
                        <td
                          key={asp}
                          className="px-2 py-2.5 text-center border-l border-slate-200/60"
                          title={`${emb?.display_name ?? best.embedding_config_id} · ${stageLabel(stageList, best.stage)} · K=${best.k}`}
                        >
                          <div className="font-bold text-lg text-slate-900 tabular-nums leading-none">
                            {best.fmax.toFixed(3)}
                          </div>
                          <div className="text-[11px] font-medium text-slate-700 truncate max-w-[140px] mx-auto mt-1">
                            {emb?.display_name ?? "—"}
                          </div>
                          <div className="text-[10px] text-slate-500 truncate max-w-[140px] mx-auto mt-0.5">
                            {stageLabel(stageList, best.stage)}
                            <span className="mx-1 text-slate-300">·</span>
                            <span className="tabular-nums">K={best.k}</span>
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Leaderboard: best Fmax per (cat, asp) across every model & stage */}
      {matrix.best_per_cell.length > 0 && (
        <section className="rounded-xl border bg-white shadow-sm p-4">
          <div className="flex items-baseline justify-between mb-3">
            <h2 className="text-sm font-semibold text-slate-800">
              Best Fmax per cell
              <span className="ml-2 text-xs font-normal text-slate-400">
                in current selection
                {stage ? ` · stage=${stageLabel(stageList, stage)}` : ""}
                {selectedK ? ` · K=${selectedK}` : ""}
              </span>
            </h2>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr>
                  <th className="px-2 py-1 text-left font-medium text-slate-500"></th>
                  {aspects.map((asp) => (
                    <th
                      key={asp}
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide"
                    >
                      {asp}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {categories.map((cat) => (
                  <tr key={cat} className="border-t">
                    <td className="px-2 py-2 font-semibold text-slate-700">{cat}</td>
                    {aspects.map((asp) => {
                      const best = bestPerCell.get(`${cat}|${asp}`);
                      if (!best) {
                        return (
                          <td
                            key={asp}
                            className="px-2 py-2 text-center text-slate-300"
                          >
                            —
                          </td>
                        );
                      }
                      const emb = embeddings.find((e) => e.id === best.embedding_config_id);
                      return (
                        <td
                          key={asp}
                          className="px-2 py-2 text-center border-l"
                          title={`${emb?.display_name ?? best.embedding_config_id} · ${stageLabel(stageList, best.stage)}`}
                        >
                          <div className="font-semibold text-slate-900 tabular-nums">
                            {best.fmax.toFixed(3)}
                          </div>
                          <div className="text-[10px] text-slate-500 truncate max-w-[120px] mx-auto">
                            {emb?.display_name ?? "—"}
                          </div>
                          <div className="text-[9px] text-slate-400 truncate max-w-[120px] mx-auto">
                            {stageLabel(stageList, best.stage)}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Matrix table */}
      {!hasData ? (
        <section className="rounded-xl border-2 border-dashed border-slate-200 bg-slate-50 p-12 text-center">
          <p className="text-slate-500 text-sm">
            No evaluation results for{" "}
            <span className="font-semibold">{currentStageLabel}</span> yet.
          </p>
          <p className="text-slate-400 text-xs mt-2">
            Run <code>run_cafa_evaluation</code> for an embedding to populate
            this cell of the matrix.
          </p>
        </section>
      ) : (
        <div className="overflow-x-auto rounded-lg border bg-white shadow-sm">
          <table className="w-full text-sm">
            <thead className="bg-slate-50">
              <tr>
                <th
                  rowSpan={2}
                  className="px-4 py-2 text-left font-medium text-slate-600 border-b sticky left-0 bg-slate-50"
                >
                  Embedding
                </th>
                {categories.map((cat) => (
                  <th
                    key={cat}
                    colSpan={aspects.length}
                    className="px-2 py-1.5 text-center font-semibold text-slate-700 border-b border-l"
                  >
                    {cat}
                  </th>
                ))}
              </tr>
              <tr>
                {categories.flatMap((cat) =>
                  aspects.map((asp) => (
                    <th
                      key={`${cat}-${asp}`}
                      className="px-2 py-1 text-center text-[10px] font-medium text-slate-500 uppercase tracking-wide border-b border-l"
                    >
                      {asp}
                    </th>
                  )),
                )}
              </tr>
            </thead>
            <tbody>
              {embeddings.map((emb) => {
                const hasRow = embeddingsWithData.has(emb.id);
                return (
                  <tr
                    key={emb.id}
                    className={`border-t ${hasRow ? "" : "opacity-50"}`}
                  >
                    <td className="px-4 py-2 sticky left-0 bg-white border-r">
                      <div className="font-medium text-slate-900">
                        {emb.display_name}
                      </div>
                      <div className="text-[10px] text-slate-400 font-mono">
                        {emb.family}
                        {emb.param_count != null
                          ? ` · ${formatParams(emb.param_count)}`
                          : ""}
                      </div>
                    </td>
                    {categories.flatMap((cat) =>
                      aspects.map((asp) => {
                        const row = rowIndex.get(cellKey(emb.id, cat, asp));
                        const best = bestPerCell.get(`${cat}|${asp}`);
                        const isWinner =
                          row && best && row.evaluation_result_id === best.evaluation_result_id;
                        return (
                          <td
                            key={`${emb.id}-${cat}-${asp}`}
                            className={`px-2 py-2 text-center tabular-nums border-l ${
                              isWinner ? "bg-green-50" : ""
                            }`}
                            title={
                              row
                                ? `precision=${row.precision ?? "—"} recall=${row.recall ?? "—"} (eval_result=${row.evaluation_result_id.slice(0, 8)}…)`
                                : "no data"
                            }
                          >
                            {row ? (
                              <span
                                className={`font-semibold ${
                                  isWinner ? "text-green-700" : "text-slate-900"
                                }`}
                              >
                                {row.fmax.toFixed(3)}
                              </span>
                            ) : (
                              <span className="text-slate-300">—</span>
                            )}
                          </td>
                        );
                      }),
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      <p className="text-xs text-slate-400">
        Display names and stage labels come from{" "}
        <code>embedding_config</code> (DB) and{" "}
        <code>protea/config/benchmark.yaml</code>. Edit the YAML to change
        ordering, labels, or the baseline tag.
      </p>
    </div>
  );
}
