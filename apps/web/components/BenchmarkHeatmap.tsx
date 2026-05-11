"use client";

import { useMemo } from "react";
import type { BenchmarkEmbedding, BenchmarkRow } from "@/lib/api";

/**
 * Small-multiples heatmap for the benchmark matrix.
 *
 * Layout: a (categories × aspects) grid of compact cards. Inside each
 * card, one horizontal bar per embedding, sorted by Fmax descending.
 * Bar width is proportional to Fmax; color reads on a fixed gradient
 * so the eye can scan rank quickly. The leader gets a subtle medal.
 *
 * The space marked CI is reserved — when bootstrap CIs are persisted
 * we'll render a `±` whisker without changing the cell layout.
 */
export type BenchmarkHeatmapProps = {
  rows: BenchmarkRow[];
  embeddings: BenchmarkEmbedding[];
  categories: string[];
  aspects: string[];
  /** Optional: hide rows whose embedding isn't in this set. */
  embeddingFilter?: Set<string> | null;
};

const ASPECT_TONE: Record<string, { ring: string; bg: string; text: string }> = {
  MFO: { ring: "ring-blue-100",    bg: "bg-blue-50/60",    text: "text-blue-700"    },
  BPO: { ring: "ring-violet-100",  bg: "bg-violet-50/60",  text: "text-violet-700"  },
  CCO: { ring: "ring-emerald-100", bg: "bg-emerald-50/60", text: "text-emerald-700" },
};

const ASPECT_LABELS: Record<string, string> = {
  MFO: "Molecular Function",
  BPO: "Biological Process",
  CCO: "Cellular Component",
};

const CATEGORY_LABELS: Record<string, string> = {
  NK: "No Knowledge",
  LK: "Limited Knowledge",
  PK: "Partial Knowledge",
};

/** Pick the best Fmax row per embedding inside a single cell. The matrix
 *  endpoint already dedupes per (eid, esid, st, k, cat, asp) — collapse
 *  further to one row per embedding for the visualization. */
function bestRowsByEmbedding(rows: BenchmarkRow[]): BenchmarkRow[] {
  const best = new Map<string, BenchmarkRow>();
  for (const r of rows) {
    const cur = best.get(r.embedding_config_id);
    if (cur == null || r.fmax > cur.fmax) best.set(r.embedding_config_id, r);
  }
  return Array.from(best.values()).sort((a, b) => b.fmax - a.fmax);
}

/** Linear interpolation between two HSL colors. Returns a CSS color. */
function fmaxToColor(fmax: number): string {
  // 0 → cool blue, 1 → deep violet (perceptually rising)
  const t = Math.max(0, Math.min(1, fmax));
  const hue = 220 - 50 * t;        // 220 (blue) → 270 (violet)
  const sat = 65 + 20 * t;          // 65 → 85
  const light = 70 - 22 * t;        // 70 → 48 (darker = better)
  return `hsl(${hue}, ${sat}%, ${light}%)`;
}

function HeatmapCell({
  cat,
  asp,
  rows,
  embeddings,
}: {
  cat: string;
  asp: string;
  rows: BenchmarkRow[];
  embeddings: BenchmarkEmbedding[];
}) {
  const tone = ASPECT_TONE[asp] ?? ASPECT_TONE.MFO;
  const empty = rows.length === 0;

  return (
    <div className={`rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden`}>
      <div className={`flex items-baseline justify-between px-3 py-2 ring-1 ring-inset ${tone.ring} ${tone.bg}`}>
        <div className="flex items-baseline gap-2">
          <span className="text-[11px] font-bold tracking-[0.12em] text-slate-700">{cat}</span>
          <span className="text-slate-300">·</span>
          <span className={`text-[11px] font-bold tracking-[0.12em] ${tone.text}`}>{asp}</span>
        </div>
        <span className="text-[10px] text-slate-600 hidden sm:inline">
          {ASPECT_LABELS[asp] ?? ""}
        </span>
      </div>
      <div className="p-3">
        {empty ? (
          <p className="text-[12px] text-slate-600 text-center py-6">No data</p>
        ) : (
          <ul className="space-y-1.5">
            {rows.map((r, i) => {
              const emb = embeddings.find((e) => e.id === r.embedding_config_id);
              const name = emb?.display_name ?? r.embedding_config_id.slice(0, 6);
              const isWinner = i === 0;
              return (
                <li
                  key={r.embedding_config_id}
                  className="grid grid-cols-[7rem_1fr_3rem] items-center gap-2 group"
                  title={`${name} · ${r.stage} · K=${r.k} · ${r.fmax.toFixed(3)}`}
                >
                  <div className="flex items-center gap-1 min-w-0">
                    {isWinner && (
                      <span aria-label="leader" className="text-[10px]">🥇</span>
                    )}
                    <span className="text-[11px] font-medium text-slate-700 truncate">{name}</span>
                  </div>
                  <div className="relative h-3 rounded-full bg-slate-100 overflow-hidden">
                    <div
                      className="absolute inset-y-0 left-0 rounded-full transition-[width] duration-300"
                      style={{
                        width: `${Math.max(2, r.fmax * 100)}%`,
                        background: fmaxToColor(r.fmax),
                      }}
                    />
                    {/* Reserved slot for bootstrap CI whisker — see roadmap */}
                  </div>
                  <span className="text-[11px] tabular-nums font-semibold text-slate-700 text-right">
                    {r.fmax.toFixed(3)}
                  </span>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </div>
  );
}

export function BenchmarkHeatmap({
  rows,
  embeddings,
  categories,
  aspects,
  embeddingFilter,
}: BenchmarkHeatmapProps) {
  const grid = useMemo(() => {
    const map = new Map<string, BenchmarkRow[]>();
    for (const r of rows) {
      if (embeddingFilter && !embeddingFilter.has(r.embedding_config_id)) continue;
      const k = `${r.category}|${r.aspect}`;
      const arr = map.get(k);
      if (arr) arr.push(r);
      else map.set(k, [r]);
    }
    return map;
  }, [rows, embeddingFilter]);

  const aspectsCount = aspects.length;
  const cols =
    aspectsCount === 3 ? "lg:grid-cols-3" : aspectsCount === 2 ? "lg:grid-cols-2" : "lg:grid-cols-1";

  return (
    <section className="space-y-3">
      <header className="flex items-baseline justify-between flex-wrap gap-2">
        <div>
          <h2 className="text-sm font-semibold text-slate-800 flex items-center gap-2">
            <span aria-hidden>📊</span>
            Per-cell heatmap · 8 PLMs × {categories.length} categories × {aspectsCount} aspects
          </h2>
          <p className="text-xs text-slate-500 mt-0.5">
            Each card shows the best Fmax per embedding within the active selection. Bars sorted descending; leader marked.
          </p>
        </div>
        <div className="flex items-center gap-2 text-[10px] text-slate-500">
          <span className="font-semibold uppercase tracking-wider">Fmax</span>
          <span className="inline-block h-2.5 w-32 rounded-full" style={{
            background: "linear-gradient(to right, hsl(220,65%,70%), hsl(245,75%,58%), hsl(270,85%,48%))",
          }} />
          <span className="tabular-nums">0.0</span>
          <span className="opacity-50">→</span>
          <span className="tabular-nums">1.0</span>
        </div>
      </header>
      <div className={`grid grid-cols-1 sm:grid-cols-2 ${cols} gap-3`}>
        {categories.map((cat) =>
          aspects.map((asp) => (
            <HeatmapCell
              key={`${cat}|${asp}`}
              cat={cat}
              asp={asp}
              rows={bestRowsByEmbedding(grid.get(`${cat}|${asp}`) ?? [])}
              embeddings={embeddings}
            />
          )),
        )}
      </div>
      <p className="text-[10px] text-slate-600 italic">
        Hover any bar for stage / K / Fmax detail. CI whiskers will render
        in the same slot once bootstrap intervals are persisted.
      </p>
    </section>
  );
}
