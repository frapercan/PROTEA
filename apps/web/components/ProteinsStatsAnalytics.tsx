"use client";

import { useEffect, useRef, useState } from "react";
import {
  getEmbeddingsByPlm,
  getGoMatrix,
  getTaxonomyTop,
  getSequenceLength,
  getGoDensity,
  getPipelineActivity,
  getDatasetRegistry,
  type EmbeddingsByPlm,
  type GoMatrix,
  type TaxonomyTop,
  type SequenceLength,
  type GoDensity,
  type PipelineActivity,
  type DatasetRegistry,
} from "@/lib/api";
import { HelpDot } from "@/components/Tooltip";

// ── Card shell + lazy-load hook ─────────────────────────────────────────────

function useLazyLoad<T>(load: () => Promise<T>): {
  ref: React.MutableRefObject<HTMLDivElement | null>;
  data: T | null;
  error: string | null;
  loading: boolean;
} {
  const ref = useRef<HTMLDivElement | null>(null);
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const triggered = useRef(false);

  useEffect(() => {
    const el = ref.current;
    if (el == null) return;
    if (triggered.current) return;

    function fire() {
      if (triggered.current) return;
      triggered.current = true;
      // Schedule state updates after the effect commit so eslint's
      // react-hooks/set-state-in-effect rule stays happy. Same behaviour
      // for the user (load fires immediately) without the cascading
      // synchronous render the rule warns about.
      queueMicrotask(() => setLoading(true));
      load()
        .then((d) => { setData(d); setLoading(false); })
        .catch((err) => { setError(String(err?.message ?? err)); setLoading(false); });
    }

    // SSR + jsdom safety: when IntersectionObserver is missing, just
    // schedule the fetch on the microtask queue so we still escape the
    // synchronous render.
    if (typeof window === "undefined" || !("IntersectionObserver" in window)) {
      queueMicrotask(fire);
      return;
    }
    const obs = new IntersectionObserver((entries) => {
      for (const e of entries) {
        if (e.isIntersecting && !triggered.current) {
          fire();
          obs.disconnect();
          break;
        }
      }
    }, { rootMargin: "200px" });
    obs.observe(el);
    return () => obs.disconnect();
  }, [load]);

  return { ref, data, error, loading };
}

function AnalyticCard({
  title,
  tooltip,
  children,
  forwardedRef,
}: {
  title: string;
  tooltip: string;
  children: React.ReactNode;
  forwardedRef: React.MutableRefObject<HTMLDivElement | null>;
}) {
  return (
    <div
      ref={forwardedRef}
      className="rounded-lg border border-stone-200 bg-white p-4 shadow-sm"
    >
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-stone-800">
          {title}
          <HelpDot text={tooltip} />
        </h3>
      </div>
      {children}
    </div>
  );
}

function CardSkeleton({ rows = 4 }: { rows?: number }) {
  return (
    <div className="space-y-2 animate-pulse">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="h-4 rounded bg-stone-200" />
      ))}
    </div>
  );
}

function CardError({ message }: { message: string }) {
  return (
    <p className="text-xs text-rose-600">Failed to load: {message}</p>
  );
}

function fmtInt(n: number | null | undefined): string {
  if (n == null) return "—";
  return n.toLocaleString();
}

// ── 1. Embeddings coverage per PLM ──────────────────────────────────────────

function EmbeddingsByPlmCard() {
  const { ref, data, error, loading } = useLazyLoad<EmbeddingsByPlm>(getEmbeddingsByPlm);
  return (
    <AnalyticCard
      title="Embedding coverage per PLM"
      tooltip="Distinct canonical Swiss-Prot proteins that have at least one stored embedding per protein language model. Percent is over the canonical reviewed population. Recomputed every 10 minutes."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={6} />}
      {error && <CardError message={error} />}
      {data && data.items.length === 0 && (
        <p className="text-xs text-stone-500">No embedding configs with stored vectors yet.</p>
      )}
      {data && data.items.length > 0 && (
        <table className="w-full text-xs">
          <thead className="text-stone-500">
            <tr>
              <th className="text-left font-medium">Model</th>
              <th className="text-right font-medium">Proteins</th>
              <th className="text-right font-medium">Coverage</th>
              <th className="pl-3" />
            </tr>
          </thead>
          <tbody>
            {data.items.map((it) => (
              <tr key={it.embedding_config_id} className="border-t border-stone-100">
                <td className="py-1.5 font-mono text-stone-700">{it.display_name || it.model_name}</td>
                <td className="py-1.5 text-right tabular-nums text-stone-700">{fmtInt(it.protein_count)}</td>
                <td className="py-1.5 text-right tabular-nums text-stone-700">{it.coverage_pct.toFixed(1)}%</td>
                <td className="py-1.5 pl-3 w-24">
                  <div className="h-2 rounded bg-stone-100 overflow-hidden">
                    <div
                      className="h-full bg-blue-500"
                      style={{ width: `${Math.min(100, Math.max(0, it.coverage_pct))}%` }}
                    />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </AnalyticCard>
  );
}

// ── 2. GO annotation matrix ────────────────────────────────────────────────

function GoMatrixCard() {
  const { ref, data, error, loading } = useLazyLoad<GoMatrix>(getGoMatrix);
  return (
    <AnalyticCard
      title="GO annotations: source × version × aspect × evidence"
      tooltip="GROUP BY of every (annotation_set source, source_version, GO aspect, experimental/non-experimental) tuple. Experimental = evidence_code in {EXP, IDA, IMP, IGI, IEP, IPI}. Recomputed every 10 minutes."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={8} />}
      {error && <CardError message={error} />}
      {data && data.items.length === 0 && (
        <p className="text-xs text-stone-500">No annotations loaded yet.</p>
      )}
      {data && data.items.length > 0 && (
        <div className="max-h-64 overflow-y-auto">
          <table className="w-full text-xs">
            <thead className="text-stone-500 sticky top-0 bg-white">
              <tr>
                <th className="text-left font-medium">Source</th>
                <th className="text-left font-medium">Version</th>
                <th className="text-left font-medium">Aspect</th>
                <th className="text-left font-medium">Evidence</th>
                <th className="text-right font-medium">Count</th>
              </tr>
            </thead>
            <tbody>
              {data.items.map((it, i) => (
                <tr key={i} className="border-t border-stone-100">
                  <td className="py-1.5 text-stone-700">{it.source}</td>
                  <td className="py-1.5 font-mono text-stone-600">{it.source_version ?? "—"}</td>
                  <td className="py-1.5 text-stone-700">{it.aspect ?? "—"}</td>
                  <td className="py-1.5">
                    <span className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${
                      it.experimental ? "bg-emerald-50 text-emerald-700" : "bg-stone-100 text-stone-600"
                    }`}>
                      {it.experimental ? "experimental" : "other"}
                    </span>
                  </td>
                  <td className="py-1.5 text-right tabular-nums text-stone-700">{fmtInt(it.count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </AnalyticCard>
  );
}

// ── 3. Taxonomy top-20 ─────────────────────────────────────────────────────

function TaxonomyTopCard() {
  const { ref, data, error, loading } = useLazyLoad<TaxonomyTop>(getTaxonomyTop);
  const max = data ? Math.max(1, ...data.items.map((i) => i.protein_count)) : 1;
  return (
    <AnalyticCard
      title="Top 20 organisms"
      tooltip="Top 20 organisms ranked by canonical-protein count (isoforms excluded so the bar reflects unique gene-products). Cached 60 minutes; organism mix rarely shifts between ingests."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={8} />}
      {error && <CardError message={error} />}
      {data && data.items.length === 0 && (
        <p className="text-xs text-stone-500">No organism data yet.</p>
      )}
      {data && data.items.length > 0 && (
        <div className="space-y-1">
          {data.items.map((it) => (
            <div key={`${it.taxonomy_id}-${it.organism}`} className="flex items-center gap-2">
              <span className="w-44 truncate text-xs text-stone-700" title={it.organism}>{it.organism}</span>
              <div className="flex-1 h-3 rounded bg-stone-100 overflow-hidden">
                <div
                  className="h-full bg-violet-500"
                  style={{ width: `${(100 * it.protein_count) / max}%` }}
                />
              </div>
              <span className="w-16 text-right text-xs tabular-nums text-stone-600">{fmtInt(it.protein_count)}</span>
            </div>
          ))}
        </div>
      )}
    </AnalyticCard>
  );
}

// ── 4. Sequence length distribution ────────────────────────────────────────

function SequenceLengthCard() {
  const { ref, data, error, loading } = useLazyLoad<SequenceLength>(getSequenceLength);
  return (
    <AnalyticCard
      title="Sequence length distribution"
      tooltip="Canonical protein length percentiles (min, p10, p50, p90, p99, max) plus a 50-bin histogram. Computed via Postgres percentile_cont and width_bucket; cached 60 minutes."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={6} />}
      {error && <CardError message={error} />}
      {data && data.count === 0 && (
        <p className="text-xs text-stone-500">No length data yet.</p>
      )}
      {data && data.count > 0 && (
        <div className="space-y-3">
          <div className="grid grid-cols-6 gap-2 text-center text-xs">
            {(["min", "p10", "p50", "p90", "p99", "max"] as const).map((k) => (
              <div key={k}>
                <p className="text-[10px] uppercase tracking-wide text-stone-500">{k}</p>
                <p className="tabular-nums font-semibold text-stone-800">
                  {fmtInt(Math.round((data.percentiles[k] as number) ?? 0))}
                </p>
              </div>
            ))}
          </div>
          <SequenceLengthHistogram bins={data.bins} />
        </div>
      )}
    </AnalyticCard>
  );
}

function SequenceLengthHistogram({ bins }: { bins: SequenceLength["bins"] }) {
  if (bins.length === 0) return null;
  const maxCount = Math.max(...bins.map((b) => b.count));
  const w = 320;
  const h = 80;
  const barW = w / bins.length;
  return (
    <svg viewBox={`0 0 ${w} ${h + 16}`} className="w-full max-w-full">
      {bins.map((b, i) => {
        const barH = maxCount > 0 ? (b.count / maxCount) * h : 0;
        return (
          <g key={i}>
            <rect
              x={i * barW}
              y={h - barH}
              width={Math.max(0.5, barW - 1)}
              height={barH}
              fill="#0ea5e9"
              opacity={0.75}
            >
              <title>{`${Math.round(b.lo)}-${Math.round(b.hi)} aa: ${b.count.toLocaleString()}`}</title>
            </rect>
          </g>
        );
      })}
      <line x1={0} y1={h} x2={w} y2={h} stroke="#d6d3d1" strokeWidth={1} />
      <text x={0} y={h + 12} fontSize="9" fill="#78716c">{Math.round(bins[0].lo)}</text>
      <text x={w} y={h + 12} fontSize="9" fill="#78716c" textAnchor="end">{Math.round(bins[bins.length - 1].hi)} aa</text>
    </svg>
  );
}

// ── 5. GO density per aspect ───────────────────────────────────────────────

function GoDensityCard() {
  const { ref, data, error, loading } = useLazyLoad<GoDensity>(getGoDensity);
  return (
    <AnalyticCard
      title="GO term density per protein per aspect"
      tooltip="Per-aspect (BPO/MFO/CCO) average and percentile of GO terms-per-protein. Computed from ProteinGOAnnotation grouped by (aspect, protein). Cached 10 minutes."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={4} />}
      {error && <CardError message={error} />}
      {data && data.items.length === 0 && (
        <p className="text-xs text-stone-500">No annotation density yet.</p>
      )}
      {data && data.items.length > 0 && (
        <table className="w-full text-xs">
          <thead className="text-stone-500">
            <tr>
              <th className="text-left font-medium">Aspect</th>
              <th className="text-right font-medium">Proteins</th>
              <th className="text-right font-medium">Avg</th>
              <th className="text-right font-medium">p50</th>
              <th className="text-right font-medium">p90</th>
              <th className="text-right font-medium">Max</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((it) => (
              <tr key={it.aspect} className="border-t border-stone-100">
                <td className="py-1.5 font-medium text-stone-700">{it.aspect}</td>
                <td className="py-1.5 text-right tabular-nums">{fmtInt(it.protein_count)}</td>
                <td className="py-1.5 text-right tabular-nums">{it.avg.toFixed(2)}</td>
                <td className="py-1.5 text-right tabular-nums">{it.p50.toFixed(1)}</td>
                <td className="py-1.5 text-right tabular-nums">{it.p90.toFixed(1)}</td>
                <td className="py-1.5 text-right tabular-nums">{fmtInt(it.max)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </AnalyticCard>
  );
}

// ── 6. Pipeline activity ───────────────────────────────────────────────────

function PipelineActivityCard() {
  const { ref, data, error, loading } = useLazyLoad<PipelineActivity>(getPipelineActivity);
  return (
    <AnalyticCard
      title={`Pipeline activity (last ${data?.window_days ?? 30} days)`}
      tooltip="Job counts grouped by operation and day_truncated(created_at). The last_24h block isolates today's run rate. Cached 60 seconds; this view is intentionally fresh."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={4} />}
      {error && <CardError message={error} />}
      {data && (
        <>
          <p className="mb-2 text-[10px] uppercase tracking-wide text-stone-500">Last 24h</p>
          <div className="mb-3 flex flex-wrap gap-1.5">
            {data.last_24h.length === 0 && (
              <span className="text-xs text-stone-500">No jobs in the last 24h.</span>
            )}
            {data.last_24h.map((it) => (
              <span key={it.operation} className="rounded-full bg-amber-50 px-2 py-0.5 text-[11px] text-amber-700 border border-amber-100">
                {it.operation}: <span className="font-semibold">{it.count}</span>
              </span>
            ))}
          </div>
          <PipelineSparkline items={data.items} />
        </>
      )}
    </AnalyticCard>
  );
}

function PipelineSparkline({ items }: { items: PipelineActivity["items"] }) {
  if (items.length === 0) {
    return <p className="text-xs text-stone-500">No activity in the window.</p>;
  }
  // Stack per-day totals across operations.
  const byDay = new Map<string, number>();
  for (const it of items) {
    if (it.day == null) continue;
    byDay.set(it.day, (byDay.get(it.day) ?? 0) + it.count);
  }
  const days = Array.from(byDay.entries()).sort();
  const max = Math.max(1, ...days.map(([, c]) => c));
  const w = 320;
  const h = 60;
  const points = days.map(([, c], i) => {
    const x = (i / Math.max(1, days.length - 1)) * w;
    const y = h - (c / max) * h;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  return (
    <svg viewBox={`0 0 ${w} ${h + 14}`} className="w-full max-w-full">
      <polyline
        fill="none"
        stroke="#10b981"
        strokeWidth={1.5}
        points={points.join(" ")}
      />
      <line x1={0} y1={h} x2={w} y2={h} stroke="#d6d3d1" strokeWidth={1} />
      {days.length > 0 && (
        <>
          <text x={0} y={h + 12} fontSize="9" fill="#78716c">{days[0][0].slice(0, 10)}</text>
          <text x={w} y={h + 12} fontSize="9" fill="#78716c" textAnchor="end">{days[days.length - 1][0].slice(0, 10)}</text>
        </>
      )}
    </svg>
  );
}

// ── 7. Dataset registry ────────────────────────────────────────────────────

function DatasetRegistryCard() {
  const { ref, data, error, loading } = useLazyLoad<DatasetRegistry>(getDatasetRegistry);
  return (
    <AnalyticCard
      title="Dataset registry"
      tooltip="Frozen reranker datasets grouped by neighbours K and (eval_snapshot_pair). Train/eval row counts are summed across all registered dumps in the cell. Cached 5 minutes."
      forwardedRef={ref}
    >
      {loading && <CardSkeleton rows={4} />}
      {error && <CardError message={error} />}
      {data && data.items.length === 0 && (
        <p className="text-xs text-stone-500">No registered datasets yet.</p>
      )}
      {data && data.items.length > 0 && (
        <table className="w-full text-xs">
          <thead className="text-stone-500">
            <tr>
              <th className="text-left font-medium">K</th>
              <th className="text-left font-medium">Eval pair</th>
              <th className="text-right font-medium">Datasets</th>
              <th className="text-right font-medium">Train rows</th>
              <th className="text-right font-medium">Eval rows</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((it, i) => (
              <tr key={i} className="border-t border-stone-100">
                <td className="py-1.5 font-mono text-stone-700">{it.k ?? "—"}</td>
                <td className="py-1.5 font-mono text-stone-600">{it.eval_snapshot_pair ?? "—"}</td>
                <td className="py-1.5 text-right tabular-nums">{fmtInt(it.dataset_count)}</td>
                <td className="py-1.5 text-right tabular-nums">{fmtInt(it.n_train_rows)}</td>
                <td className="py-1.5 text-right tabular-nums">{fmtInt(it.n_eval_rows)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </AnalyticCard>
  );
}

// ── Public composition ─────────────────────────────────────────────────────

export function ProteinsStatsAnalytics() {
  return (
    <div className="space-y-4">
      <EmbeddingsByPlmCard />
      <GoMatrixCard />
      <TaxonomyTopCard />
      <SequenceLengthCard />
      <GoDensityCard />
      <PipelineActivityCard />
      <DatasetRegistryCard />
    </div>
  );
}
