// Exporting the benchmark table as CSV.
//
// Split out of the page for the same reason as the helpers: it is a pure
// transformation of rows into text, plus one browser call to hand the file
// over, and neither needs to sit inside a 1,100-line component.
//
// The column set is deliberately wide. A reader who exports a leaderboard and
// finds only the headline number cannot tell a macro figure from a micro one,
// nor an IA-weighted cell from a plain one, so every metric travels with the
// row that produced it.

import type { BenchmarkEmbedding, BenchmarkRow } from "@/lib/api";

export function rowsToCsv(
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
    "primary",
    "primary_metric",
    "f_micro_w",
    "fmax",
    "precision",
    "recall",
    "coverage",
    "n_proteins",
    "frame",
    "temporal_window",
    "arms_enabled",
    "leakage_role",
    "prediction_set_status",
    "self_hit_rate",
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
        r.primary,
        r.primary_metric,
        r.f_micro_w ?? "",
        r.fmax,
        r.precision ?? "",
        r.recall ?? "",
        r.coverage ?? "",
        r.n_proteins ?? "",
        r.frame ?? "",
        r.temporal_window ?? "",
        r.arms_enabled
          ? Object.entries(r.arms_enabled)
              .filter(([, on]) => on)
              .map(([k]) => k)
              .join("+")
          : "",
        r.leakage_role ?? "",
        r.prediction_set_status ?? "",
        r.self_hit_rate ?? "",
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

export function downloadCsv(filename: string, content: string): void {
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
