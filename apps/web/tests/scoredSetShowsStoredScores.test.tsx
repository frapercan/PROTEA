// A scored set's rows carry the score it stored, and the table shows that one.
//
// The stored score is the one the evaluator reads. A score recomputed in the
// browser from a configuration's weights is a different number under the same
// column, so when a row carries a stored score the table shows it and orders by
// it, whatever configuration is selected.

import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  useTranslations: () => {
    const t = (key: string) => key;
    (t as unknown as { rich: unknown }).rich = (key: string) => key;
    return t;
  },
}));

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children?: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

import { PredictionTable } from "@/app/[locale]/instrument/functional-annotation/[id]/page";
import type { Prediction, ScoringConfig } from "@/lib/api";

function pred(over: Partial<Prediction> = {}): Prediction {
  return {
    go_id: "GO:0003674",
    name: "molecular_function",
    aspect: "F",
    distance: 0.2,
    ref_protein_accession: "P12345",
    qualifier: null,
    evidence_code: "IDA",
    identity_nw: null,
    similarity_nw: null,
    alignment_score_nw: null,
    gaps_pct_nw: null,
    alignment_length_nw: null,
    identity_sw: null,
    similarity_sw: null,
    alignment_score_sw: null,
    gaps_pct_sw: null,
    alignment_length_sw: null,
    ...over,
  } as Prediction;
}

const CONFIG: ScoringConfig = {
  id: "c1",
  name: "embedding only",
  formula: "linear",
  weights: { embedding_similarity: 1.0 },
  evidence_weights: null,
  created_at: "2026-11-21T00:00:00Z",
};

describe("a scored set", () => {
  it("shows the score it stored rather than one computed in the browser", () => {
    render(
      <PredictionTable
        preds={[pred({ score: 0.123 })]}
        knownByGoId={new Map()}
        scoringConfig={CONFIG}
      />,
    );
    expect(screen.getAllByText("0.123").length).toBeGreaterThan(0);
    // The browser would compute 1 - 0.2 / 2 = 0.900 from the same row.
    expect(screen.queryAllByText("0.900")).toHaveLength(0);
  });

  it("orders its rows by the stored score, highest first", () => {
    render(
      <PredictionTable
        preds={[pred({ go_id: "GO:0000001", score: 0.2 }), pred({ go_id: "GO:0000002", score: 0.8 })]}
        knownByGoId={new Map()}
        scoringConfig={undefined}
      />,
    );
    const html = document.body.innerHTML;
    expect(html.indexOf("GO:0000002")).toBeLessThan(html.indexOf("GO:0000001"));
  });
});
