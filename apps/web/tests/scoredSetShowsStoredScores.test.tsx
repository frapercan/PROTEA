// A scored set's rows carry the score it stored, and the table shows that one.
//
// The stored score is the one the evaluator reads. The page used to recompute a
// score in the browser from a configuration's weights, a different number under
// the same column; a candidate set is now scored only by its job
// (score_prediction_set), so its rows carry no score and the table shows none.

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
import type { Prediction } from "@/lib/api";

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

describe("a scored set", () => {
  it("shows the score it stored", () => {
    render(<PredictionTable preds={[pred({ score: 0.123 })]} knownByGoId={new Map()} />);
    expect(screen.getAllByText("0.123").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Score").length).toBeGreaterThan(0);
  });

  it("orders its rows by the stored score, highest first", () => {
    render(
      <PredictionTable
        preds={[pred({ go_id: "GO:0000001", score: 0.2 }), pred({ go_id: "GO:0000002", score: 0.8 })]}
        knownByGoId={new Map()}
      />,
    );
    const html = document.body.innerHTML;
    expect(html.indexOf("GO:0000002")).toBeLessThan(html.indexOf("GO:0000001"));
  });
});

describe("a candidate set", () => {
  it("shows no score, since it is scored by its job and never in the browser", () => {
    render(
      <PredictionTable
        preds={[pred({ go_id: "GO:0000001", distance: 0.4 }), pred({ go_id: "GO:0000002", distance: 0.1 })]}
        knownByGoId={new Map()}
      />,
    );
    expect(screen.queryAllByText("Score")).toHaveLength(0);
    // Nearest donor first.
    const html = document.body.innerHTML;
    expect(html.indexOf("GO:0000002")).toBeLessThan(html.indexOf("GO:0000001"));
  });
});
