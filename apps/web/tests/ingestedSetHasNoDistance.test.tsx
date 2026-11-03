// A set with no retrieval has no distance, and the page must still draw it.
//
// Reported on 2026-10-28 while looking at prediction set ac1d4f00:
// "can't access property "toFixed", e.distance is null ... y aparte, sin knn
// parece que no va no?". It did not. That set was written by
// ingest_method_predictions from a TSV, so there is no donor and no
// neighbourhood: measured, 0 of its 12.159.300 rows carry a distance, against
// 607.788 of 607.788 on the retrieval arm beside it.
//
// The root cause was in the type, which is why nothing warned: `Prediction`
// declared `distance: number` while every sibling optional field was
// `number | null`. With the type corrected, `tsc` reports
// "TS18047: 'pred.distance' is possibly 'null'" on the old line, so the next
// one of these cannot be written silently. These tests cover what the type
// cannot: that the row still renders, and that the score stays a number.

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

import {
  PredictionTable,
  computeScore,
} from "@/app/[locale]/instrument/functional-annotation/[id]/page";
import type { Prediction, ScoringConfig } from "@/lib/api";

function pred(over: Partial<Prediction> = {}): Prediction {
  return {
    go_id: "GO:0003674",
    name: "molecular_function",
    aspect: "F",
    distance: null,
    ref_protein_accession: "sfjepa-recipe-ens8-cut211",
    qualifier: null,
    evidence_code: null,
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
  name: "test",
  formula: "weighted_mean",
  weights: { embedding_similarity: 1.0, identity_nw: 1.0 },
  evidence_weights: null,
  created_at: "2026-10-28T00:00:00Z",
};

describe("a prediction set written by an ingested method", () => {
  it("renders its rows instead of throwing on toFixed", () => {
    render(
      <PredictionTable preds={[pred()]} knownByGoId={new Map()} scoringConfig={undefined} />,
    );
    expect(screen.getAllByText("GO:0003674").length).toBeGreaterThan(0);
  });

  it("shows an em dash where the distance would go", () => {
    render(
      <PredictionTable preds={[pred()]} knownByGoId={new Map()} scoringConfig={undefined} />,
    );
    expect(screen.getAllByText("—").length).toBeGreaterThan(0);
  });

  it("orders rows by GO id when there is no distance to order by", () => {
    // The old comparator returned NaN for every pair, which leaves the order
    // to the sort implementation: the same set could read differently twice.
    render(
      <PredictionTable
        preds={[pred({ go_id: "GO:0009" }), pred({ go_id: "GO:0003" })]}
        knownByGoId={new Map()}
        scoringConfig={undefined}
      />,
    );
    const html = document.body.innerHTML;
    expect(html.indexOf("GO:0003")).toBeLessThan(html.indexOf("GO:0009"));
  });
});

describe("the score of a candidate with no distance", () => {
  it("is a number and not NaN", () => {
    // embedding_similarity was `1 - null / 2`, which is NaN, and NaN
    // propagated through the weighted sum: every row scored NaN and the
    // colour helper compared NaN against its thresholds.
    const score = computeScore(pred({ identity_nw: 0.5 }), CONFIG);
    expect(Number.isNaN(score)).toBe(false);
  });

  it("renormalises over the signals that exist", () => {
    // With embedding_similarity absent and identity_nw at 0.5, the only
    // signal with a value carries the whole weight.
    expect(computeScore(pred({ identity_nw: 0.5 }), CONFIG)).toBeCloseTo(0.5, 6);
  });

  it("still uses the distance when there is one", () => {
    // The guard must not disable the signal for retrieval arms, which are
    // every other set in the registry.
    const score = computeScore(pred({ distance: 0.5, identity_nw: 0.5 }), CONFIG);
    expect(score).toBeCloseTo((1 - 0.5 / 2 + 0.5) / 2, 6);
  });
});
