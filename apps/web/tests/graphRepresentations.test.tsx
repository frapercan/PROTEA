/**
 * The representations section, held to the rules the record forces on it.
 *
 * The Substrate node publishes "1 / 13" and names nothing. This section is
 * that denominator expanded, and the assertions below are about what it must
 * never smooth over:
 *
 *   1. A configuration with no stored embedding reads as UNBUILT, not as an
 *      untried alternative. The two are different situations and a table that
 *      renders them alike overstates what was passed over.
 *   2. A missing parameter count renders as missing. Never a zero, never a
 *      dash that could be read as one, and never the thing the table is
 *      ordered by.
 *   3. A coverage below the corpus says by how much. Rounding 528,234 of
 *      528,294 to "100%" turns an unfinished encoding into an available one.
 *   4. A fitted encoding names the annotation release it was fitted against,
 *      because a fitted encoding and a pretrained backbone are not two
 *      settings of one knob.
 *   5. An API build that does not send the key renders no section at all,
 *      which is a different answer from an empty one.
 *
 * Translations come from the real catalogue with a throwing `onError`, so a
 * key this section references and the catalogue does not carry fails here.
 */

import { render, screen, waitFor, within } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { describe, it, expect, vi, beforeEach } from "vitest";
import messages from "@/messages/en.json";
import { SubstrateRepresentations } from "@/components/SubstrateRepresentations";
import type { GraphRepresentation, GraphRepresentations } from "@/lib/graph";
import { GRAPH_FIXTURE } from "./fixtures/graph";

const getGraph = vi.fn();

vi.mock("@/lib/graph", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/graph")>();
  return { ...actual, getGraph: () => getGraph() };
});

const { default: GraphPage } = await import("@/app/[locale]/instrument/graph/page");

function rep(over: Partial<GraphRepresentation> = {}): GraphRepresentation {
  return {
    id: "ec-1",
    label: "esm2_650m",
    display_name: "esm2_650m",
    model_name: "facebook/esm2_t33_650M_UR50D",
    model_backend: "esm",
    family: "esm2",
    param_count: 652353941,
    layer_indices: "[0]",
    layer_agg: "mean",
    pooling: "mean",
    normalize: true,
    normalize_residues: false,
    max_length: "2048",
    use_chunking: false,
    embeddings_stored: 528294,
    coverage: 1,
    state: "retrieved",
    trained_on: null,
    prediction_sets: 2,
    results: 16,
    ...over,
  };
}

const PAYLOAD: GraphRepresentations = {
  corpus_sequences: 528294,
  total: 4,
  built: 3,
  retrieved: 1,
  rows: [
    rep(),
    rep({
      id: "ec-2",
      label: "ElnaggarLab/ankh-base",
      display_name: null,
      model_name: "ElnaggarLab/ankh-base",
      model_backend: "ankh",
      family: null,
      param_count: null,
      layer_indices: "[10]",
      state: "built",
      prediction_sets: 0,
      results: 0,
    }),
    rep({
      id: "ec-3",
      label: "rung2-residue",
      display_name: "rung2-residue",
      model_name: "rung2-residue",
      model_backend: "residue-sparse",
      family: "residue-sparse",
      param_count: null,
      pooling: "residue-sparse-mean",
      embeddings_stored: 528234,
      coverage: 528234 / 528294,
      state: "built",
      trained_on: {
        annotation_set_id: "as-220",
        source: "goa",
        version: "220",
        published_at: "2024-04-16",
      },
      prediction_sets: 0,
      results: 0,
    }),
    rep({
      id: "ec-4",
      label: "never-built",
      display_name: "never-built",
      model_name: "never-built",
      embeddings_stored: 0,
      coverage: 0,
      state: "unbuilt",
      param_count: null,
      prediction_sets: 0,
      results: 0,
    }),
  ],
};

function renderSection(payload: GraphRepresentations) {
  return render(
    <NextIntlClientProvider
      locale="en"
      messages={messages}
      onError={(e) => {
        throw e;
      }}
    >
      <SubstrateRepresentations representations={payload} />
    </NextIntlClientProvider>,
  );
}

describe("representations section", () => {
  it("restates the node's ratio and names the corpus it is a fraction of", () => {
    renderSection(PAYLOAD);
    const counts = screen.getByTestId("graph-representations-counts");
    expect(within(counts).getByText("Registered")).toBeInTheDocument();
    expect(within(counts).getByText("Built")).toBeInTheDocument();
    expect(within(counts).getByText("Retrieved against")).toBeInTheDocument();
    expect(within(counts).getByText("Corpus")).toBeInTheDocument();
    expect(within(counts).getByText("528,294")).toBeInTheDocument();
  });

  it("separates what was retrieved against from what was only built, and from what was never built", () => {
    renderSection(PAYLOAD);
    expect(screen.getByText("retrieved against")).toBeInTheDocument();
    expect(screen.getByText("built, never used")).toBeInTheDocument();
    // The third group is the one a bare ratio cannot express: a configuration
    // with no stored embedding is not an alternative that was passed over.
    expect(screen.getByText("not built")).toBeInTheDocument();
  });

  it("says a parameter count is missing rather than printing a zero", () => {
    renderSection(PAYLOAD);
    // Three of the four rows carry no size, and the note says so instead of
    // letting the blanks read as the page failing to load.
    expect(screen.getAllByText("∅ not recorded")).toHaveLength(3);
    expect(
      screen.getByText(/Parameter count is recorded for 1 of 4/),
    ).toBeInTheDocument();
  });

  it("keeps two configurations of one backbone apart by their layer", () => {
    renderSection(PAYLOAD);
    expect(screen.getByText("[10]")).toBeInTheDocument();
    expect(screen.getByText("no display name recorded")).toBeInTheDocument();
  });

  it("does not round a shortfall away", () => {
    renderSection(PAYLOAD);
    expect(screen.getByText("99.989%")).toBeInTheDocument();
    expect(screen.getByText("60 sequences short")).toBeInTheDocument();
  });

  it("names the release a fitted encoding was fitted against", () => {
    renderSection(PAYLOAD);
    expect(screen.getByText("fitted")).toBeInTheDocument();
    expect(screen.getByText(/goa 220/)).toBeInTheDocument();
    expect(screen.getAllByText("pretrained").length).toBeGreaterThan(0);
  });

  it("prints never rather than a column of zeros", () => {
    renderSection(PAYLOAD);
    expect(screen.getAllByText("never")).toHaveLength(3);
    expect(screen.getByText("prediction sets")).toBeInTheDocument();
  });

  it("withholds the ratio when a stored row is a chunk and not a sequence", () => {
    renderSection({
      corpus_sequences: 528294,
      total: 1,
      built: 1,
      retrieved: 0,
      rows: [rep({ state: "built", use_chunking: true, coverage: null })],
    });
    expect(
      screen.getByText("chunked: a row is a chunk and not a sequence, so no ratio is taken"),
    ).toBeInTheDocument();
  });

  it("says the record is empty rather than drawing an empty table", () => {
    renderSection({ corpus_sequences: null, total: 0, built: 0, retrieved: 0, rows: [] });
    expect(screen.getByText("The record holds no registered representation.")).toBeInTheDocument();
  });
});

describe("the page when the key is absent", () => {
  beforeEach(() => {
    getGraph.mockReset();
  });

  it("renders no section at all, which is not the same as an empty one", async () => {
    // The captured fixture predates the key, which is exactly the shape an
    // older API build answers with.
    getGraph.mockResolvedValue(GRAPH_FIXTURE);
    render(
      <NextIntlClientProvider
        locale="en"
        messages={messages}
        onError={(e) => {
          throw e;
        }}
      >
        <GraphPage />
      </NextIntlClientProvider>,
    );
    await screen.findByTestId("graph-nodes");
    await waitFor(() => {
      expect(screen.queryByTestId("graph-representations-section")).toBeNull();
    });
  });

  it("draws the section when the build does send it", async () => {
    getGraph.mockResolvedValue({ ...GRAPH_FIXTURE, representations: PAYLOAD });
    render(
      <NextIntlClientProvider
        locale="en"
        messages={messages}
        onError={(e) => {
          throw e;
        }}
      >
        <GraphPage />
      </NextIntlClientProvider>,
    );
    const section = await screen.findByTestId("graph-representations-section");
    expect(within(section).getByText("Representations")).toBeInTheDocument();
    expect(within(section).getByText("retrieved against")).toBeInTheDocument();
  });
});
