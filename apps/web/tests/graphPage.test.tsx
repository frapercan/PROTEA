/**
 * The graph page, rendered against the record as it actually stands.
 *
 * These assertions are about the model's rules rather than about pixels.
 * The four that matter:
 *
 *   1. The unsealed count is on the page, in the frame, above every score.
 *      A number that cannot be attributed cannot be compared, so a surface
 *      that shows the scores and hides the attribution is the failure this
 *      page exists to prevent.
 *   2. A blocked node states its reason inline. If it took a click, an
 *      operator scanning the pipeline would read a gap as a zero.
 *   3. There are nine panels and exactly nine columns of numbers. No
 *      total, no mean, no "overall": cardinality is a vector over the
 *      nine and any scalar built from it is a claim the model forbids.
 *   4. Empty and broken are distinguishable from loading. An instrument
 *      that shows a skeleton forever cannot say which of the three it is.
 *
 * Translations come from the real message catalogue with a throwing
 * `onError`, so a key this page references and the catalogue does not
 * carry fails here rather than shipping as a raw key path on screen.
 */

import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { describe, it, expect, vi, beforeEach } from "vitest";
import messages from "@/messages/en.json";
import { ApiError } from "@/lib/api";
import { EMPTY_GRAPH_FIXTURE, GRAPH_FIXTURE } from "./fixtures/graph";

const getGraph = vi.fn();

vi.mock("@/lib/graph", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/graph")>();
  return { ...actual, getGraph: () => getGraph() };
});

// Imported after the mock is registered so the page binds the double.
const { default: GraphPage } = await import("@/app/[locale]/instrument/graph/page");

function renderPage() {
  return render(
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
}

beforeEach(() => {
  getGraph.mockReset();
});

describe("graph page, populated", () => {
  beforeEach(() => {
    getGraph.mockResolvedValue(GRAPH_FIXTURE);
  });

  it("puts the frame and its unsealed rows above the scores", async () => {
    renderPage();
    const frame = await screen.findByTestId("graph-frame");
    expect(within(frame).getByText("220->227")).toBeInTheDocument();
    expect(within(frame).getByText("valid")).toBeInTheDocument();
    expect(within(frame).getByText("reconciled")).toBeInTheDocument();
    expect(within(frame).getByText("instance-zero-220-227-valid")).toBeInTheDocument();
    expect(within(frame).getByText("14,032 entries")).toBeInTheDocument();
    // Declared and populated are different questions. Every field above is
    // present and the frame is still not declared, because no result row
    // seals to it. The card has to say both, not swap one for the other.
    expect(within(frame).getByText("not declared")).toBeInTheDocument();

    const sealing = within(frame).getByTestId("graph-sealing");
    expect(within(sealing).getByText("Sealed rows")).toBeInTheDocument();
    expect(within(sealing).getByText("Unsealed rows")).toBeInTheDocument();
    expect(within(sealing).getByText("8")).toBeInTheDocument();
    // Not just the count: the reason it disqualifies a comparison.
    expect(
      within(frame).getByText(/cannot be attributed cannot be compared/i),
    ).toBeInTheDocument();
  });

  it("shows the accretion regime and its sha, truncated but recoverable", async () => {
    renderPage();
    const frame = await screen.findByTestId("graph-frame");
    expect(within(frame).getByText("lafa")).toBeInTheDocument();
    const sha = within(frame).getByTitle(
      "308fc28ef3df566658a9e3cbe28a0c72b41000e0e1e4eec6ebfca2801c38b55e",
    );
    expect(sha).toHaveTextContent("308fc28ef3df");
  });

  it("renders every node once, in pipeline order", async () => {
    renderPage();
    const nodes = await screen.findByTestId("graph-nodes");
    const titles = [
      "Frame",
      "Substrate",
      "Bank",
      "Retriever",
      "Generator",
      "Scoring",
      "Features",
      "Re-ranking",
      "Combination",
      "Routing",
    ];
    const rows = within(nodes)
      .getAllByRole("row")
      .filter((r) => r.id.startsWith("node-") && !r.id.endsWith("-why"));
    expect(rows.map((r) => r.id)).toEqual([
      "node-frame",
      "node-substrate",
      "node-bank",
      "node-retriever",
      "node-generator",
      "node-scoring",
      "node-features",
      "node-reranking",
      "node-combination",
      "node-routing",
    ]);
    for (const title of titles) {
      expect(within(nodes).getAllByText(title).length).toBeGreaterThan(0);
    }
  });

  it("never calls a one-level node measured, whatever it scored", async () => {
    renderPage();
    await screen.findByTestId("graph-nodes");
    const scoring = document.getElementById("node-scoring")!;
    // Eight levels instantiated and eight results, which is what makes the
    // contrast a contrast at all.
    expect(within(scoring).getAllByText("8")).toHaveLength(2);

    const substrate = document.getElementById("node-substrate")!;
    expect(within(substrate).getByText("inherited")).toBeInTheDocument();
    expect(within(substrate).queryByText("measured")).toBeNull();
    // One of thirteen. The twelve that were never tried are the reason the
    // count is a pair and not a single number.
    expect(within(substrate).getByText("/ 13")).toBeInTheDocument();
    expect(document.getElementById("node-substrate-why")).toHaveTextContent(
      /never tried/i,
    );
  });

  it("states a blocked node's reason on the row, not behind a click", async () => {
    renderPage();
    await screen.findByTestId("graph-nodes");
    const reranking = document.getElementById("node-reranking")!;
    expect(within(reranking).getByText("blocked")).toBeInTheDocument();
    // The reason sits on the line directly under the node, in the table,
    // with no disclosure to open.
    const why = document.getElementById("node-reranking-why")!;
    expect(why).toHaveTextContent(/reranker_model holds 0 rows/i);
    expect(reranking.nextElementSibling).toBe(why);
  });

  it("draws the nine panels with their populations and never a tenth", async () => {
    renderPage();
    const grid = await screen.findByTestId("graph-panel-grid");
    for (const cat of ["NK", "LK", "PK"]) {
      for (const asp of ["BPO", "MFO", "CCO"]) {
        expect(within(grid).getByText(`${cat} · ${asp}`)).toBeInTheDocument();
      }
    }
    expect(within(grid).getAllByText("1,509").length).toBeGreaterThan(0);
    expect(within(grid).getAllByText("5,800").length).toBeGreaterThan(0);
    expect(within(grid).getAllByText("821").length).toBeGreaterThan(0);
  });

  it("gives the matrix exactly nine value columns and no aggregate", async () => {
    renderPage();
    const matrix = await screen.findByTestId("graph-matrix");
    const bodyRows = within(matrix).getAllByRole("row").slice(2);
    expect(bodyRows).toHaveLength(8);
    for (const row of bodyRows) {
      // One row header (the level) plus nine panels. A tenth cell could
      // only be a total, which is the collapse the model forbids.
      expect(within(row).getAllByRole("cell")).toHaveLength(9);
    }
    const text = matrix.textContent ?? "";
    for (const forbidden of ["Overall", "Mean", "Average", "Total"]) {
      expect(text).not.toContain(forbidden);
    }
  });

  it("lists every blocked level with its precondition", async () => {
    renderPage();
    const blocked = await screen.findByTestId("graph-blocked");
    expect(
      within(blocked).getByText(/rows in interpro_annotation and interpro_go_mapping/),
    ).toBeInTheDocument();
    expect(within(blocked).getByText("a row in reranker_model")).toBeInTheDocument();
    expect(
      within(blocked).getByText(/a second flow, which the generator node has to produce first/),
    ).toBeInTheDocument();
  });
});

describe("graph page, nothing instantiated", () => {
  it("says so instead of holding a skeleton open", async () => {
    getGraph.mockResolvedValue(EMPTY_GRAPH_FIXTURE);
    renderPage();
    expect(await screen.findByTestId("graph-empty")).toHaveTextContent(
      "Nothing is instantiated yet",
    );
    expect(screen.queryByTestId("graph-loading")).toBeNull();
    expect(screen.queryByTestId("graph-panel-grid")).toBeNull();
  });
});

describe("graph page, endpoint failures", () => {
  it("separates a route this API does not serve from a data problem", async () => {
    getGraph.mockRejectedValue(new ApiError("http", 404, "/graph", "Not Found"));
    renderPage();
    const err = await screen.findByTestId("graph-error");
    expect(err).toHaveTextContent("This API build does not serve the graph");
    expect(err).toHaveTextContent("http · 404 · /graph");
  });

  it("reports any other failure with its own message", async () => {
    getGraph.mockRejectedValue(new ApiError("network", 0, "/graph", "fetch failed"));
    renderPage();
    const err = await screen.findByTestId("graph-error");
    expect(err).toHaveTextContent("The graph could not be read");
    expect(err).toHaveTextContent("fetch failed");
  });

  it("offers a retry that calls the endpoint again", async () => {
    getGraph.mockRejectedValueOnce(new ApiError("network", 0, "/graph", "fetch failed"));
    getGraph.mockResolvedValueOnce(GRAPH_FIXTURE);
    renderPage();
    fireEvent.click(await screen.findByText("Try again"));
    await waitFor(() => expect(screen.queryByTestId("graph-error")).toBeNull());
    expect(await screen.findByTestId("graph-frame")).toBeInTheDocument();
  });
});
