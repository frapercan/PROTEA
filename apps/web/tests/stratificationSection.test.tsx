/**
 * The fine stratification, rendered against a record shaped like the real one.
 *
 * These assertions are about the model's rules, not about pixels:
 *
 *   1. A cell below the floor is SHOWN with its population and marked. A
 *      table that dropped it would look identical to one that covered
 *      everything, and the difference is why the section exists.
 *   2. The arms the comparison covers are on screen before the first cell.
 *      A crossing read over half the arms and quoted as if it covered them
 *      all is the error this page exists not to make.
 *   3. Length and identity are not drawn as interchangeable: one carries a
 *      routing count, the other carries the same count struck out.
 *   4. Nothing is judged without a floor. With no floor served the section
 *      says so and prints no verdict, because a zero would read as a
 *      finding.
 *
 * Translations come from the real catalogue with a throwing `onError`, so a
 * key this component references and the catalogue does not carry fails here
 * rather than shipping as a raw key path on screen.
 */

import { render, screen, waitFor, within } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import messages from "@/messages/en.json";
import { StratificationSection } from "@/components/StratificationSection";
import type { ContrastFloors, GraphPanel } from "@/lib/graph";
import type { ComparePayload, CompareRow } from "@/lib/strataStructure";

const FLOORS: ContrastFloors = {
  target_effect: 0.02,
  z_sum: 2.8016,
  classes: [
    {
      key: "reporting",
      sigma_paired: 0.081,
      population: 129,
      contrast: "arms sharing retrieval",
    },
    {
      key: "routing",
      sigma_paired: 0.13,
      population: 332,
      contrast: "arms retrieving differently",
    },
  ],
};

const PANELS: GraphPanel[] = [
  {
    category: "NK",
    aspect: "BPO",
    units: 1509,
    detectable_effect: 0.0058,
    results: [],
  },
  { category: "PK", aspect: "BPO", units: 5811, detectable_effect: 0.003, results: [] },
];

function row(over: Partial<CompareRow>): CompareRow {
  return {
    evaluation_result_id: "arm-a",
    model: "m",
    display_name: "m",
    k: 10,
    category: "NK",
    aspect: "P",
    length: "<=512",
    homology: "60-90",
    n_proteins: 100,
    reportable: true,
    ...over,
  };
}

/** Two arms, so a cell can disagree with itself across them. */
function bothArms(over: Partial<CompareRow>, low: number, high: number): CompareRow[] {
  return [
    row({ ...over, evaluation_result_id: "arm-a", n_proteins: low }),
    row({ ...over, evaluation_result_id: "arm-b", n_proteins: high }),
  ];
}

const ROWS: Record<string, CompareRow[]> = {
  NK: [
    // Clears the routing floor under both arms.
    ...bothArms({ category: "NK", length: "<=512", homology: "60-90" }, 900, 950),
    // Clears reporting, never routing.
    ...bothArms({ category: "NK", length: "512-1024", homology: "<=30" }, 200, 210),
    // Below every floor, and it has to stay visible.
    ...bothArms({ category: "NK", length: ">2048", homology: ">90" }, 17, 17),
  ],
  LK: [],
  PK: [...bothArms({ category: "PK", length: "<=512", homology: ">90" }, 4000, 4100)],
};

function payload(setting: string, over: Partial<ComparePayload> = {}): ComparePayload {
  return {
    evaluation_set_id: "eval-set-1",
    setting,
    where: {},
    arms_total: 16,
    arms_with_strata: 16,
    rows: ROWS[setting] ?? [],
    ...over,
  };
}

type Responder = (setting: string) => Partial<Response> & { json?: () => unknown };

function mockFetch(responder: Responder) {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    const setting = new URL(url, "http://x").searchParams.get("setting") ?? "?";
    const res = responder(setting);
    return {
      ok: res.ok ?? true,
      status: res.status ?? 200,
      json: res.json ?? (() => payload(setting)),
    } as unknown as Response;
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function renderSection(props: Partial<Parameters<typeof StratificationSection>[0]> = {}) {
  return render(
    <NextIntlClientProvider
      locale="en"
      messages={messages}
      onError={(e) => {
        throw e;
      }}
    >
      <StratificationSection
        evaluationSetId="eval-set-1"
        floors={FLOORS}
        panels={PANELS}
        {...props}
      />
    </NextIntlClientProvider>,
  );
}

beforeEach(() => {
  vi.stubEnv("NEXT_PUBLIC_API_URL", "http://api.test");
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

describe("fine stratification, populated", () => {
  beforeEach(() => {
    mockFetch(() => ({}));
  });

  it("asks for the three knowledge categories separately", async () => {
    const fetchMock = mockFetch(() => ({}));
    renderSection();
    await screen.findByTestId("strata-triple");
    const settings = fetchMock.mock.calls.map((c) =>
      new URL(String(c[0]), "http://x").searchParams.get("setting"),
    );
    expect(settings.sort()).toEqual(["LK", "NK", "PK"]);
  });

  it("shows a cell below the floor with its population, marked and not dropped", async () => {
    renderSection();
    const length = await screen.findByTestId("strata-axis-length");
    // 17 proteins clears nothing, and it is on screen anyway.
    const thin = within(length).getByText("17");
    expect(thin).toHaveAttribute("title", "below every floor");
    // The cell that clears everything is on the same table.
    expect(within(length).getByText("900\u2013950")).toHaveAttribute(
      "title",
      "clears the strictest floor",
    );
    // And the one in between says which floor it cleared.
    expect(within(length).getByText("200\u2013210")).toHaveAttribute(
      "title",
      "below the strictest floor, above a weaker one",
    );
  });

  it("prints a population the arms disagree about as a range, never as one number", async () => {
    renderSection();
    const homology = await screen.findByTestId("strata-axis-homology");
    // 4,000 under one arm and 4,100 under the other. A single figure here
    // would hide that the band belongs to the arm.
    expect(within(homology).getByText("4,000–4,100")).toBeInTheDocument();
  });

  it("names the arms the comparison covers before any cell", async () => {
    renderSection();
    const arms = await screen.findByTestId("strata-arms");
    expect(within(arms).getByText("Arms stratified")).toBeInTheDocument();
    expect(within(arms).getAllByText("16")).not.toHaveLength(0);
  });

  it("draws length as routable and identity as not, in the same geometry", async () => {
    renderSection();
    const length = await screen.findByTestId("strata-axis-length");
    const homology = await screen.findByTestId("strata-axis-homology");
    expect(within(length).getByText("can route")).toBeInTheDocument();
    expect(within(homology).getByText("cannot route")).toBeInTheDocument();
    // Same column, so the two tables are comparable at a glance.
    expect(within(length).getAllByText("Routing regions")).toHaveLength(1);
    expect(within(homology).getAllByText("Routing regions")).toHaveLength(1);
  });

  it("states the verdict of the triple crossing as counts it can support", async () => {
    renderSection();
    const verdict = await screen.findByTestId("strata-verdict");
    // Two cells clear the routing floor, one per panel; the NK panel keeps
    // three cells and the PK panel one.
    expect(verdict).toHaveTextContent(
      "2 of 4 cells with a population clear the strictest floor",
    );
    expect(verdict).toHaveTextContent("0 of 2 panels get none");
  });

  it("never draws a total over the panels", async () => {
    renderSection();
    const section = await screen.findByTestId("graph-strata");
    const text = section.textContent ?? "";
    for (const forbidden of ["Overall", "Mean", "Average", "Total"]) {
      expect(text).not.toContain(forbidden);
    }
  });
});

describe("fine stratification, the record cannot answer", () => {
  it("says nothing was stratified when every category 404s", async () => {
    mockFetch(() => ({ status: 404, ok: false }));
    renderSection();
    expect(
      await screen.findByText(/No arm of this set has been stratified/),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("strata-triple")).toBeNull();
  });

  it("separates an endpoint failure from an unstratified set", async () => {
    mockFetch(() => ({ status: 500, ok: false }));
    renderSection();
    expect(
      await screen.findByText(/whether anything was stratified is not known here/),
    ).toBeInTheDocument();
    expect(screen.queryByText(/No arm of this set has been stratified/)).toBeNull();
  });

  it("warns when the comparison covers only some of the arms", async () => {
    mockFetch((setting) => ({
      json: async () => payload(setting, { arms_with_strata: 8 }),
    }));
    renderSection();
    await screen.findByTestId("strata-triple");
    expect(
      screen.getByText(/holds arms that were never stratified/),
    ).toBeInTheDocument();
  });

  it("refuses to judge any cell when the build serves no floor", async () => {
    mockFetch(() => ({}));
    renderSection({ floors: null });
    const verdict = await screen.findByTestId("strata-verdict");
    expect(verdict).toHaveTextContent(/not knowable here/);
    // A zero here would read as a measurement rather than as an absence.
    expect(verdict).not.toHaveTextContent(/clear the strictest floor/);
    expect(
      screen.getByText(/This API build does not serve the floors/),
    ).toBeInTheDocument();
  });

  it("says so rather than fetching when the frame names no evaluation set", async () => {
    const fetchMock = mockFetch(() => ({}));
    renderSection({ evaluationSetId: null });
    await waitFor(() =>
      expect(screen.getByText(/frame names no evaluation set/)).toBeInTheDocument(),
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
