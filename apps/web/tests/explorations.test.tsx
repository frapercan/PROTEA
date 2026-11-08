// The explorations page (ADR-D55): what it must show and what it must refuse to offer.
//
// The author, 2026-11-04: "Necesitamos una pantalla que nos atomice cada exploración
// evaluada (hipótesis)", in place of a graph and a benchmark table that crowned a
// champion by maximum. Three things here would fail quietly if they broke: the list
// must group by question and never reorder by a figure; the decision form must appear
// only to a signed-in session on a reviewed exploration, since the server refuses
// anything else; and the operator's move must be the one the state allows.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  listExplorations: vi.fn(),
  getExploration: vi.fn(),
  getExplorationReceipt: vi.fn(),
  moveExploration: vi.fn(),
  decideExploration: vi.fn(),
  signedIn: { value: true },
  operator: { value: true },
}));

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  useTranslations: () => (key: string, args?: Record<string, unknown>) =>
    args ? `${key}:${JSON.stringify(args)}` : key,
}));

vi.mock("@/lib/useRole", () => ({
  useHasRole: () => mocks.operator.value,
  useRole: () => (mocks.operator.value ? "operator" : "viewer"),
  useIsAuthenticated: () => mocks.signedIn.value,
}));

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    listExplorations: mocks.listExplorations,
    getExploration: mocks.getExploration,
    getExplorationReceipt: mocks.getExplorationReceipt,
    moveExploration: mocks.moveExploration,
    decideExploration: mocks.decideExploration,
  };
});

import { ExplorationsPanel } from "@/components/ExplorationsPanel";
import type { Exploration, ExplorationHeadline } from "@/lib/api";

const BANK: ExplorationHeadline = {
  id: "e2",
  number: 2,
  title: "corpus membership at the window's start",
  question: "does the bank leak the future?",
  kind: "one_axis",
  state: "reported",
  archived_reason: null,
  grade: "development",
  verdict: "for",
  post_hoc: false,
  proposer: "key:desktop",
  launcher: "key:laptop",
  reviewer: null,
  decision: null,
  evaluation_set_id: "ea0f4334-0000-0000-0000-000000000000",
  created_at: null,
};

const REPEAT: ExplorationHeadline = {
  ...BANK,
  id: "e1",
  number: 1,
  title: "the instrument repeats",
  question: "does the same run give the same panels?",
  kind: "instrument",
  state: "draft",
  verdict: null,
  grade: null,
};

function detail(head: ExplorationHeadline, events: string[]): Exploration {
  return {
    ...head,
    hypothesis: "h",
    rules: { axis: "donor_policy" },
    rules_sha256: head.state === "draft" ? null : "a".repeat(64),
    rules_frozen_at: null,
    rules_frozen_by: null,
    decision_text: null,
    decided_at: null,
    decided_by: null,
    archived_at: null,
    report_sha256: null,
    links: [],
    events: events.map((event) => ({ at: null, actor: "key:laptop", actor_type: "api_key", event, body: {}, link_id: null })),
  };
}

const RECEIPT = { layers: { method: { status: "one axis" } }, blocks_launch: [], blocks_verdict: [] };

beforeEach(() => {
  mocks.signedIn.value = true;
  mocks.operator.value = true;
  mocks.listExplorations.mockResolvedValue([REPEAT, BANK]);
  mocks.getExplorationReceipt.mockResolvedValue(RECEIPT);
  mocks.decideExploration.mockResolvedValue({});
  mocks.moveExploration.mockResolvedValue({});
});

afterEach(() => vi.clearAllMocks());

describe("the list", () => {
  it("groups by question and keeps the numbers in order", async () => {
    render(<ExplorationsPanel />);
    const headings = await screen.findAllByRole("heading", { level: 2 });
    expect(headings.map((h) => h.textContent)).toEqual([REPEAT.question, BANK.question]);
  });
});

describe("the decision", () => {
  async function openBank(events: string[]) {
    mocks.getExploration.mockResolvedValue(detail(BANK, events));
    render(<ExplorationsPanel />);
    fireEvent.click(await screen.findByText(/corpus membership/));
    await screen.findByText("blocks.receipt");
  }

  it("waits for the review", async () => {
    await openBank(["created", "launched", "report"]);
    expect(screen.getByText("decision.awaitingReview")).toBeTruthy();
    expect(screen.queryByText("decision.submit")).toBeNull();
  });

  it("is offered only to a signed-in session", async () => {
    mocks.signedIn.value = false;
    await openBank(["created", "launched", "report", "reviewed"]);
    expect(screen.getByText("decision.signIn")).toBeTruthy();
    expect(screen.queryByText("decision.submit")).toBeNull();
  });

  it("sends the author's words with the decision", async () => {
    await openBank(["created", "launched", "report", "reviewed"]);
    fireEvent.click(screen.getByLabelText("decision.rejected"));
    fireEvent.change(screen.getByLabelText("decision.why"), { target: { value: "the frame moved" } });
    fireEvent.click(screen.getByText("decision.submit"));
    await waitFor(() => expect(mocks.decideExploration).toHaveBeenCalledWith("e2", "rejected", "the frame moved"));
  });
});

describe("the moves", () => {
  it("offers the move the state allows", async () => {
    mocks.getExploration.mockResolvedValue(detail(REPEAT, ["created"]));
    render(<ExplorationsPanel />);
    fireEvent.click(await screen.findByText(/the instrument repeats/));
    fireEvent.click(await screen.findByText("moves.launch"));
    await waitFor(() => expect(mocks.moveExploration).toHaveBeenCalledWith("e1", "launch"));
  });

  it("is not offered to a viewer", async () => {
    mocks.operator.value = false;
    mocks.getExploration.mockResolvedValue(detail(REPEAT, ["created"]));
    render(<ExplorationsPanel />);
    fireEvent.click(await screen.findByText(/the instrument repeats/));
    await screen.findByText("blocks.receipt");
    expect(screen.queryByText("moves.launch")).toBeNull();
  });
});
