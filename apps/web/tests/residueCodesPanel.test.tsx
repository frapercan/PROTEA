// The per-residue codes tab must show how far a pass has gone, from the index.
//
// On 2026-11-03 the corpus pass of encode_residue_vocabulary was at 22 % while
// its coordinator sat SUCCEEDED with a full green bar on the jobs page: it had
// run code from before #1091 and closed the moment it dispatched its batches.
// The author: "porque no veo en PROTEA nada de que estemos generando
// embeddings". This tab reads the index instead, and these tests pin what it
// must say: the coverage the endpoint computed (not one the page recomputes),
// the counts, the time left, the job that wrote the codes, and an honest
// empty state when no residue vocabulary exists.

import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ listResidueCodes: vi.fn() }));

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  useTranslations: () => (key: string, args?: Record<string, unknown>) =>
    args ? `${key}:${JSON.stringify(args)}` : key,
}));

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children?: React.ReactNode }) => <a href={href}>{children}</a>,
}));

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return { ...actual, listResidueCodes: mocks.listResidueCodes };
});

import { ResidueCodesPanel } from "@/components/ResidueCodesPanel";

/** The shape the live endpoint answered with during the pass, trimmed. */
const DURING_THE_PASS = [
  {
    embedding_config_id: "22166300-a178-599e-835c-afe1c007feb5",
    name: "residue-vocabulary:06fed75ca76d17be:f3a7fe16",
    description: "per-residue code of the residue vocabulary vocabulary-v1",
    layer_indices: [2, 10, 22, 40, 48],
    created_at: "2026-11-03T01:54:33+00:00",
    source: { embedding_config_id: "f3a7fe16-eaf5-5083-b459-7c5fa7a9a9e0", name: "Ankh base", sequences: 683887 },
    index: {
      sequences: 150480,
      residues: 52000000,
      entries: 6500000000,
      shards: 1172,
      first_written_at: "2026-11-03T01:56:00+00:00",
      last_written_at: "2026-11-03T07:26:18+00:00",
      sequences_last_hour: 36000,
    },
    coverage: 0.22,
    remaining: 533407,
    eta_hours: 14.8,
    job: {
      id: "15624da2-3ea7-4d4e-ae05-4200dd101772",
      status: "SUCCEEDED",
      progress_current: null,
      progress_total: 5329,
      created_at: "2026-11-03T01:54:33+00:00",
      finished_at: "2026-11-03T01:54:35+00:00",
    },
  },
];

describe("ResidueCodesPanel", () => {
  beforeEach(() => {
    mocks.listResidueCodes.mockReset();
  });

  it("shows the coverage the endpoint computed, the counts and the time left", async () => {
    mocks.listResidueCodes.mockResolvedValue(DURING_THE_PASS);
    render(<ResidueCodesPanel />);
    await waitFor(() => expect(screen.getByText("22.0 %")).toBeTruthy());
    expect(screen.getByText('sequencesOfTotal:{"done":"150,480","total":"683,887"}')).toBeTruthy();
    expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("22");
    expect(screen.getByText("1,172")).toBeTruthy();
    expect(screen.getByText("14.8 h")).toBeTruthy();
    expect(screen.getByText('layers:{"layers":"2, 10, 22, 40, 48"}')).toBeTruthy();
  });

  it("links the job that wrote the codes and shows its own counter as it is", async () => {
    mocks.listResidueCodes.mockResolvedValue(DURING_THE_PASS);
    render(<ResidueCodesPanel />);
    const link = await screen.findByText("15624da2");
    expect(link.closest("a")?.getAttribute("href")).toBe(
      "/en/instrument/jobs/15624da2-3ea7-4d4e-ae05-4200dd101772",
    );
    expect(screen.getByText(/batches:\{"done":"0","total":"5,329"\}/)).toBeTruthy();
  });

  it("says a finished pass is done instead of estimating", async () => {
    const done = [{ ...DURING_THE_PASS[0], coverage: 1, remaining: 0, eta_hours: null }];
    mocks.listResidueCodes.mockResolvedValue(done);
    render(<ResidueCodesPanel />);
    await waitFor(() => expect(screen.getByText("100.0 %")).toBeTruthy());
    expect(screen.getByText("done")).toBeTruthy();
  });

  it("says there is no residue vocabulary instead of spinning", async () => {
    mocks.listResidueCodes.mockResolvedValue([]);
    render(<ResidueCodesPanel />);
    await waitFor(() => expect(screen.getByText("emptyTitle")).toBeTruthy());
    expect(screen.queryByRole("progressbar")).toBeNull();
  });

  it("shows the error when the endpoint fails", async () => {
    mocks.listResidueCodes.mockRejectedValue(new Error("network down"));
    render(<ResidueCodesPanel />);
    await waitFor(() => expect(screen.getByText(/network down/)).toBeTruthy());
  });
});
