// The benchmark with no data must say so, not spin for ever.
//
// Reported on 2026-10-07: "El benchmark está vacío, y se queda como
// cargando, eso es un error." It was. With the corpus empty the matrix
// endpoint answers {"rows":[],"stages":[],...} perfectly well, but the
// page resolved its `stage` from matrix.stages, so stage stayed null and
// the gate `if (!embeddings || !matrix || stage === null)` held the
// loading branch for ever. A tool with no data looked exactly like a
// tool that was broken.
//
// This cannot be checked by fetching the page with curl: next-intl
// inlines the whole message catalogue into the HTML payload, so every
// string appears in the document whether it was rendered or not.

import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getBenchmarkEmbeddings: vi.fn(),
  getBenchmarkMatrix: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  useParams: () => ({ locale: "en" }),
  useRouter: () => ({ replace: vi.fn() }),
  usePathname: () => "/en/instrument/benchmark",
  useSearchParams: () => new URLSearchParams(""),
}));

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

vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<Record<string, unknown>>();
  return {
    ...actual,
    getBenchmarkEmbeddings: mocks.getBenchmarkEmbeddings,
    getBenchmarkMatrix: mocks.getBenchmarkMatrix,
  };
});

import BenchmarkPage from "@/app/[locale]/instrument/benchmark/page";

/** Exactly what the live API answers with an empty corpus. */
const EMPTY_MATRIX = {
  rows: [],
  total: 0,
  primary_metric: "f_micro_w",
  evaluation_sets: [],
  embedding_config_ids: [],
  stages: [],
  categories: ["NK", "LK", "PK"],
  aspects: ["BPO", "MFO", "CCO"],
  ks: [],
  per_task: [],
  best_per_cell: [],
};

beforeEach(() => {
  mocks.getBenchmarkEmbeddings.mockReset().mockResolvedValue({ embeddings: [], total: 0 });
  mocks.getBenchmarkMatrix.mockReset().mockResolvedValue(EMPTY_MATRIX);
});

describe("benchmark with an empty corpus", () => {
  it("states that it is built and waiting, instead of loading for ever", async () => {
    render(<BenchmarkPage />);
    await waitFor(() => {
      expect(screen.getByText("awaitingTitle")).toBeInTheDocument();
    });
    expect(screen.getByText("awaitingBody")).toBeInTheDocument();
  });

  it("stops claiming to be busy", async () => {
    const { container } = render(<BenchmarkPage />);
    await waitFor(() => {
      expect(screen.getByText("awaitingTitle")).toBeInTheDocument();
    });
    // The loading branch marks itself aria-busy; the empty one must not,
    // or a screen reader is told to keep waiting on nothing.
    expect(container.querySelector('[aria-busy="true"]')).toBeNull();
  });

  it("offers somewhere to look instead", async () => {
    render(<BenchmarkPage />);
    await waitFor(() => {
      expect(screen.getByText("awaitingJobs")).toBeInTheDocument();
    });
    expect(screen.getByText("awaitingGraph")).toBeInTheDocument();
  });
});
