// A prediction set's page says when the set is still being written.
//
// While the job that writes a set runs, the API reads its counts live and keeps
// none (counts_provisional): the numbers will change, and the page says so
// rather than present them as the set's.

import { Suspense } from "react";
import { act, render, screen } from "@testing-library/react";
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

vi.mock("next/navigation", () => ({ notFound: () => null }));

vi.mock("@/components/Toast", () => ({ useToast: () => () => undefined }));

const record = (provisional: boolean) => ({
  id: "67c37d74-4199-4087-b23d-44e794f21cff",
  embedding_config_id: null,
  annotation_set_id: "453f8cf5-90ef-4b11-8ceb-2d713d4c4997",
  ontology_snapshot_id: "fb8fef9b-1ed6-43eb-84f7-369149285d9e",
  limit_per_entry: 300,
  created_at: "2026-11-22T00:10:13+00:00",
  prediction_count: 11297389,
  counts_provisional: provisional,
  scored_from: null,
  scored_sets: [],
});

const answer = vi.hoisted(() => ({ provisional: true }));

// The set's record answers; its proteins and terms are still being read.
vi.mock("@/lib/api", async (importOriginal) => {
  const real = await importOriginal<Record<string, unknown>>();
  const never = () => new Promise(() => undefined);
  return {
    ...real,
    getPredictionSet: () => Promise.resolve(record(answer.provisional)),
    getPredictionSetProteins: never,
    getGoTermDistribution: never,
  };
});

import PredictionSetDetailPage from "@/app/[locale]/instrument/functional-annotation/[id]/page";

async function open() {
  const params = Promise.resolve({ id: "67c37d74-4199-4087-b23d-44e794f21cff" });
  await act(async () => {
    render(
      <Suspense fallback={null}>
        <PredictionSetDetailPage params={params} />
      </Suspense>,
    );
  });
}

describe("a prediction set's page and the counts of a set being written", () => {
  it("says the counts are provisional while the set's job writes it", async () => {
    answer.provisional = true;
    await open();
    expect(screen.getByTestId("set-provisional-notice").textContent).toMatch(/still being written/);
  });

  it("says nothing of the kind about a complete set", async () => {
    answer.provisional = false;
    await open();
    expect(screen.queryByTestId("set-provisional-notice")).toBeNull();
  });
});
