// A prediction set's page says it is loading until the set's record arrives.
//
// On a set of about two million candidates the page's three reads take up to
// 20 s, and meanwhile it printed "0 proteins" and "No scored set has been made
// from it yet" about a set that had four. Until a read answers, the page now
// says what it is waiting for and states nothing about the set.

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

// Every read of the set hangs, as a slow one does for its first seconds.
vi.mock("@/lib/api", async (importOriginal) => {
  const real = await importOriginal<Record<string, unknown>>();
  const never = () => new Promise(() => undefined);
  return {
    ...real,
    getPredictionSet: never,
    getPredictionSetProteins: never,
    getGoTermDistribution: never,
  };
});

import PredictionSetDetailPage from "@/app/[locale]/instrument/functional-annotation/[id]/page";

describe("a prediction set's page while its reads are pending", () => {
  it("says it is loading and states nothing about the set", async () => {
    const params = Promise.resolve({ id: "032be544-b45d-46f8-b36e-d845220b99ca" });
    await act(async () => {
      render(
        <Suspense fallback={null}>
          <PredictionSetDetailPage params={params} />
        </Suspense>,
      );
    });
    expect(screen.getByTestId("set-loading-notice").textContent).toMatch(/Loading/);
    expect(screen.queryByText(/No scored set has been made/)).toBeNull();
    expect(screen.queryByText(/^0 proteins$/)).toBeNull();
    expect(screen.getAllByText(/Counting proteins/).length).toBeGreaterThan(0);
  });
});
