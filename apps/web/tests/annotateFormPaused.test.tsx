// The annotate form while the predictor is paused.
//
// Before this, a visitor who submitted got the API's own 409 body
// rendered verbatim: "No annotation sets available. Load GO annotations
// first." That is an instruction to a database operator, shown to
// whoever was evaluating the project. Reproduced against the live API on
// 2026-10-07 before the fix.
//
// The second assertion matters as much as the first: while paused the
// component must not touch the network at all. The queue banner it used
// to feed cannot be reached from the paused panel, so the polling would
// be requests with no reader.

import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

// vi.hoisted because vi.mock factories are lifted above plain consts.
const mocks = vi.hoisted(() => ({
  getGpuAvailability: vi.fn(),
  annotateProteins: vi.fn(),
}));

vi.mock("next/navigation", () => ({ useRouter: () => ({ push: vi.fn() }) }));

vi.mock("next-intl", () => ({
  useLocale: () => "en",
  useTranslations: () => (key: string) => key,
}));

vi.mock("next/link", () => ({
  default: ({ href, children }: { href: string; children?: React.ReactNode }) => (
    <a href={href}>{children}</a>
  ),
}));

vi.mock("@/lib/api", () => ({
  publicBaseUrl: () => "/api-proxy",
  getGpuAvailability: mocks.getGpuAvailability,
  annotateProteins: mocks.annotateProteins,
  getJob: vi.fn(),
  launchPredictGoTerms: vi.fn(),
  resolvePredictionSet: vi.fn(),
}));

import { AnnotateForm } from "@/components/AnnotateForm";
import { LIVE_ANNOTATION_AVAILABLE } from "@/lib/campaign";

beforeEach(() => {
  mocks.getGpuAvailability.mockClear();
  mocks.annotateProteins.mockClear();
});

describe("AnnotateForm, predictor paused", () => {
  it("is the state this test is written for", () => {
    // Flip LIVE_ANNOTATION_AVAILABLE and this test tells you to revisit
    // it, rather than passing vacuously against a live form.
    expect(LIVE_ANNOTATION_AVAILABLE).toBe(false);
  });

  it("explains itself instead of offering a submit that fails", () => {
    render(<AnnotateForm />);
    expect(screen.getByText("annotatePausedTitle")).toBeInTheDocument();
    expect(screen.getByText("annotatePausedBody")).toBeInTheDocument();
    // No sequence box, so nothing to submit and no 409 to surface.
    expect(screen.queryByRole("textbox")).toBeNull();
  });

  it("makes no request while paused", () => {
    render(<AnnotateForm />);
    expect(mocks.getGpuAvailability).not.toHaveBeenCalled();
    expect(mocks.annotateProteins).not.toHaveBeenCalled();
  });

  it("offers somewhere real to go instead", () => {
    render(<AnnotateForm />);
    const api = screen.getByRole("link", { name: "annotatePausedApi" });
    expect(api.getAttribute("href")).toBe("/api-proxy/docs");
    const instrument = screen.getByRole("link", { name: "annotatePausedInstrument" });
    expect(instrument.getAttribute("href")).toBe("/en/instrument/benchmark");
  });
});
