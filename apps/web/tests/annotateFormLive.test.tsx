// The annotate form now that the predictor answers.
//
// This file used to be annotateFormPaused.test.tsx and its first assertion
// existed to break here: "Flip LIVE_ANNOTATION_AVAILABLE and this test tells
// you to revisit it, rather than passing vacuously against a live form." It
// did, so this is the revisit.
//
// What it was written for still matters and is kept: before the pause, a
// visitor who submitted got the API's own 409 body rendered verbatim, "No
// annotation sets available. Load GO annotations first." -- an instruction to
// a database operator, shown to whoever was evaluating the project. The pause
// existed so that could not happen. It can no longer happen for the other
// reason: measured 2026-10-29, the newest annotation set is GOA 235 with
// 8.118.592 annotations and the config the endpoint resolves has 684.048
// vectors, so the precondition behind that 409 is satisfied.
//
// The live form has TWO ways to be unusable and they say different things.
// The predictor being paused is gone; the shared GPU being busy is not, and
// it is the one a visitor meets tonight. These tests cover both branches,
// because a single flag collapsing them is what the component's own comment
// warns against.

import { render, screen, waitFor } from "@testing-library/react";
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

describe("AnnotateForm, predictor live and GPU free", () => {
  it("is the state this test is written for", () => {
    // The mirror of the assertion this file used to carry. Flip it back and
    // this says so, instead of a live-form test passing against a paused one.
    expect(LIVE_ANNOTATION_AVAILABLE).toBe(true);
  });

  it("does not claim to be paused", () => {
    mocks.getGpuAvailability.mockResolvedValue({ busy: false, queued: 0 });
    render(<AnnotateForm />);
    expect(screen.queryByText("annotatePausedTitle")).not.toBeInTheDocument();
    expect(screen.queryByText("annotatePausedBody")).not.toBeInTheDocument();
  });

  it("leaves the input usable", () => {
    mocks.getGpuAvailability.mockResolvedValue({ busy: false, queued: 0 });
    render(<AnnotateForm />);
    const box = screen.getByRole("textbox");
    expect(box).toBeInTheDocument();
    expect(box).not.toBeDisabled();
  });

  it("asks the backend whether the card is free", async () => {
    // The inverse of "makes no request while paused". While paused the
    // polling would have been requests with no reader; now the answer is
    // what decides which banner the visitor gets, so it has to be asked.
    mocks.getGpuAvailability.mockResolvedValue({ busy: false, queued: 0 });
    render(<AnnotateForm />);
    await waitFor(() => expect(mocks.getGpuAvailability).toHaveBeenCalled());
  });
});

describe("AnnotateForm, predictor live but the GPU is busy", () => {
  // The branch a visitor meets while a long job runs, which on 2026-10-29 was
  // 10.689 embedding batches on the compute node. Two different facts needed
  // two banners: this one must not say the predictor is paused, because it is
  // not, and it must still refuse the submission.
  const busy = { busy: true, queued: 3, running_fresh: 1, active_operation: "compute_embeddings" };

  it("blocks without claiming the predictor is paused", async () => {
    mocks.getGpuAvailability.mockResolvedValue(busy);
    render(<AnnotateForm />);
    await waitFor(() => expect(mocks.getGpuAvailability).toHaveBeenCalled());
    expect(screen.queryByText("annotatePausedTitle")).not.toBeInTheDocument();
  });

  it("keeps the submit disabled while the card is taken", async () => {
    mocks.getGpuAvailability.mockResolvedValue(busy);
    render(<AnnotateForm />);
    await waitFor(() => {
      const submits = screen
        .getAllByRole("button")
        .filter((b) => (b as HTMLButtonElement).type === "submit");
      expect(submits.length).toBeGreaterThan(0);
      for (const b of submits) expect(b).toBeDisabled();
    });
  });

  it("never reaches the 409 this file was written about", async () => {
    mocks.getGpuAvailability.mockResolvedValue(busy);
    render(<AnnotateForm />);
    await waitFor(() => expect(mocks.getGpuAvailability).toHaveBeenCalled());
    expect(mocks.annotateProteins).not.toHaveBeenCalled();
  });
});
