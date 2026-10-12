/**
 * The frame timeline against the series it actually receives.
 *
 * The component was written for "an axis with a band and a dozen ticks", which
 * its own note says. The record now hands it 141 marks, 75 of them annotation
 * releases, and labelling all 75 with a 70px minimum gap needs 5,250px of label
 * room inside a 900-wide viewBox that scales to its container: about 62 labels
 * were drawn past the right edge and clipped away in silence, and the dozen that
 * survived sat at uniform 70px intervals with no relation to the dates beneath.
 *
 * These assert the two halves of the contract: every release keeps its dot, and
 * only the load-bearing ones get a label.
 */

import { render, screen } from "@testing-library/react";
import { NextIntlClientProvider } from "next-intl";
import { describe, expect, it } from "vitest";

import { FrameTimeline } from "@/components/FrameTimeline";
import type { GraphTimeline, TimelineMark } from "@/lib/graph";
import messages from "@/messages/en.json";

/** 75 annotation releases a month apart, shaped like the real payload. */
function serie(): GraphTimeline {
  const marks: TimelineMark[] = [];
  for (let i = 0; i < 75; i += 1) {
    const y = 2016 + Math.floor(i / 12);
    const m = String((i % 12) + 1).padStart(2, "0");
    const role: TimelineMark["role"] =
      i === 70 ? "window_start" : i === 71 ? "inside" : i === 72 ? "window_end" : i < 70 ? "before" : "beyond";
    marks.push({
      kind: "annotation_set",
      label: `GOA ${156 + i}`,
      date: `${y}-${m}-08`,
      role,
      in_window: i >= 70 && i <= 72,
      is_pivot: false,
    });
  }
  marks.push({
    kind: "ontology_snapshot",
    label: "releases/2025-03-16",
    date: "2022-02-16",
    role: "pivot",
    in_window: true,
    is_pivot: true,
  });
  return { window: { from: "2021-11-08", to: "2022-01-08" }, marks };
}

function pinta(timeline: GraphTimeline) {
  return render(
    <NextIntlClientProvider locale="en" messages={messages} onError={() => {}}>
      <FrameTimeline timeline={timeline} />
    </NextIntlClientProvider>,
  );
}

describe("FrameTimeline with the whole series", () => {
  it("keeps one dot per release, so the shape of the series survives", () => {
    const { container } = pinta(serie());
    expect(container.querySelectorAll("circle").length).toBeGreaterThanOrEqual(75);
  });

  it("labels only the load-bearing marks, not all 75", () => {
    pinta(serie());
    const etiquetas = screen.queryAllByText(/^GOA \d+$/);
    expect(etiquetas.length).toBeLessThanOrEqual(6);
    expect(etiquetas.length).toBeGreaterThan(0);
  });

  it("names both ends of the window and what sits inside it", () => {
    pinta(serie());
    for (const l of ["GOA 226", "GOA 227", "GOA 228"]) {
      expect(screen.getByText(l)).toBeTruthy();
    }
  });

  it("names the release before the window and the first one past it", () => {
    pinta(serie());
    expect(screen.getByText("GOA 225")).toBeTruthy();
    expect(screen.getByText("GOA 229")).toBeTruthy();
  });

  it("leaves every label inside the drawing, which is what used to fail", () => {
    const { container } = pinta(serie());
    const svg = container.querySelector("svg");
    const ancho = Number(svg?.getAttribute("viewBox")?.split(" ")[2] ?? 0);
    expect(ancho).toBeGreaterThan(0);
    for (const t of Array.from(container.querySelectorAll("text"))) {
      const x = Number(t.getAttribute("x") ?? "0");
      expect(x).toBeLessThanOrEqual(ancho);
      expect(x).toBeGreaterThanOrEqual(0);
    }
  });

  it("draws no label at an undefined x", () => {
    const { container } = pinta(serie());
    for (const t of Array.from(container.querySelectorAll("text"))) {
      expect(t.getAttribute("x")).not.toBe("undefined");
    }
  });
});
