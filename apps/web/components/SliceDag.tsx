"use client";

// FARM-UI.4 cytoscape-dagre adapter for the agent-farm slice catalog.
// Mirrors the patterns in GoGraph.tsx (cytoscape-dagre layout, hover
// tooltip, click-to-fade neighborhood) but adapts the data model to the
// PlanSlice shape returned by /plan on the farm-api sidecar.
//
// Node color encodes status (pending / in_progress / blocked / done /
// deferred); edges encode the deps[] array from each slice. Critical-
// path edges receive an extra class so the parent page can toggle
// visibility without re-running the layout.

import { useEffect, useMemo, useRef, useState } from "react";
import type cytoscape from "cytoscape";
import type { FarmPlanSlice } from "@/lib/farmApi";

// ── status palette ───────────────────────────────────────────────────────────
//
// Anchored on scripts/lib/plan_parser.STATUS_GLYPH:
//   pending     ⬜  slate     (cool neutral, default catalog state)
//   in_progress 🟡  amber     (active work)
//   blocked     🟥  red       (waiting on a peer slice)
//   done        ✅  green     (shipped)
//   deferred    ⬛  zinc-dark (intentionally parked)
// Colors match FarmStatusPill / StatusBadge color vocabulary so the
// dashboard reads as one product.

export const SLICE_STATUS_COLOR: Record<
  string,
  { bg: string; border: string; fg: string; glyph: string; label: string }
> = {
  pending: {
    bg: "#f1f5f9",
    border: "#94a3b8",
    fg: "#334155",
    glyph: "⬜",
    label: "Pending",
  },
  in_progress: {
    bg: "#fde68a",
    border: "#b45309",
    fg: "#78350f",
    glyph: "🟡",
    label: "In progress",
  },
  blocked: {
    bg: "#fecaca",
    border: "#b91c1c",
    fg: "#7f1d1d",
    glyph: "🟥",
    label: "Blocked",
  },
  done: {
    bg: "#bbf7d0",
    border: "#15803d",
    fg: "#14532d",
    glyph: "✅",
    label: "Done",
  },
  deferred: {
    bg: "#d4d4d8",
    border: "#3f3f46",
    fg: "#18181b",
    glyph: "⬛",
    label: "Deferred",
  },
};

const FALLBACK_STATUS = SLICE_STATUS_COLOR.pending;

function statusStyle(status: string) {
  return SLICE_STATUS_COLOR[status] ?? FALLBACK_STATUS;
}

// ── cytoscape stylesheet ─────────────────────────────────────────────────────

const CY_STYLE = [
  {
    selector: "node",
    style: {
      label: "data(label)",
      "text-valign": "center",
      "text-halign": "center",
      "text-wrap": "wrap",
      "text-max-width": "140px",
      "font-size": "10px",
      "font-family": "ui-monospace, SFMono-Regular, Menlo, monospace",
      shape: "roundrectangle",
      width: "label",
      height: "label",
      padding: "10px",
      "border-width": 1.5,
      "background-color": "data(bg)",
      "border-color": "data(border)",
      color: "data(fg)",
    },
  },
  {
    selector: "node.priority-P0",
    style: { "border-width": 3 },
  },
  {
    selector: "node.priority-P1",
    style: { "border-width": 2.5 },
  },
  {
    selector: "node.requires-human",
    style: { "border-style": "dashed" },
  },
  {
    selector: "edge",
    style: {
      "line-color": "#cbd5e1",
      "target-arrow-color": "#cbd5e1",
      "target-arrow-shape": "triangle",
      "arrow-scale": 0.9,
      "curve-style": "bezier",
      width: 1.5,
    },
  },
  {
    selector: "edge.critical",
    style: {
      "line-color": "#dc2626",
      "target-arrow-color": "#dc2626",
      width: 3,
    },
  },
  {
    selector: "node.critical",
    style: {
      "border-color": "#dc2626",
      "border-width": 3.5,
    },
  },
  {
    selector: ".faded",
    style: { opacity: 0.12 },
  },
  {
    selector: "node:selected",
    style: { "border-color": "#1d4ed8", "border-width": 3.5 },
  },
];

// ── tooltip ──────────────────────────────────────────────────────────────────

export type SliceTooltip = {
  x: number;
  y: number;
  slice: FarmPlanSlice;
} | null;

// ── props ────────────────────────────────────────────────────────────────────

interface Props {
  slices: FarmPlanSlice[];
  criticalPathIds: Set<string>;
  height?: number;
  onHover?: (state: SliceTooltip) => void;
  // Accessible label for the canvas (cytoscape renders to <canvas>, which
  // has no inherent text alternative; supplying ariaLabel + a visually
  // hidden slice summary satisfies WCAG 1.1.1 for screen-reader users).
  ariaLabel?: string;
  // Translated string for the visually-hidden "X slice(s) in this graph"
  // intro line. The parent owns i18n so we accept a pre-built summary.
  srSummary?: string;
  // Translated section title for the visually-hidden slice list. Used in
  // the screen-reader-only DOM region only.
  srListTitle?: string;
}

// Truncate the inline node label without breaking the GO-style id prefix.
function truncate(text: string, max: number) {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

// ── component ────────────────────────────────────────────────────────────────

export default function SliceDag({
  slices,
  criticalPathIds,
  height = 560,
  onHover,
  ariaLabel,
  srSummary,
  srListTitle,
}: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<{ destroy: () => void } | null>(null);
  const [internalTip, setInternalTip] = useState<SliceTooltip>(null);

  // Derive (id -> slice) once so the cytoscape callbacks can look up the
  // full record without keeping the array in a closure that changes on
  // every filter chip click.
  const byId = useMemo(() => {
    const map = new Map<string, FarmPlanSlice>();
    for (const s of slices) map.set(s.id, s);
    return map;
  }, [slices]);

  const sliceIds = useMemo(() => new Set(slices.map((s) => s.id)), [slices]);

  useEffect(() => {
    if (!containerRef.current || slices.length === 0) return;

    let cancelled = false;

    async function init() {
      const cytoscape = (await import("cytoscape")).default;
      const dagre = (await import("cytoscape-dagre")).default;
      cytoscape.use(dagre);
      if (cancelled) return;

      const elements: cytoscape.ElementDefinition[] = [];
      for (const s of slices) {
        const style = statusStyle(s.status);
        const titleCore = s.title.includes("—")
          ? s.title.split("—").slice(1).join("—").trim()
          : s.title;
        elements.push({
          data: {
            id: s.id,
            label: `${s.id}\n${truncate(titleCore || s.id, 32)}`,
            bg: style.bg,
            border: style.border,
            fg: style.fg,
            status: s.status,
            priority: s.priority ?? "P2",
          },
          classes: [
            `status-${s.status}`,
            `priority-${s.priority ?? "P2"}`,
            s.requires_human ? "requires-human" : "",
            criticalPathIds.has(s.id) ? "critical" : "",
          ]
            .filter(Boolean)
            .join(" "),
        });
      }

      for (const s of slices) {
        for (const dep of s.deps) {
          // Skip edges that point at filtered-out slices; the parent
          // already trims the deps list, but this is a defensive guard.
          if (!sliceIds.has(dep)) continue;
          const isCritical =
            criticalPathIds.has(s.id) && criticalPathIds.has(dep);
          elements.push({
            data: {
              id: `e-${dep}->${s.id}`,
              source: dep,
              target: s.id,
            },
            classes: isCritical ? "critical" : "",
          });
        }
      }

      if (cyRef.current) cyRef.current.destroy();

      const cy = cytoscape({
        container: containerRef.current,
        elements,
        // The cytoscape types model `style` as a Stylesheet with a
        // narrowly-typed selector list; our property bag matches the
        // runtime expectation but trips the union resolution. Match
        // GoGraph.tsx and cast through the runtime shape.
        style: CY_STYLE as unknown as cytoscape.StylesheetCSS[],
        layout: {
          name: "dagre",
          rankDir: "LR",
          nodeSep: 32,
          rankSep: 96,
          padding: 28,
        } as unknown as cytoscape.LayoutOptions,
        minZoom: 0.15,
        maxZoom: 2.5,
        wheelSensitivity: 0.2,
        userZoomingEnabled: true,
        userPanningEnabled: true,
        boxSelectionEnabled: false,
      });

      cy.ready(() => {
        cy.fit(undefined, 32);
        cy.center();
      });

      // P1.5: tap on a node fades neighborhood AND surfaces the tooltip
      // so touch users get the same metadata reveal as desktop hover.
      // Tapping the canvas background clears both the fade and the
      // tooltip. Real cytoscape tap events carry `renderedPosition`;
      // when absent (e.g. an event emitted via the test seam) we fall
      // back to the node's own renderedPosition so the tooltip still
      // anchors near the tapped node.
      cy.on(
        "tap",
        "node",
        (evt: {
          target: cytoscape.NodeSingular;
          renderedPosition?: { x: number; y: number };
        }) => {
          const node = evt.target;
          cy.elements().removeClass("faded");
          cy.elements().difference(node.closedNeighborhood()).addClass("faded");
          const slice = byId.get(node.id());
          if (!slice) return;
          const pos = evt.renderedPosition ?? node.renderedPosition();
          const state: SliceTooltip = { x: pos.x, y: pos.y, slice };
          setInternalTip(state);
          onHover?.(state);
        },
      );
      cy.on("tap", (evt: { target: unknown }) => {
        if (evt.target === cy) {
          cy.elements().removeClass("faded");
          setInternalTip(null);
          onHover?.(null);
        }
      });

      cy.on(
        "mouseover",
        "node",
        (evt: {
          target: cytoscape.NodeSingular;
          renderedPosition: { x: number; y: number };
        }) => {
          const node = evt.target;
          const id = node.id();
          const slice = byId.get(id);
          if (!slice) return;
          const pos = evt.renderedPosition;
          const state: SliceTooltip = { x: pos.x, y: pos.y, slice };
          setInternalTip(state);
          onHover?.(state);
        },
      );
      cy.on("mouseout", "node", () => {
        setInternalTip(null);
        onHover?.(null);
      });

      cyRef.current = cy as unknown as { destroy: () => void };

      // Expose the live cytoscape instance on a window-scoped hook so
      // playwright (which cannot hit-test the cytoscape <canvas>) can
      // exercise the tap pathway in tests. Production code never reads
      // this handle; it is purely a test seam.
      if (typeof window !== "undefined") {
        (window as unknown as { __sliceDagCy?: cytoscape.Core }).__sliceDagCy =
          cy;
      }
    }

    void init();

    return () => {
      cancelled = true;
      setInternalTip(null);
      if (cyRef.current) {
        cyRef.current.destroy();
        cyRef.current = null;
      }
      if (typeof window !== "undefined") {
        delete (window as unknown as { __sliceDagCy?: unknown }).__sliceDagCy;
      }
    };
  }, [slices, criticalPathIds, byId, sliceIds, onHover]);

  if (slices.length === 0) {
    return (
      <div
        className="flex items-center justify-center rounded-lg border bg-white text-sm text-slate-600"
        style={{ height }}
        data-testid="slice-dag-empty"
      >
        No slices match the current filters.
      </div>
    );
  }

  // Status tally + top in-progress / blocked summary for the screen-
  // reader-only region. Derived from the same `slices` prop the canvas
  // visualises, so the two views stay in sync. We avoid fabricating
  // anything beyond what is in `slices`.
  const statusTally: Record<string, number> = {};
  for (const s of slices) {
    statusTally[s.status] = (statusTally[s.status] ?? 0) + 1;
  }
  const inProgressSlices = slices.filter((s) => s.status === "in_progress");
  const blockedSlices = slices.filter((s) => s.status === "blocked");
  const accessibleLabel =
    ariaLabel ?? `Slice DAG, ${slices.length} slices`;
  const accessibleSummary =
    srSummary ?? `${slices.length} slices in this graph.`;
  const accessibleListTitle = srListTitle ?? "Slice list";

  return (
    <div className="relative rounded-lg border bg-white overflow-hidden">
      <div
        ref={containerRef}
        role="img"
        aria-label={accessibleLabel}
        style={{ height }}
        data-testid="slice-dag-canvas"
      />
      {/* Visually-hidden text alternative so screen readers receive the
          slice catalog rather than an opaque <canvas>. Mirrors WCAG 1.1.1
          guidance for canvas-based visualisations. */}
      <div className="sr-only" data-testid="slice-dag-sr-summary">
        <p>{accessibleSummary}</p>
        <p>{accessibleListTitle}:</p>
        <ul>
          {Object.entries(statusTally).map(([status, count]) => (
            <li key={status}>
              {status}: {count}
            </li>
          ))}
        </ul>
        {inProgressSlices.length > 0 && (
          <>
            <p>In progress:</p>
            <ul>
              {inProgressSlices.slice(0, 3).map((s) => (
                <li key={s.id}>
                  {s.id}: {s.title}
                </li>
              ))}
            </ul>
          </>
        )}
        {blockedSlices.length > 0 && (
          <>
            <p>Blocked:</p>
            <ul>
              {blockedSlices.slice(0, 3).map((s) => (
                <li key={s.id}>
                  {s.id}: {s.title}
                </li>
              ))}
            </ul>
          </>
        )}
      </div>
      {internalTip && (
        <div
          className="pointer-events-none absolute z-20 rounded-md border bg-white px-3 py-2 text-xs shadow-lg max-w-[280px]"
          style={{ left: internalTip.x + 12, top: internalTip.y + 12 }}
          data-testid="slice-dag-tooltip"
        >
          <p className="font-mono font-semibold text-slate-800">
            {internalTip.slice.id}
          </p>
          <p className="mt-0.5 text-slate-700">{internalTip.slice.title}</p>
          <dl className="mt-1.5 grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5 text-[11px] text-slate-600">
            <dt className="font-medium">Loop</dt>
            <dd className="font-mono">{internalTip.slice.loop}</dd>
            <dt className="font-medium">Phase</dt>
            <dd className="font-mono">{internalTip.slice.phase}</dd>
            <dt className="font-medium">Priority</dt>
            <dd className="font-mono">{internalTip.slice.priority ?? "-"}</dd>
            <dt className="font-medium">Hours</dt>
            <dd className="font-mono">
              {internalTip.slice.estimated_hours ?? "-"}
            </dd>
            <dt className="font-medium">Deps</dt>
            <dd className="font-mono">
              {internalTip.slice.deps.length === 0
                ? "(root)"
                : internalTip.slice.deps.join(", ")}
            </dd>
          </dl>
          {internalTip.slice.acceptance && (
            <p
              className="mt-1.5 border-t pt-1.5 text-[11px] text-slate-600 line-clamp-4 whitespace-pre-wrap"
              style={{
                display: "-webkit-box",
                WebkitLineClamp: 4,
                WebkitBoxOrient: "vertical",
                overflow: "hidden",
              }}
            >
              {internalTip.slice.acceptance}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
