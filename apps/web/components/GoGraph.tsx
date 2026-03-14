"use client";

import { useEffect, useRef, useState } from "react";
import type { GoSubgraph } from "@/lib/api";

// ── helpers ──────────────────────────────────────────────────────────────────

function truncate(text: string, max: number) {
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

// ── node classification ───────────────────────────────────────────────────────

function classifyNode(
  goId: string,
  isQuery: boolean,
  knownGoIds?: Set<string>,
  predictedGoIds?: Set<string>,
): "both" | "predicted_only" | "known_only" | "ancestor" {
  if (!isQuery) return "ancestor";
  const isKnown    = knownGoIds?.has(goId)    ?? false;
  const isPredicted = predictedGoIds?.has(goId) ?? true;
  if (isKnown && isPredicted) return "both";
  if (isKnown)                return "known_only";
  return "predicted_only";
}

// ── constants ────────────────────────────────────────────────────────────────

const ASPECT_LABELS: Record<string, string> = {
  F: "Molecular Function",
  P: "Biological Process",
  C: "Cellular Component",
};
const ASPECT_TAB_COLORS: Record<string, string> = {
  F: "text-purple-700 border-purple-600",
  P: "text-green-700  border-green-600",
  C: "text-orange-700 border-orange-600",
};

const NODE_LEGEND: { kind: string; label: string; bg: string; border: string }[] = [
  { kind: "both",           label: "Predicted + Known", bg: "#16a34a", border: "#14532d" },
  { kind: "predicted_only", label: "Predicted only",    bg: "#2563eb", border: "#1e3a8a" },
  { kind: "known_only",     label: "Known only",        bg: "#d97706", border: "#92400e" },
  { kind: "ancestor",       label: "Ancestor",          bg: "#f8fafc", border: "#94a3b8" },
];

// ── cytoscape style ───────────────────────────────────────────────────────────

const CY_STYLE = [
  {
    selector: "node",
    style: {
      label: "data(label)",
      "text-valign": "center",
      "text-halign": "center",
      "text-wrap": "wrap",
      "text-max-width": "130px",
      "font-size": "9px",
      shape: "roundrectangle",
      width: "label",
      height: "label",
      padding: "8px",
      "border-width": 1.5,
      "background-color": "#f8fafc",
      "border-color": "#94a3b8",
      color: "#475569",
    },
  },
  // ── node kinds ──
  {
    selector: "node.both",
    style: { "background-color": "#16a34a", "border-color": "#14532d", color: "#fff" },
  },
  {
    selector: "node.predicted_only",
    style: { "background-color": "#2563eb", "border-color": "#1e3a8a", color: "#fff" },
  },
  {
    selector: "node.known_only",
    style: { "background-color": "#d97706", "border-color": "#92400e", color: "#fff" },
  },
  {
    selector: "node.ancestor",
    style: { opacity: 0.7 },
  },
  // ── query emphasis ──
  {
    selector: "node.query",
    style: { "border-width": 3, "font-weight": "bold" },
  },
  // ── edges ──
  {
    selector: "edge",
    style: {
      "line-color": "#94a3b8",
      "target-arrow-color": "#94a3b8",
      "target-arrow-shape": "triangle",
      "arrow-scale": 0.8,
      "curve-style": "bezier",
      width: 1.5,
    },
  },
  {
    selector: 'edge[relation_type = "part_of"]',
    style: { "line-style": "dashed", "line-color": "#f59e0b", "target-arrow-color": "#f59e0b" },
  },
  {
    selector: 'edge[relation_type *= "regulates"]',
    style: { "line-style": "dotted", "line-color": "#60a5fa", "target-arrow-color": "#60a5fa" },
  },
  // ── interaction ──
  {
    selector: ".faded",
    style: { opacity: 0.12 },
  },
  {
    selector: "node:selected",
    style: { "border-color": "#1d4ed8", "border-width": 3 },
  },
];

// ── tooltip state type ────────────────────────────────────────────────────────

type TooltipState = {
  x: number; y: number;
  goId: string; name: string; kind: string; isQuery: boolean;
} | null;

// ── component ────────────────────────────────────────────────────────────────

interface Props {
  subgraph: GoSubgraph;
  knownGoIds?: Set<string>;
  predictedGoIds?: Set<string>;
  height?: number;
}

export default function GoGraph({ subgraph, knownGoIds, predictedGoIds, height = 420 }: Props) {
  const aspectsWithNodes = (["F", "P", "C"] as const).filter((asp) =>
    subgraph.nodes.some((n) => n.aspect === asp)
  );
  const [activeAspect, setActiveAspect] = useState<string>(aspectsWithNodes[0] ?? "F");
  const [tooltip, setTooltip] = useState<TooltipState>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef        = useRef<any>(null);

  useEffect(() => {
    if (aspectsWithNodes.length > 0 && !aspectsWithNodes.includes(activeAspect as any)) {
      setActiveAspect(aspectsWithNodes[0]);
    }
  }, [subgraph]);

  useEffect(() => {
    if (!containerRef.current || subgraph.nodes.length === 0) return;

    const nodeIds = new Set(
      subgraph.nodes.filter((n) => n.aspect === activeAspect).map((n) => String(n.id))
    );
    if (nodeIds.size === 0) return;

    const filteredEdges = subgraph.edges.filter(
      (e) => nodeIds.has(String(e.source)) && nodeIds.has(String(e.target))
    );

    let cy: any;

    async function init() {
      const cytoscape = (await import("cytoscape")).default;
      const dagre     = (await import("cytoscape-dagre")).default;
      cytoscape.use(dagre);

      const elements: any[] = [];

      for (const n of subgraph.nodes.filter((n) => n.aspect === activeAspect)) {
        const kind = classifyNode(n.go_id, n.is_query, knownGoIds, predictedGoIds);
        elements.push({
          data: {
            id:        String(n.id),
            label:     `${n.go_id}\n${truncate(n.name ?? "", 28)}`,
            go_id:     n.go_id,
            full_name: n.name ?? "",
            is_query:  n.is_query,
            kind,
          },
          classes: `${kind}${n.is_query ? " query" : " context"}`,
        });
      }

      for (const e of filteredEdges) {
        // API stores edges as child→parent; invert for TB layout (root at top)
        elements.push({
          data: {
            id:            `e-${e.source}-${e.target}-${e.relation_type}`,
            source:        String(e.target),
            target:        String(e.source),
            relation_type: e.relation_type,
          },
          classes: e.relation_type,
        });
      }

      if (cyRef.current) cyRef.current.destroy();

      cy = cytoscape({
        container: containerRef.current,
        elements,
        style: CY_STYLE as any,
        layout: {
          name:      "dagre",
          rankDir:   "TB",
          nodeSep:   50,
          rankSep:   80,
          padding:   24,
        } as any,
        minZoom:          0.2,
        maxZoom:          2.5,
        wheelSensitivity: 0.2,
        userZoomingEnabled:    true,
        userPanningEnabled:    true,
        boxSelectionEnabled:   false,
      });

      cy.ready(() => { cy.fit(undefined, 24); cy.center(); });

      // ── click: highlight neighborhood ──
      cy.on("tap", "node", (evt: any) => {
        const node = evt.target;
        cy.elements().removeClass("faded");
        cy.elements().difference(node.closedNeighborhood()).addClass("faded");
      });
      cy.on("tap", (evt: any) => {
        if (evt.target === cy) cy.elements().removeClass("faded");
      });

      // ── hover tooltip ──
      cy.on("mouseover", "node", (evt: any) => {
        const node    = evt.target;
        const pos     = evt.renderedPosition;
        const bb      = containerRef.current!.getBoundingClientRect();
        setTooltip({
          x:       pos.x,
          y:       pos.y,
          goId:    node.data("go_id"),
          name:    node.data("full_name"),
          kind:    node.data("kind"),
          isQuery: node.data("is_query"),
        });
      });
      cy.on("mouseout", "node", () => setTooltip(null));

      cyRef.current = cy;
    }

    init();

    return () => {
      setTooltip(null);
      if (cyRef.current) { cyRef.current.destroy(); cyRef.current = null; }
    };
  }, [subgraph, activeAspect, knownGoIds, predictedGoIds]);

  if (subgraph.nodes.length === 0) {
    return (
      <div className="flex items-center justify-center h-24 text-sm text-gray-400">
        No graph data available.
      </div>
    );
  }

  return (
    <div className="rounded-lg border bg-white overflow-hidden">
      {/* Aspect tabs */}
      {aspectsWithNodes.length > 1 && (
        <div className="flex gap-1 border-b px-3 pt-2">
          {aspectsWithNodes.map((asp) => (
            <button
              key={asp}
              onClick={() => setActiveAspect(asp)}
              className={`px-3 py-1.5 text-xs font-medium border-b-2 transition-colors ${
                activeAspect === asp
                  ? (ASPECT_TAB_COLORS[asp] ?? "border-blue-600 text-blue-600")
                  : "border-transparent text-gray-400 hover:text-gray-600"
              }`}
            >
              {ASPECT_LABELS[asp] ?? asp}
            </button>
          ))}
        </div>
      )}

      {/* Canvas + tooltip */}
      <div className="relative">
        <div ref={containerRef} style={{ height }} />

        {tooltip && (
          <div
            className="pointer-events-none absolute z-10 rounded-md border bg-white px-3 py-2 text-xs shadow-lg max-w-[240px]"
            style={{ left: tooltip.x + 12, top: tooltip.y + 12 }}
          >
            <p className="font-mono font-semibold text-blue-600">{tooltip.goId}</p>
            {tooltip.name && <p className="text-gray-700 mt-0.5">{tooltip.name}</p>}
            <p className="mt-1 text-gray-400 capitalize">{tooltip.kind.replace(/_/g, " ")}</p>
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 px-4 py-2 border-t bg-gray-50 text-xs text-gray-500">
        <span className="font-medium text-gray-600">Nodes:</span>
        {NODE_LEGEND.map(({ kind, label, bg, border }) => (
          <span key={kind} className="flex items-center gap-1.5">
            <span className="inline-block w-3 h-3 rounded-sm border"
              style={{ backgroundColor: bg, borderColor: border }} />
            {label}
          </span>
        ))}
        <span className="ml-auto flex items-center gap-3">
          <span className="flex items-center gap-1">
            <span className="inline-block w-4 h-0.5 bg-gray-400 rounded" /> is_a
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-4 border-t border-dashed border-yellow-400" /> part_of
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block w-4 border-t border-dotted border-blue-400" /> regulates
          </span>
        </span>
      </div>
    </div>
  );
}
