"use client";

/**
 * The experiment graph.
 *
 * WHAT THIS SURFACE IS FOR. A reader arriving at a table of scores cannot
 * see what the scores are evidence FOR. The retired ladder answered that
 * with progress, a count of finished jobs over a declared grid, which is a
 * fact about a queue and not about knowledge: it stayed true after every
 * result behind it was deleted. This page answers it with structure
 * instead. Every decision the
 * pipeline makes is a NODE, the edge into each node carries a STRENGTH
 * saying what the record can support about that decision, and the strength
 * is the first thing visible on every row.
 *
 * HOW IT IS ORDERED. Frame first and compact, because a score that cannot
 * be attributed to a frame cannot be compared to anything, and the count of
 * unsealed result rows is the single number that decides whether the rest
 * of the page is worth reading. Then the nodes as a pipeline. Then the nine
 * panels. Then what is blocked, as a list with a precondition per line, so
 * the page ends on something a reader can act on.
 *
 * WHAT IT NEVER DOES. It never aggregates the nine panels. There is no
 * "overall" cell anywhere, no mean across categories, no mean across
 * aspects: cardinality is a vector over the nine and a scalar built from
 * it is a claim the model does not license. The one place an ordering
 * across levels appears, the wide matrix, is ordered by a panel the reader
 * picks, which is a statement about that panel alone.
 *
 * It also computes nothing. Every value rendered here came out of
 * GET /v1/graph. When a cell looks wrong the query behind it is one hop
 * away, not behind a client-side derivation.
 */

import { Fragment, useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useLocale, useTranslations } from "next-intl";
import {
  Ban,
  Circle,
  CircleCheckBig,
  Gauge,
  RotateCcw,
  ChevronRight,
  RefreshCw,
  type LucideIcon,
} from "lucide-react";
import { Skeleton } from "@/components/Skeleton";
import { ApiError } from "@/lib/api";
import {
  EDGE_STRENGTHS,
  PANEL_ASPECTS,
  PANEL_CATEGORIES,
  getGraph,
  indexPanelResults,
  indexPanels,
  isEmptyGraph,
  panelKey,
  panelLevels,
  panelSummary,
  type EdgeStrength,
  type GraphNode,
  type GraphPanel,
  type GraphResponse,
} from "@/lib/graph";

// ── Presentation of the five edge strengths ──────────────────────────────
//
// Colour alone is not a channel. An operator scanning a pipeline for the
// one node that cannot answer has to find it without resolving hue, so
// every strength also carries its own SHAPE (square with a heavy left bar,
// pill, dashed, diagonally striped, double-weight rectangle) and its own
// icon, and the word itself is always printed beside them. Four redundant
// channels, any one of which is enough.

type StrengthStyle = {
  chip: string;
  icon: LucideIcon;
  /** Diagonal hatching, the shape channel for `unpowered`. */
  striped: boolean;
  /** The rail down the left of the node row. */
  rail: string;
};

const STRENGTH_STYLE: Record<EdgeStrength, StrengthStyle> = {
  measured: {
    chip: "rounded-md border-2 border-emerald-600 bg-emerald-50 text-emerald-900",
    icon: CircleCheckBig,
    striped: false,
    rail: "bg-emerald-600",
  },
  chosen: {
    chip: "rounded-full border border-blue-400 bg-blue-50 text-blue-900",
    icon: Circle,
    striped: false,
    rail: "bg-blue-400",
  },
  inherited: {
    chip: "rounded-md border border-dashed border-slate-400 bg-white text-slate-700",
    icon: RotateCcw,
    striped: false,
    rail: "bg-slate-300",
  },
  unpowered: {
    chip: "rounded-md border border-amber-500 text-amber-900",
    icon: Gauge,
    striped: true,
    rail: "bg-amber-500",
  },
  blocked: {
    chip: "rounded-none border border-rose-400 border-l-4 border-l-rose-600 bg-rose-50 text-rose-900",
    icon: Ban,
    striped: false,
    rail: "bg-rose-600",
  },
};

/** amber-100 hatching, drawn rather than tinted so it survives greyscale. */
const HATCH: React.CSSProperties = {
  backgroundImage:
    "repeating-linear-gradient(135deg, #fffbeb 0 3px, #fde68a 3px 6px)",
};

/** A strength the endpoint sent that this build does not know how to draw. */
const UNKNOWN_STYLE: StrengthStyle = {
  chip: "rounded-md border border-slate-400 bg-slate-100 text-slate-700",
  icon: Circle,
  striped: false,
  rail: "bg-slate-400",
};

function styleFor(strength: string): StrengthStyle {
  return STRENGTH_STYLE[strength as EdgeStrength] ?? UNKNOWN_STYLE;
}

function StrengthChip({ strength, size = "sm" }: { strength: string; size?: "sm" | "xs" }) {
  const s = styleFor(strength);
  const Icon = s.icon;
  const pad = size === "xs" ? "px-1.5 py-0.5 text-[10px]" : "px-2 py-0.5 text-xs";
  return (
    <span
      className={`inline-flex items-center gap-1 font-medium whitespace-nowrap ${pad} ${s.chip}`}
      style={s.striped ? HATCH : undefined}
    >
      <Icon className={size === "xs" ? "h-3 w-3 shrink-0" : "h-3.5 w-3.5 shrink-0"} aria-hidden />
      {strength}
    </span>
  );
}

// ── Small shared pieces ──────────────────────────────────────────────────

/** A short, copyable identifier. Full value stays in the title attribute. */
function Ident({ value, head = 12 }: { value: string; head?: number }) {
  const short = value.length > head ? `${value.slice(0, head)}…` : value;
  return (
    <span className="font-mono text-xs text-slate-700" title={value}>
      {short}
    </span>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-[11px] uppercase tracking-wider text-slate-500">{label}</dt>
      <dd className="mt-0.5 text-sm text-slate-900 break-words">{children}</dd>
    </div>
  );
}

/**
 * Field names as chips, with a cap on how many are drawn.
 *
 * The cap exists for the constant half only, and the caller sets it. The
 * fields that VARY are what the node's comparison is about and are never
 * elided; the ones held constant can run to a dozen on a representation,
 * and a dozen chips turn one row into three without adding anything a
 * reader is scanning for. The remainder is never dropped: it travels in
 * the overflow chip's title.
 */
function FieldList({
  fields,
  empty,
  max,
}: {
  fields: string[];
  empty: string;
  max?: number;
}) {
  if (fields.length === 0) {
    return <span className="text-xs text-slate-400">{empty}</span>;
  }
  const shown = max === undefined ? fields : fields.slice(0, max);
  const rest = fields.slice(shown.length);
  return (
    <span className="flex flex-wrap gap-1">
      {shown.map((f) => (
        <span
          key={f}
          className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-700"
        >
          {f}
        </span>
      ))}
      {rest.length > 0 && (
        <span
          className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[11px] text-slate-500"
          title={rest.join(", ")}
        >
          +{rest.length}
        </span>
      )}
    </span>
  );
}

/**
 * Where a node's levels are instantiated, so a reader can go and look.
 *
 * Navigation only. Nothing about the graph is read from this map: a node
 * whose key is absent simply renders without a link.
 */
const NODE_SURFACE: Record<string, string> = {
  substrate: "/instrument/embeddings",
  bank: "/instrument/annotations",
  retriever: "/instrument/functional-annotation",
  generator: "/instrument/functional-annotation",
  scoring: "/instrument/scoring",
  "re-ranking": "/instrument/reranker",
  reranking: "/instrument/reranker",
};

// ── Frame ────────────────────────────────────────────────────────────────

function FrameCard({ frame }: { frame: GraphResponse["frame"] }) {
  const t = useTranslations("graph");
  const locale = useLocale();
  const n = (v: number) => v.toLocaleString(locale);

  // Declared and populated are different questions, and the record answers
  // them differently: a frame can hold a window, a pivot, an accretion table
  // and a query set and still not be DECLARED, because declaring it is what
  // the result rows do when they seal to it. Collapsing the card to a bare
  // warning in that state would hide the window, the pivot, the sha and the
  // unsealed count, which is every fact a reader needs in order to see why
  // the frame is not declared. The card is only reduced to the warning when
  // there is genuinely nothing behind it.
  const populated =
    frame.evaluation_set_id !== null ||
    frame.window !== null ||
    frame.pivot_snapshot !== null ||
    frame.information_accretion_set !== null ||
    frame.query_set !== null ||
    frame.sealed_rows > 0 ||
    frame.unsealed_rows > 0;

  if (!frame.declared && !populated) {
    return (
      <section
        className="rounded-lg border border-amber-300 bg-amber-50 p-4"
        data-testid="graph-frame"
      >
        <h2 className="text-sm font-semibold uppercase tracking-wider text-amber-900">
          {t("frameHeading")}
        </h2>
        <p className="mt-1 text-sm text-amber-900">{t("frameUndeclared")}</p>
      </section>
    );
  }

  const unsealed = frame.unsealed_rows;
  const bad = unsealed > 0;

  return (
    <section
      className="rounded-lg border border-slate-200 bg-white"
      data-testid="graph-frame"
    >
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 px-4 py-2.5">
        <div className="flex flex-wrap items-baseline gap-3">
          {/* The heading is a word of jargon on a page a newcomer reads
              first. It gets its sentence beside it rather than in a tooltip,
              because a reader who does not know what a frame is cannot judge
              anything else the page says. */}
          <h2
            className="cursor-help text-sm font-semibold uppercase tracking-wider text-slate-500 decoration-dotted underline-offset-4 hover:underline"
            title={t("frameWhat")}
          >
            {t("frameHeading")}
          </h2>
          <span className="font-mono text-base font-semibold text-slate-900">
            {frame.window ?? t("unrecorded")}
          </span>
          {/* The dates, beside the release numbers rather than instead of them.
              The numbers are what a payload names and what a job reports; the
              dates are what tells a reader how long the window had to fill. */}
          {frame.window_span && (
            <span className="font-mono text-xs text-slate-500">
              {frame.window_span.from} → {frame.window_span.to}
              <span className="ml-1.5 text-slate-400">
                ({frame.window_span.months} {t("months")})
              </span>
            </span>
          )}
          {frame.window_role && (
            <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700 ring-1 ring-inset ring-slate-200">
              {frame.window_role}
            </span>
          )}
          {frame.mode && (
            <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700 ring-1 ring-inset ring-slate-200">
              {frame.mode}
            </span>
          )}
          {!frame.declared && (
            <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-900 ring-1 ring-inset ring-amber-300">
              {t("frameNotDeclared")}
            </span>
          )}
        </div>

        {/* Attribution, before any score. Sealed and unsealed are shown as a
            pair: one number alone reads as a total, and the ratio is what
            says whether the board below can be compared to anything. */}
        <div className="flex items-stretch gap-2" data-testid="graph-sealing">
          <div className="rounded-md border border-slate-200 px-3 py-1 text-right">
            <div className="text-[11px] uppercase tracking-wider text-slate-500">
              {t("sealed")}
            </div>
            <div className="font-mono text-lg leading-tight text-slate-900">
              {n(frame.sealed_rows)}
            </div>
          </div>
          <div
            className={`rounded-md border px-3 py-1 text-right ${
              bad ? "border-rose-300 bg-rose-50" : "border-slate-200"
            }`}
          >
            <div
              className={`text-[11px] uppercase tracking-wider ${
                bad ? "text-rose-700" : "text-slate-500"
              }`}
            >
              {t("unsealed")}
            </div>
            <div
              className={`font-mono text-lg leading-tight ${
                bad ? "font-semibold text-rose-800" : "text-slate-900"
              }`}
            >
              {n(unsealed)}
            </div>
          </div>
        </div>
      </div>

      {!frame.declared && (
        <p
          className="border-b border-amber-200 bg-amber-50 px-4 py-2 text-xs text-amber-900"
          role="status"
        >
          {t("frameUndeclared")}
        </p>
      )}

      {bad && (
        <p
          className="border-b border-rose-100 bg-rose-50/60 px-4 py-2 text-xs text-rose-800"
          role="status"
        >
          {t("unsealedWhy", { count: unsealed })}
        </p>
      )}

      <dl className="grid grid-cols-1 gap-x-6 gap-y-3 px-4 py-3 sm:grid-cols-2 lg:grid-cols-4">
        <Field label={t("evaluationSet")}>
          {frame.evaluation_set_id ? (
            <Ident value={frame.evaluation_set_id} />
          ) : (
            <span className="text-slate-400">{t("unrecorded")}</span>
          )}
        </Field>
        <Field label={t("pivot")}>
          {frame.pivot_snapshot ? (
            <span className="flex flex-wrap items-baseline gap-2">
              <span className="font-mono text-sm">{frame.pivot_snapshot.version}</span>
              <Ident value={frame.pivot_snapshot.id} head={8} />
            </span>
          ) : (
            <span className="text-slate-400">{t("unrecorded")}</span>
          )}
        </Field>
        <Field label={t("accretion")}>
          {frame.information_accretion_set ? (
            <span className="flex flex-wrap items-baseline gap-2">
              <span className="text-sm">{frame.information_accretion_set.regime}</span>
              <Ident value={frame.information_accretion_set.sha256} />
            </span>
          ) : (
            <span className="text-slate-400">{t("unrecorded")}</span>
          )}
        </Field>
        <Field label={t("querySet")}>
          {frame.query_set ? (
            <span className="flex flex-wrap items-baseline gap-2">
              <span className="text-sm">{frame.query_set.name}</span>
              <span className="font-mono text-xs text-slate-500">
                {t("entries", { count: n(frame.query_set.entries) })}
              </span>
            </span>
          ) : (
            <span className="text-slate-400">{t("unrecorded")}</span>
          )}
        </Field>
      </dl>
    </section>
  );
}

// ── Nodes ────────────────────────────────────────────────────────────────

/** The pipeline in one glance: stage order, strength, nothing else. */
function Spine({ nodes }: { nodes: GraphNode[] }) {
  return (
    <div className="overflow-x-auto" data-testid="graph-spine">
      <ol className="flex min-w-max items-center gap-1 pb-1">
        {nodes.map((node, i) => (
          <li key={node.key} className="flex items-center gap-1">
            {i > 0 && <ChevronRight className="h-3.5 w-3.5 shrink-0 text-slate-300" aria-hidden />}
            <a
              href={`#node-${node.key}`}
              className="flex flex-col items-center gap-1 rounded-md px-2 py-1 hover:bg-slate-50"
            >
              <span className="text-[11px] font-medium text-slate-700">{node.title}</span>
              <StrengthChip strength={node.strength} size="xs" />
            </a>
          </li>
        ))}
      </ol>
    </div>
  );
}

function NodesTable({ nodes }: { nodes: GraphNode[] }) {
  const t = useTranslations("graph");
  const locale = useLocale();
  const n = (v: number) => v.toLocaleString(locale);

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
      <table className="w-full min-w-[62rem] text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" className="px-3 py-2.5 w-10">{t("colStage")}</th>
            <th scope="col" className="px-3 py-2.5">{t("colNode")}</th>
            <th scope="col" className="px-3 py-2.5">{t("colStrength")}</th>
            <th scope="col" className="px-3 py-2.5 text-right whitespace-nowrap">{t("colLevels")}</th>
            <th scope="col" className="px-3 py-2.5 text-right">{t("colResults")}</th>
            <th scope="col" className="px-3 py-2.5">{t("colFields")}</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map((node, i) => {
            const s = styleFor(node.strength);
            const href = NODE_SURFACE[node.key];
            const rule = i > 0 ? "border-t border-slate-100" : "";
            return (
              <Fragment key={node.key}>
              <tr id={`node-${node.key}`} className={`align-top ${rule}`}>
                <td className="py-3 pl-0 pr-3">
                  <div className="flex items-stretch gap-2">
                    {/* The rail repeats the strength as a shape at the row
                        edge, so a column of rows can be scanned without
                        reading any of them. */}
                    <span className={`w-1 shrink-0 self-stretch rounded-sm ${s.rail}`} aria-hidden />
                    <span className="pt-0.5 font-mono text-xs text-slate-400">{node.stage}</span>
                  </div>
                </td>
                <td className="px-3 py-3">
                  <div className="font-medium text-slate-900">
                    {href ? (
                      <SurfaceLink href={href}>{node.title}</SurfaceLink>
                    ) : (
                      node.title
                    )}
                  </div>
                  <p className="mt-0.5 max-w-md text-xs leading-relaxed text-slate-600">
                    {node.question}
                  </p>
                </td>
                <td className="px-3 py-3">
                  <StrengthChip strength={node.strength} />
                </td>
                <td className="px-3 py-3 text-right whitespace-nowrap font-mono text-sm text-slate-900">
                  {n(node.levels_instantiated)}
                  <span className="text-slate-400"> / {n(node.levels_available)}</span>
                  <div className="text-[10px] uppercase tracking-wider text-slate-400">
                    {t("instantiatedOfAvailable")}
                  </div>
                </td>
                <td className="px-3 py-3 text-right font-mono text-sm text-slate-900">
                  {n(node.results)}
                </td>
                <td className="px-3 py-3">
                  <div className="space-y-1.5">
                    {/* The value first, because it is what a reader came for.
                        A strength tells them how firmly the decision is held
                        and nothing at all about what was decided. */}
                    {node.held.length > 0 && (
                      <dl className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
                        {node.held.map((h) => (
                          <div key={h.field} className="flex items-baseline gap-1.5">
                            <dt className="text-[10px] uppercase tracking-wider text-slate-400">
                              {h.field}
                            </dt>
                            <dd className="font-mono text-xs font-medium text-slate-900">
                              {h.value}
                            </dd>
                          </div>
                        ))}
                      </dl>
                    )}
                    <div className="flex flex-wrap items-baseline gap-2">
                      <span className="text-[10px] uppercase tracking-wider text-slate-500">
                        {t("colVarying")}
                      </span>
                      <FieldList fields={node.varying_fields} empty={t("none")} />
                    </div>
                    <div className="flex flex-wrap items-baseline gap-2">
                      <span className="text-[10px] uppercase tracking-wider text-slate-400">
                        {t("colConstant")}
                      </span>
                      <FieldList fields={node.constant_fields} empty={t("none")} max={6} />
                    </div>
                  </div>
                </td>
              </tr>
              {/* The reason, on its own full-width line directly under the
                  node it belongs to. Never behind a click: an operator
                  scanning the pipeline for the one node that cannot answer
                  would otherwise read a gap as a zero. Full width rather
                  than squeezed into the strength column, because these run
                  to several sentences and a 20rem column turns them into a
                  tower that strands the counts beside it. */}
              {node.blocked_reason && (
                <tr id={`node-${node.key}-why`}>
                  <td className="py-0" />
                  <td colSpan={5} className="px-3 pb-3 pt-0">
                    <p className="max-w-4xl text-xs leading-relaxed text-slate-600">
                      {node.blocked_reason}
                    </p>
                  </td>
                </tr>
              )}
              </Fragment>
            );
          })}
          {nodes.length === 0 && (
            <tr>
              <td colSpan={6} className="px-4 py-8 text-center text-sm text-slate-600">
                {t("noNodes")}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}

function SurfaceLink({ href, children }: { href: string; children: React.ReactNode }) {
  const locale = useLocale();
  return (
    <Link href={`/${locale}${href}`} className="text-blue-700 hover:underline">
      {children}
    </Link>
  );
}

function StrengthLegend() {
  const t = useTranslations("graph");
  return (
    <dl className="grid grid-cols-1 gap-x-6 gap-y-2 rounded-lg border border-slate-200 bg-slate-50 px-4 py-3 sm:grid-cols-2 xl:grid-cols-3">
      {EDGE_STRENGTHS.map((s) => (
        <div key={s} className="flex items-start gap-2">
          <dt className="shrink-0 pt-0.5">
            <StrengthChip strength={s} size="xs" />
          </dt>
          <dd className="text-xs leading-relaxed text-slate-600">{t(`legend.${s}`)}</dd>
        </div>
      ))}
    </dl>
  );
}

// ── Panels ───────────────────────────────────────────────────────────────

type Metric = "f_micro_w" | "tau";

/**
 * Grid axes taken from the payload, ordered by the model's own order.
 *
 * The canonical nine come first; anything the endpoint sends that is not
 * one of them is appended rather than dropped. A panel the client does not
 * recognise is still a panel, and silently hiding it would make the grid
 * lie about what was scored.
 */
function axisOrder(values: string[], canonical: string[]): string[] {
  const seen = new Set(values);
  const head = canonical.filter((c) => seen.has(c));
  const tail = [...new Set(values)].filter((v) => !canonical.includes(v)).sort();
  return [...head, ...tail];
}

/**
 * A metric as text, or the empty mark when the record does not carry it.
 *
 * The endpoint declares every metric nullable, because a stored result can be
 * missing a threshold or a weighted score, and a surface that assumes otherwise
 * renders nothing at all rather than rendering a gap. An absent number is a
 * fact about the record and has to survive to the page.
 */
function fmt(v: number | null | undefined, metric: Metric): string {
  return v == null ? "∅" : v.toFixed(metric === "tau" ? 3 : 4);
}

/**
 * The nine populations, laid out as the nine.
 *
 * Population is shown whether or not a result exists, because an empty
 * panel with 5,811 units and an empty panel with none are different
 * situations and a blank cell would render them the same.
 */
function PanelGrid({ panels, metric }: { panels: GraphPanel[]; metric: Metric }) {
  const t = useTranslations("graph");
  const locale = useLocale();
  const byKey = indexPanels(panels);
  const categories = axisOrder(panels.map((p) => p.category), PANEL_CATEGORIES);
  const aspects = axisOrder(panels.map((p) => p.aspect), PANEL_ASPECTS);

  return (
    <div className="overflow-x-auto" data-testid="graph-panel-grid">
      <div
        className="grid min-w-[46rem] gap-2"
        style={{ gridTemplateColumns: `repeat(${aspects.length}, minmax(0, 1fr))` }}
      >
        {categories.map((cat) =>
          aspects.map((asp) => {
            const panel = byKey.get(panelKey(cat, asp));
            const summary = panel ? panelSummary(panel, metric) : null;
            return (
              <div
                key={panelKey(cat, asp)}
                className="rounded-lg border border-slate-200 bg-white p-3"
              >
                <div className="flex items-baseline justify-between gap-2">
                  <span className="font-mono text-xs font-semibold tracking-wide text-slate-900">
                    {cat} · {asp}
                  </span>
                  <span className="font-mono text-sm text-slate-900">
                    {panel?.units != null ? panel.units.toLocaleString(locale) : "∅"}
                    <span className="ml-1 text-[10px] uppercase tracking-wider text-slate-400">
                      {t("units")}
                    </span>
                  </span>
                </div>
                {/* What the panel can resolve, from its population alone.
                    Printed beside the spread on purpose: a spread smaller than
                    this is not a small result, it is no result, and a reader
                    who sees only the spread has no way to tell. */}
                {panel?.detectable_effect != null && (
                  <div className="mt-1.5 flex items-baseline justify-between gap-2 border-b border-dashed border-slate-200 pb-1.5 text-xs">
                    <span className="text-slate-500">{t("panelDetectable")}</span>
                    <span
                      className={`font-mono ${
                        summary != null && summary.spread < panel.detectable_effect
                          ? "font-semibold text-amber-700"
                          : "text-slate-900"
                      }`}
                    >
                      {panel.detectable_effect.toFixed(4)}
                    </span>
                  </div>
                )}
                {!panel ? (
                  <p className="mt-2 text-xs text-slate-400">{t("panelAbsent")}</p>
                ) : summary === null ? (
                  <p className="mt-2 text-xs text-slate-400">{t("panelNoResults")}</p>
                ) : (
                  <dl className="mt-2 space-y-1 text-xs">
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-slate-500">{t("panelBest")}</dt>
                      <dd className="min-w-0 truncate font-mono text-slate-900" title={summary.best.level}>
                        {summary.best.level}
                      </dd>
                    </div>
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-slate-500">{metric}</dt>
                      <dd className="font-mono font-semibold text-slate-900">
                        {fmt(summary.best[metric], metric)}
                      </dd>
                    </div>
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-slate-500" title={t("panelSpreadHint")}>
                        {t("panelSpread")}
                      </dt>
                      <dd className="font-mono text-slate-700">{fmt(summary.spread, metric)}</dd>
                    </div>
                    <div className="flex items-baseline justify-between gap-2">
                      <dt className="text-slate-500">{t("panelLevels")}</dt>
                      <dd className="font-mono text-slate-700">{panel.results.length}</dd>
                    </div>
                  </dl>
                )}
              </div>
            );
          }),
        )}
      </div>
    </div>
  );
}

/**
 * Every level against every panel, one number per cell.
 *
 * No row total and no column total. A row total would be an average over
 * the nine panels, which is the collapse the model forbids; a column total
 * would be an average over levels, which answers no question anybody has.
 * Sorting is per column, so choosing an order is choosing a panel to
 * believe rather than inventing a combined score.
 */
function PanelMatrix({ panels, metric }: { panels: GraphPanel[]; metric: Metric }) {
  const t = useTranslations("graph");
  const locale = useLocale();
  const [sortBy, setSortBy] = useState<string | null>(null);

  const byKey = indexPanels(panels);
  const categories = axisOrder(panels.map((p) => p.category), PANEL_CATEGORIES);
  const aspects = axisOrder(panels.map((p) => p.aspect), PANEL_ASPECTS);
  const columns: { cat: string; asp: string; key: string; panel: GraphPanel | undefined }[] = [];
  for (const cat of categories) {
    for (const asp of aspects) {
      const key = panelKey(cat, asp);
      columns.push({ cat, asp, key, panel: byKey.get(key) });
    }
  }
  const lookup = new Map(columns.map((c) => [c.key, indexPanelResults(c.panel)]));

  let levels = panelLevels(panels);
  if (sortBy) {
    const col = lookup.get(sortBy);
    levels = [...levels].sort((a, b) => {
      const va = col?.get(a)?.[metric];
      const vb = col?.get(b)?.[metric];
      // An absent metric sorts last whether it is missing from the record or
      // stored as null: a level nobody scored is not a level that scored zero.
      if (va == null && vb == null) return a.localeCompare(b);
      if (va == null) return 1;
      if (vb == null) return -1;
      return vb - va;
    });
  }

  if (levels.length === 0) {
    return (
      <p className="rounded-lg border border-slate-200 bg-white px-4 py-8 text-center text-sm text-slate-600">
        {t("matrixEmpty")}
      </p>
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white" data-testid="graph-matrix">
      <table className="w-full min-w-[54rem] text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" rowSpan={2} className="px-3 py-2 text-left align-bottom">
              {t("colLevel")}
            </th>
            {categories.map((cat) => (
              <th
                key={cat}
                scope="colgroup"
                colSpan={aspects.length}
                className="border-l border-slate-200 px-3 py-1.5 text-center"
              >
                {cat}
              </th>
            ))}
          </tr>
          <tr>
            {columns.map((c, i) => (
              <th
                key={c.key}
                scope="col"
                className={`px-2 py-1.5 text-right font-normal ${
                  i % aspects.length === 0 ? "border-l border-slate-200" : ""
                }`}
              >
                <button
                  type="button"
                  onClick={() => setSortBy(sortBy === c.key ? null : c.key)}
                  aria-pressed={sortBy === c.key}
                  title={t("sortByPanel", { panel: `${c.cat} ${c.asp}` })}
                  className={`rounded px-1 py-0.5 hover:bg-slate-200/70 ${
                    sortBy === c.key ? "bg-slate-200 text-slate-900" : ""
                  }`}
                >
                  {c.asp}
                </button>
                {/* The column's population sits in its head. A number is
                    not comparable to the one beside it without knowing how
                    many units each was measured over. */}
                <div className="mt-0.5 font-mono text-[10px] normal-case tracking-normal text-slate-400">
                  {c.panel?.units != null ? c.panel.units.toLocaleString(locale) : "∅"}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {levels.map((level) => (
            <tr key={level} className="hover:bg-slate-50/60">
              <th scope="row" className="px-3 py-2 text-left font-mono text-xs font-normal text-slate-800">
                {level}
              </th>
              {columns.map((c, i) => {
                const r = lookup.get(c.key)?.get(level);
                return (
                  <td
                    key={c.key}
                    className={`px-2 py-2 text-right font-mono text-xs ${
                      i % aspects.length === 0 ? "border-l border-slate-200" : ""
                    } ${r ? "text-slate-900" : "text-slate-300"}`}
                    title={
                      r
                        ? `tau ${r.tau != null ? r.tau.toFixed(3) : "∅"} · f_micro_w ${r.f_micro_w != null ? r.f_micro_w.toFixed(4) : "∅"}`
                        : t("panelNoResults")
                    }
                  >
                    {r ? fmt(r[metric], metric) : "∅"}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Blocked ──────────────────────────────────────────────────────────────

function BlockedList({ blocked }: { blocked: GraphResponse["blocked"] }) {
  const t = useTranslations("graph");
  if (blocked.length === 0) {
    return (
      <p className="rounded-lg border border-slate-200 bg-white px-4 py-6 text-center text-sm text-slate-600">
        {t("nothingBlocked")}
      </p>
    );
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white" data-testid="graph-blocked">
      <table className="w-full min-w-[52rem] text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" className="px-3 py-2.5">{t("colNode")}</th>
            <th scope="col" className="px-3 py-2.5">{t("colWhat")}</th>
            <th scope="col" className="px-3 py-2.5">{t("colWhy")}</th>
            <th scope="col" className="px-3 py-2.5">{t("colPrecondition")}</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {blocked.map((b, i) => (
            <tr key={`${b.node}-${i}`} className="align-top">
              <td className="px-3 py-3 whitespace-nowrap">
                <span className="flex items-center gap-2">
                  <Ban className="h-3.5 w-3.5 shrink-0 text-rose-600" aria-hidden />
                  <span className="font-medium text-slate-900">{b.node}</span>
                </span>
              </td>
              <td className="px-3 py-3 font-mono text-xs text-slate-800">{b.what}</td>
              <td className="px-3 py-3 text-xs leading-relaxed text-slate-600">{b.why}</td>
              <td className="px-3 py-3 text-xs leading-relaxed text-rose-900">{b.precondition}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── States ───────────────────────────────────────────────────────────────

function LoadingState() {
  const t = useTranslations("graph");
  return (
    <div className="space-y-4" data-testid="graph-loading" aria-busy="true">
      <p className="text-sm text-slate-500">{t("loading")}</p>
      <Skeleton className="h-24 w-full" />
      <Skeleton className="h-10 w-2/3" />
      <Skeleton className="h-64 w-full" />
    </div>
  );
}

/**
 * The failure, named.
 *
 * A 404 is called out separately because it is the one failure that is
 * about the deployment rather than about the data: an API build that does
 * not serve this route yet. Reporting it as a generic error would send a
 * reader looking for a missing frame that is not missing.
 */
function ErrorState({ error, onRetry }: { error: unknown; onRetry: () => void }) {
  const t = useTranslations("graph");
  const api = error instanceof ApiError ? error : null;
  const message = error instanceof Error ? error.message : String(error);
  const notServed = api?.status === 404;
  return (
    <div
      className="rounded-lg border border-rose-200 bg-rose-50 p-4"
      role="alert"
      data-testid="graph-error"
    >
      <h2 className="text-sm font-semibold text-rose-900">
        {notServed ? t("errorNotServedTitle") : t("errorTitle")}
      </h2>
      <p className="mt-1 text-sm text-rose-800">
        {notServed ? t("errorNotServedBody") : message}
      </p>
      {api && (
        <p className="mt-1 font-mono text-xs text-rose-700">
          {api.kind} · {api.status || t("noStatus")} · {api.path}
        </p>
      )}
      <button
        type="button"
        onClick={onRetry}
        className="mt-3 rounded-md border border-rose-300 bg-white px-3 py-1.5 text-xs font-medium text-rose-800 hover:bg-rose-100"
      >
        {t("retry")}
      </button>
    </div>
  );
}

function EmptyState() {
  const t = useTranslations("graph");
  return (
    <div
      className="rounded-lg border border-slate-200 bg-white p-6"
      data-testid="graph-empty"
    >
      <h2 className="text-base font-semibold text-slate-900">{t("emptyTitle")}</h2>
      <p className="mt-1 max-w-2xl text-sm leading-relaxed text-slate-600">{t("emptyBody")}</p>
    </div>
  );
}

// ── Page ─────────────────────────────────────────────────────────────────

export default function GraphPage() {
  const t = useTranslations("graph");
  const [graph, setGraph] = useState<GraphResponse | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [loading, setLoading] = useState(true);
  const [metric, setMetric] = useState<Metric>("f_micro_w");

  // Split in two on purpose. `fetch` touches no state until its promise
  // settles, which is what lets the mount effect call it without the
  // synchronous setState that would cascade a render; `reload` is the
  // button's entry point and may mark the page busy immediately, because
  // it runs from an event and not from an effect. The initial busy state
  // is the useState default above, so the mount path needs no setter.
  const fetchGraph = useCallback(() => {
    getGraph()
      .then((g) => {
        setGraph(g);
        setError(null);
      })
      .catch((e: unknown) => {
        setGraph(null);
        setError(e);
      })
      .finally(() => setLoading(false));
  }, []);

  const reload = useCallback(() => {
    setLoading(true);
    setError(null);
    fetchGraph();
  }, [fetchGraph]);

  useEffect(() => {
    fetchGraph();
  }, [fetchGraph]);

  const nodes = graph ? [...graph.nodes].sort((a, b) => a.stage - b.stage) : [];
  const empty = graph !== null && isEmptyGraph(graph);

  return (
    <div className="mx-auto max-w-6xl space-y-6 px-4 py-8 sm:px-6">
      <header className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-slate-900">{t("title")}</h1>
          <p className="mt-1 max-w-3xl text-sm leading-relaxed text-slate-600">{t("subtitle")}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {/* One control, and it changes which number is shown, never how
              many numbers are shown. Both metrics come from the payload. */}
          <div className="inline-flex overflow-hidden rounded-md border border-slate-300">
            {(["f_micro_w", "tau"] as Metric[]).map((m) => (
              <button
                key={m}
                type="button"
                onClick={() => setMetric(m)}
                aria-pressed={metric === m}
                className={`px-2.5 py-1.5 font-mono text-xs ${
                  metric === m
                    ? "bg-slate-800 text-white"
                    : "bg-white text-slate-700 hover:bg-slate-50"
                }`}
              >
                {m}
              </button>
            ))}
          </div>
          <button
            type="button"
            onClick={reload}
            disabled={loading}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} aria-hidden />
            {loading ? t("reloading") : t("reload")}
          </button>
        </div>
      </header>

      {loading && !graph && <LoadingState />}
      {error !== null && <ErrorState error={error} onRetry={reload} />}

      {graph && empty && <EmptyState />}

      {graph && !empty && (
        <>
          {/* 1. The frame. Everything below is unreadable without it. */}
          <FrameCard frame={graph.frame} />

          {/* 2. The nodes. */}
          <section className="space-y-3" data-testid="graph-nodes">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h2 className="text-lg font-semibold tracking-tight text-slate-900">
                {t("nodesHeading")}
              </h2>
              <p className="text-xs text-slate-500">{t("nodesHint")}</p>
            </div>
            <Spine nodes={nodes} />
            <StrengthLegend />
            <NodesTable nodes={nodes} />
          </section>

          {/* 3. The nine panels, never nine collapsed into one. */}
          <section className="space-y-3" data-testid="graph-panels">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h2 className="text-lg font-semibold tracking-tight text-slate-900">
                {t("panelsHeading")}
              </h2>
              <p className="max-w-xl text-xs text-slate-500">{t("panelsHint")}</p>
            </div>
            <PanelGrid panels={graph.panels} metric={metric} />
            <PanelMatrix panels={graph.panels} metric={metric} />
          </section>

          {/* 4. What is blocked, with the precondition on the line. */}
          <section className="space-y-3" data-testid="graph-blocked-section">
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h2 className="text-lg font-semibold tracking-tight text-slate-900">
                {t("blockedHeading")}
              </h2>
              <p className="text-xs text-slate-500">{t("blockedHint")}</p>
            </div>
            <BlockedList blocked={graph.blocked} />
          </section>
        </>
      )}
    </div>
  );
}
