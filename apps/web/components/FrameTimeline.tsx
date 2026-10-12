"use client";

import { useMemo } from "react";
import { useTranslations } from "next-intl";
import type { GraphTimeline, TimelineMark } from "@/lib/graph";

/**
 * The frame on a date axis.
 *
 * `GoaReleaseTimeline` puts one dot per annotation release on a line, which
 * answers "when was each release published". This answers a different question
 * and needs a different shape: a window is an INTERVAL, and the thing worth
 * seeing is how the releases sit relative to it.
 *
 * Three facts fall out of the geometry that no table on this page states:
 *
 * The pivot sits INSIDE the window it reconciles, not at either end. The
 * ontology every gained term is expressed in was published six weeks before the
 * window closed, so neither end of the window is scored under a graph
 * contemporary with it.
 *
 * An ontology contemporary with the window's opening EXISTS and is not the one
 * in use. It sits just left of the start, and the distance between it and the
 * pivot is the room the reconciliation has to move a term between panels.
 *
 * And a release lies BEYOND the closing end. That is the cohort nobody is
 * allowed to look at yet, and its distance from the window is how much
 * annotation has accumulated since anything here was measured.
 *
 * Hand-rolled SVG, like its neighbour: an axis with a band and a dozen ticks
 * does not justify a chart dependency, and a dependency would not know which of
 * these marks is load bearing.
 */

type FrameTimelineProps = {
  timeline: GraphTimeline;
};

const W = 900;
const H = 132;
const PAD_L = 16;
const PAD_R = 16;
const AXIS_Y = 92;
const BAND_TOP = 44;
const BAND_H = 30;

/** Milliseconds of a YYYY-MM-DD, parsed as UTC so no timezone shifts a day. */
function ms(iso: string): number {
  const [y, m, d] = iso.split("-").map(Number);
  return Date.UTC(y, m - 1, d);
}

function yearTicks(from: number, to: number): { at: number; label: string }[] {
  const out: { at: number; label: string }[] = [];
  const first = new Date(from).getUTCFullYear();
  const last = new Date(to).getUTCFullYear();
  for (let y = first; y <= last + 1; y += 1) {
    const at = Date.UTC(y, 0, 1);
    if (at >= from && at <= to) out.push({ at, label: String(y) });
  }
  return out;
}

/**
 * Nudge overlapping labels apart along the axis.
 *
 * Releases cluster: two ontology snapshots a week apart would print their dates
 * on top of each other and neither would be readable. Positions are laid out
 * left to right and each one is pushed right of its predecessor when they would
 * collide, so the label moves and the mark itself never does.
 */
/**
 * De-collide label positions, keeping the whole run between `lo` and `hi`.
 *
 * The one-directional push this replaces only ever moved labels RIGHT, so a run
 * clustered near the right edge walked off the drawing: the window marks sit at
 * the end of a nine-year series, and five labels were enough to put the last one
 * at x=956 in a 900-wide viewBox. Anything past the edge is clipped in silence,
 * which is the failure mode worth preventing -- a chart that drops a label does
 * not look broken, it looks like the label was never there.
 *
 * So: push right to open the gaps, shift the whole run back if it overran `hi`,
 * then push right off `lo` in case that shift pulled the head out the other
 * side. With the handful of marks this labels there is always room; if there
 * ever is not, the overflow lands on the right, where `overflow-x-auto` on the
 * figure can still reach it.
 */
function spread(xs: number[], minGap: number, lo?: number, hi?: number): number[] {
  const out = [...xs];
  for (let i = 1; i < out.length; i += 1) {
    if (out[i] - out[i - 1] < minGap) out[i] = out[i - 1] + minGap;
  }
  if (out.length === 0) return out;
  if (hi !== undefined && out[out.length - 1] > hi) {
    const shift = out[out.length - 1] - hi;
    for (let i = 0; i < out.length; i += 1) out[i] -= shift;
  }
  if (lo !== undefined && out[0] < lo) {
    out[0] = lo;
    for (let i = 1; i < out.length; i += 1) {
      if (out[i] - out[i - 1] < minGap) out[i] = out[i - 1] + minGap;
    }
  }
  return out;
}

/**
 * Which annotation marks get a printed label.
 *
 * Every one of them used to. That worked while the frame carried a dozen marks,
 * which is what the note above describes, and broke once the series was loaded:
 * the timeline now ships 141 marks, 75 of them annotation releases, and
 * `spread` pushes each label right until it clears the previous by 70px. 75 x 70
 * is 5.250px of labels inside an svg whose viewBox is 900 wide and which scales
 * to its container, so about 62 of them were drawn past the right edge and
 * clipped away in silence. The dozen that survived sat at uniform 70px
 * intervals, which reads as a tidy list with no relation to the dates underneath
 * -- the one thing this chart exists to show.
 *
 * So the labels go to the marks the note calls load bearing: the two ends of the
 * window, whatever sits inside it, and the pivot. The release just before the
 * window opens and the first one past its close are added in the body, because
 * those two are what say how much annotation the window leaves on each side.
 * Every other release keeps its dot, its colour and its <title>, so the shape of
 * the series is still there and hovering still names any of them.
 */
const LABELLED_ROLES = new Set(["window_start", "window_end", "inside", "pivot"]);

const ROLE_FILL: Record<string, string> = {
  window_start: "#0f766e",
  window_end: "#0f766e",
  pivot: "#b45309",
  inside: "#94a3b8",
  before: "#cbd5e1",
  beyond: "#cbd5e1",
};

export function FrameTimeline({ timeline }: FrameTimelineProps) {
  const t = useTranslations("graph");
  const marks = timeline.marks;

  const geometry = useMemo(() => {
    if (marks.length === 0) return null;
    const dates = marks.map((m) => ms(m.date));
    const lo = Math.min(...dates);
    const hi = Math.max(...dates);
    // A month of padding each side, so the outermost mark is not clipped and
    // the axis does not imply the record ends exactly where the data does.
    const month = 30 * 24 * 3600 * 1000;
    const from = lo - month;
    const to = hi + month;
    const span = Math.max(1, to - from);
    const axisW = W - PAD_L - PAD_R;
    const xOf = (at: number) => PAD_L + ((at - from) / span) * axisW;
    return { from, to, xOf, years: yearTicks(from, to) };
  }, [marks]);

  if (!geometry) return null;
  const { xOf, years } = geometry;

  const windowFrom = timeline.window.from ? xOf(ms(timeline.window.from)) : null;
  const windowTo = timeline.window.to ? xOf(ms(timeline.window.to)) : null;

  const annotations = marks.filter((m) => m.kind === "annotation_set");
  const ontologies = marks.filter((m) => m.kind === "ontology_snapshot");
  // Sorted here rather than trusted from the payload: the neighbour picks below
  // are "the last one before" and "the first one past", which are wrong if the
  // order is not by date.
  const byDate = [...annotations].sort((a, b) => ms(a.date) - ms(b.date));
  const labelled = new Set(byDate.filter((m) => LABELLED_ROLES.has(m.role)));
  const lastBefore = [...byDate].reverse().find((m) => m.role === "before");
  const firstBeyond = byDate.find((m) => m.role === "beyond");
  if (lastBefore) labelled.add(lastBefore);
  if (firstBeyond) labelled.add(firstBeyond);

  const labelledInOrder = byDate.filter((m) => labelled.has(m));
  const spreadXs = spread(
    labelledInOrder.map((m) => xOf(ms(m.date))),
    70,
    PAD_L + 35,
    W - PAD_R - 35,
  );
  const labelXs = new Map(labelledInOrder.map((m, i) => [m, spreadXs[i]]));

  return (
    <figure tabIndex={0} className="overflow-x-auto" data-testid="graph-timeline">
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="min-w-[720px] w-full"
        role="img"
        aria-label={t("timelineAria")}
      >
        {/* The window, as a band. It is an interval and drawing it as two
            points would lose the one thing worth seeing: what falls inside. */}
        {windowFrom !== null && windowTo !== null && (
          <>
            <rect
              x={windowFrom}
              y={BAND_TOP}
              width={Math.max(2, windowTo - windowFrom)}
              height={BAND_H}
              className="fill-teal-50 stroke-teal-600"
              strokeWidth={1}
            />
            <text
              x={(windowFrom + windowTo) / 2}
              y={BAND_TOP + 19}
              textAnchor="middle"
              className="fill-teal-800 text-[11px] font-medium"
            >
              {t("timelineWindow")}
            </text>
          </>
        )}

        {/* Year gridlines behind everything, unlabelled ticks above the axis. */}
        {years.map((y) => (
          <g key={y.label}>
            <line
              x1={xOf(y.at)}
              y1={BAND_TOP - 10}
              x2={xOf(y.at)}
              y2={AXIS_Y}
              className="stroke-slate-200"
              strokeWidth={1}
            />
            <text
              x={xOf(y.at) + 3}
              y={BAND_TOP - 14}
              className="fill-slate-500 text-[10px]"
            >
              {y.label}
            </text>
          </g>
        ))}

        <line x1={PAD_L} y1={AXIS_Y} x2={W - PAD_R} y2={AXIS_Y} className="stroke-slate-300" />

        {/* Ontology releases as ticks above the axis: they are the graph a
            number is read under, and there are many, so they read as a comb
            rather than as a row of things each deserving a label. */}
        {ontologies.map((m) => {
          const x = xOf(ms(m.date));
          const isPivot = m.is_pivot;
          return (
            <g key={`o-${m.label}`}>
              <line
                x1={x}
                y1={isPivot ? BAND_TOP - 6 : AXIS_Y - 12}
                x2={x}
                y2={AXIS_Y}
                stroke={isPivot ? ROLE_FILL.pivot : "#cbd5e1"}
                strokeWidth={isPivot ? 2 : 1}
              />
              {isPivot && (
                <text
                  x={x}
                  y={BAND_TOP - 12}
                  textAnchor="middle"
                  className="fill-amber-700 text-[10px] font-semibold"
                >
                  {t("timelinePivot")}
                </text>
              )}
              <title>{`${m.label} · ${m.date}`}</title>
            </g>
          );
        })}

        {/* Annotation releases as dots below the axis, each labelled. These are
            the ends a window is made of, so every one of them is named. */}
        {annotations.map((m) => {
          const x = xOf(ms(m.date));
          const lx = labelXs.get(m);
          const end = m.role === "window_start" || m.role === "window_end";
          return (
            <g key={`a-${m.label}`}>
              {lx !== undefined && Math.abs(lx - x) > 1 && (
                <line
                  x1={x}
                  y1={AXIS_Y + 4}
                  x2={lx}
                  y2={AXIS_Y + 12}
                  className="stroke-slate-300"
                  strokeWidth={1}
                />
              )}
              <circle
                cx={x}
                cy={AXIS_Y}
                r={end ? 5 : 3.5}
                fill={ROLE_FILL[m.role] ?? ROLE_FILL.inside}
                stroke="#fff"
                strokeWidth={1.5}
              />
              {lx !== undefined && (
                <>
                  <text
                    x={lx}
                    y={AXIS_Y + 24}
                    textAnchor="middle"
                    className={`text-[11px] ${end ? "fill-teal-800 font-semibold" : "fill-slate-500"}`}
                  >
                    {m.label}
                  </text>
                  <text
                    x={lx}
                    y={AXIS_Y + 35}
                    textAnchor="middle"
                    className="fill-slate-500 text-[9px]"
                    fontFamily="ui-monospace, monospace"
                  >
                    {m.date}
                  </text>
                </>
              )}
              <title>{`${m.label} · ${m.date} · ${m.role}`}</title>
            </g>
          );
        })}
      </svg>
    </figure>
  );
}
