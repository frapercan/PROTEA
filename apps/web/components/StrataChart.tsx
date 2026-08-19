// The readable half of the strata panel: a grid the eye can scan and a
// profile the eye can follow.
//
// The numbers were already correct in the plain table; the problem was that 47
// rows of four-decimal figures is not something a reader can take in, so a
// result that was technically on screen was practically unreadable.
//
// Three rules carried over from the table, because they are what stops a chart
// from flattering the data:
//   - population is shown on every cell, since 0.61 over 85 proteins and 0.61
//     over 3,886 are not the same claim;
//   - cells below the population floor are drawn hatched and labelled, never
//     dropped, because a chart of only the surviving cells looks identical to
//     one that covered every stratum;
//   - the colour scale spans the whole grid, not each row, so a weak row
//     cannot be re-normalised into looking as strong as a strong one.

"use client";

import {
  ASPECT_NAME,
  ASPECT_ORDER,
  HOMOLOGY_MEANING,
  HOMOLOGY_ORDER,
  LENGTH_ORDER,
  heatColour,
  linePoints,
  profileByAspect,
  scoreRange,
  sortBands,
  textOn,
  toGrid,
  type ProfilePoint,
} from "@/lib/strataView";
import type { StratumCell } from "@/lib/strata";

const AXIS = "text-[10px] fill-slate-500";
const LINE_COLOUR: Record<string, string> = {
  F: "#0369a1",
  P: "#b45309",
  C: "#15803d",
};

function Profile({ cells }: { cells: StratumCell[] }) {
  const byAspect = profileByAspect(cells);
  const aspects = ASPECT_ORDER.filter((a) => byAspect[a]?.length);
  if (aspects.length === 0) return null;

  const all: ProfilePoint[] = aspects.flatMap((a) => byAspect[a]);
  const lo = Math.min(...all.map((p) => p.value));
  const hi = Math.max(...all.map((p) => p.value));
  const bands = sortBands([...new Set(all.map((p) => p.band))], HOMOLOGY_ORDER);
  const W = 300;
  const H = 90;

  return (
    <figure className="m-0">
      <figcaption className="mb-1 text-[11px] text-slate-600">
        Score against neighbourhood homology, one line per aspect. Each point is
        the population-weighted mean of that band&apos;s cells.
      </figcaption>
      <svg
        viewBox={`0 0 ${W + 46} ${H + 34}`}
        className="w-full max-w-lg"
        role="img"
      >
        <g transform="translate(34, 8)">
          <line x1="0" y1={H} x2={W} y2={H} stroke="#cbd5e1" strokeWidth="1" />
          <line x1="0" y1="0" x2="0" y2={H} stroke="#cbd5e1" strokeWidth="1" />
          <text x="-6" y="8" textAnchor="end" className={AXIS}>
            {hi.toFixed(2)}
          </text>
          <text x="-6" y={H} textAnchor="end" className={AXIS}>
            {lo.toFixed(2)}
          </text>
          {aspects.map((a) => (
            <polyline
              key={a}
              points={linePoints(byAspect[a], W, H, lo, hi)}
              fill="none"
              stroke={LINE_COLOUR[a]}
              strokeWidth="2"
            />
          ))}
          {aspects.map((a) =>
            byAspect[a].map((p, i) => {
              const step =
                byAspect[a].length > 1 ? W / (byAspect[a].length - 1) : 0;
              const span = hi > lo ? hi - lo : 1;
              return (
                <circle
                  key={`${a}-${p.band}`}
                  cx={i * step}
                  cy={H - ((p.value - lo) / span) * H}
                  r="2.5"
                  fill={LINE_COLOUR[a]}
                >
                  <title>
                    {`${ASPECT_NAME[a]} / ${p.band}: ${p.value.toFixed(4)} over ${p.n.toLocaleString()} proteins`}
                  </title>
                </circle>
              );
            }),
          )}
          {bands.map((b, i) => (
            <text
              key={b}
              x={bands.length > 1 ? (i * W) / (bands.length - 1) : 0}
              y={H + 14}
              textAnchor="middle"
              className={AXIS}
            >
              {b}
            </text>
          ))}
        </g>
      </svg>
      <div className="mt-1 flex flex-wrap gap-3">
        {aspects.map((a) => (
          <span
            key={a}
            className="inline-flex items-center gap-1 text-[10px] text-slate-600"
          >
            <span
              className="inline-block h-2 w-3 rounded-sm"
              style={{ background: LINE_COLOUR[a] }}
            />
            {ASPECT_NAME[a]}
          </span>
        ))}
      </div>
    </figure>
  );
}

function AspectGrid({
  cells,
  aspect,
  lo,
  hi,
}: {
  cells: StratumCell[];
  aspect: string;
  lo: number;
  hi: number;
}) {
  const grid = toGrid(
    cells.filter((c) => String(c.aspect) === aspect),
    "length",
    "homology",
    LENGTH_ORDER,
    HOMOLOGY_ORDER,
  );
  if (grid.rows.length === 0) return null;
  return (
    <div className="mb-3">
      <h5 className="mb-1 text-[11px] font-semibold text-slate-700">
        {ASPECT_NAME[aspect]}
      </h5>
      <table className="border-collapse">
        <thead>
          <tr>
            <th className="px-1 py-0.5 text-left text-[10px] font-medium text-slate-500">
              length
            </th>
            {grid.cols.map((col) => (
              <th
                key={col}
                className="px-1 py-0.5 text-[10px] font-medium text-slate-500"
                title={HOMOLOGY_MEANING[col] ?? col}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {grid.rows.map((row) => (
            <tr key={row}>
              <td className="px-1 py-0.5 text-[10px] text-slate-600">{row}</td>
              {grid.cols.map((col) => {
                const cell = grid.at(row, col);
                if (!cell) {
                  return (
                    <td key={col} className="px-1 py-0.5">
                      <div
                        className="flex h-9 w-16 items-center justify-center rounded border border-dashed border-slate-200 text-[10px] text-slate-300"
                        title="no protein in this stratum"
                      >
                        &ndash;
                      </div>
                    </td>
                  );
                }
                const thin = !cell.reportable;
                return (
                  <td key={col} className="px-1 py-0.5">
                    <div
                      className={`flex h-9 w-16 flex-col items-center justify-center rounded ${
                        thin ? "border border-dashed border-slate-300" : ""
                      }`}
                      style={
                        thin
                          ? undefined
                          : {
                              background: heatColour(cell.f_micro_w, lo, hi),
                              color: textOn(cell.f_micro_w, lo, hi),
                            }
                      }
                      title={
                        thin
                          ? `withheld: ${cell.n_proteins} proteins, below the population floor`
                          : `${cell.f_micro_w.toFixed(4)} over ${cell.n_proteins.toLocaleString()} proteins`
                      }
                    >
                      <span
                        className={`text-[11px] tabular-nums ${thin ? "text-slate-400" : ""}`}
                      >
                        {cell.f_micro_w.toFixed(3)}
                      </span>
                      <span
                        className={`text-[9px] tabular-nums ${thin ? "text-slate-400" : "opacity-80"}`}
                      >
                        n={cell.n_proteins.toLocaleString()}
                      </span>
                    </div>
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

export default function StrataChart({ cells }: { cells: StratumCell[] }) {
  const [lo, hi] = scoreRange(cells);
  const aspects = ASPECT_ORDER.filter((a) =>
    cells.some((c) => String(c.aspect) === a),
  );
  return (
    <div>
      <Profile cells={cells} />
      <div className="mt-3 flex flex-wrap gap-6">
        {aspects.map((a) => (
          <AspectGrid key={a} cells={cells} aspect={a} lo={lo} hi={hi} />
        ))}
      </div>
      <p className="mt-1 text-[10px] text-slate-500">
        Colour spans {lo.toFixed(3)} to {hi.toFixed(3)} across every cell shown,
        so cells are comparable between grids. Dashed cells are withheld: their
        population is below the floor, and they are drawn rather than dropped so
        the coverage of the table is visible.
      </p>
    </div>
  );
}
