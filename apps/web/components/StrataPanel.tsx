// Per-stratum cells for one evaluation result.
//
// The headline number of an evaluation is an average over populations that
// differ by more than an order of magnitude. This shows the cells underneath
// it: what the model scored on short sequences whose nearest donor sits in the
// twilight zone, and how many proteins that claim rests on.
//
// Two rules it exists to enforce, both visible rather than documented:
//   - every cell shows its population, because 0.61 over 85 proteins and 0.61
//     over 3,886 are not the same claim;
//   - cells below the population floor are shown and marked, never dropped. A
//     panel that rendered only the reportable ones would look identical to one
//     that covered every stratum.
//
// Pure presentation over lib/strata.ts, fetched on demand: an evaluation that
// was never stratified answers 404 and the panel says so instead of blocking
// the page it sits on.

"use client";

import { Fragment, useEffect, useState } from "react";
import Link from "next/link";

import { baseUrl } from "@/lib/api";
import StrataChart from "@/components/StrataChart";
import { StratumMembers } from "@/components/StratumMembers";
import { CATEGORY_NAME, coverage } from "@/lib/strataView";
import { stratumHref } from "@/lib/stratumProteins";
import {
  axisLabel,
  cellKey,
  formatScore,
  inReportOrder,
  primaryAxis,
  scoreShade,
  withheldShare,
  type StrataResponse,
  type StratumCell,
} from "@/lib/strata";

type Props = {
  evaluationResultId: string;
  /** The set whose neighbourhood a cell is opened against. Optional because
   *  not every caller has it; without it the panel reads as before and the
   *  open control is not offered. */
  predictionSetId?: string;
  /** The evaluation set this result belongs to. Optional for the same reason,
   *  and it buys the other descent: the full cell view needs it to offer the
   *  other arms scored at the same coordinates. */
  evaluationSetId?: string;
  locale: string;
};

const TH = "px-2 py-1 text-left text-[11px] font-semibold text-slate-600";
const TD = "px-2 py-1 text-[12px] tabular-nums";

function SettingTable({
  setting,
  cells,
  axes,
  evaluationResultId,
  evaluationSetId,
  predictionSetId,
  locale,
}: {
  setting: string;
  cells: StratumCell[];
  axes: string[];
  /** The arm these cells belong to, which is the arm the cell view opens on. */
  evaluationResultId: string;
  evaluationSetId?: string;
  /** Absent on an evaluation whose row does not carry it; the open control
   *  is then not offered rather than offered and broken. */
  predictionSetId?: string;
  locale: string;
}) {
  const ordered = inReportOrder(cells, axes);
  // One cell at a time. Opening recomputes the neighbourhood for every query
  // in the prediction set, so a panel that let a reader open ten rows would
  // fire ten of those.
  const [open, setOpen] = useState<string | null>(null);
  const shaded = ordered.filter((c) => c.reportable);
  const cov = coverage(cells);
  return (
    <div className="mb-5">
      <div className="mb-1 flex items-baseline gap-2">
        <h4 className="text-[12px] font-semibold text-slate-800">
          {CATEGORY_NAME[setting] ?? setting}
        </h4>
        <span className="text-[11px] text-slate-500">
          {cov.total.toLocaleString()} proteins in {cov.cells} strata
          {cov.withheld > 0
            ? `, ${((cov.withheld / cov.total) * 100).toFixed(1)}% withheld across ${cov.withheldCells} thin cells`
            : ", every cell above the floor"}
        </span>
      </div>
      <StrataChart cells={cells} />
      <details className="mt-2">
        <summary className="cursor-pointer text-[11px] text-slate-600">
          every cell as a table
        </summary>
        <table className="w-full border-collapse">
          <thead>
            <tr className="border-b border-slate-200">
              {axes.map((a) => (
                <th key={a} className={TH}>
                  {a}
                </th>
              ))}
              <th className={`${TH} text-right`}>proteins</th>
              <th className={`${TH} text-right`}>f_micro_w</th>
              <th className={TH} />
            </tr>
          </thead>
          <tbody>
            {ordered.map((cell) => {
              const key = cellKey(cell, axes);
              const isOpen = open === key;
              return (
              <Fragment key={key}>
              <tr className={cell.reportable ? "" : "opacity-60"}>
                {axes.map((axis) => (
                  <td key={axis} className={`${TD} text-slate-700`}>
                    {axisLabel(axis, String(cell[axis] ?? ""))}
                  </td>
                ))}
                <td className={`${TD} text-right text-slate-500`}>
                  {cell.n_proteins.toLocaleString()}
                </td>
                <td
                  className={`${TD} text-right ${
                    cell.reportable ? scoreShade(cell.f_micro_w, shaded) : ""
                  }`}
                >
                  {formatScore(cell.f_micro_w)}
                </td>
                <td className={`${TD} whitespace-nowrap`}>
                  {cell.reportable ? null : (
                    <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600">
                      withheld
                    </span>
                  )}
                  {/* Two descents, named for the two different questions they
                      answer. "band" expands the queries the length and
                      homology bands hold, which is a fact about the retrieval
                      and counts more proteins than this cell does, because
                      category and aspect are asserted there rather than
                      filtered. "proteins" opens the cell itself, whose
                      population is the one printed on this row. Naming both
                      "open" invited reading the larger count as this cell's. */}
                  {predictionSetId ? (
                    <button
                      type="button"
                      onClick={() => setOpen(isOpen ? null : key)}
                      aria-expanded={isOpen}
                      title="The queries in this length and homology band, which is a larger population than this cell"
                      className="ml-1 rounded px-1.5 py-0.5 text-[10px] text-sky-700 hover:bg-sky-50 hover:underline"
                    >
                      {isOpen ? "close band" : "band"}
                    </button>
                  ) : null}
                  {evaluationSetId ? (
                    <Link
                      href={stratumHref({
                        evaluationSetId,
                        category: setting,
                        aspect: String(cell.aspect ?? ""),
                        length: cell.length ? String(cell.length) : null,
                        homology: cell.homology ? String(cell.homology) : null,
                        arm: evaluationResultId,
                      })}
                      title="This cell's own proteins, each with what it scored"
                      className="ml-1 rounded px-1.5 py-0.5 text-[10px] text-sky-700 hover:bg-sky-50 hover:underline"
                    >
                      proteins
                    </Link>
                  ) : null}
                </td>
              </tr>
              {isOpen && predictionSetId ? (
                <tr>
                  <td colSpan={axes.length + 3} className="px-1 pb-2">
                    <StratumMembers
                      // Keyed on the cell: opening another one remounts
                      // rather than reusing a panel still holding the
                      // previous cell's proteins.
                      key={key}
                      predictionSetId={predictionSetId}
                      category={setting}
                      aspect={String(cell.aspect ?? "")}
                      length={cell.length ? String(cell.length) : null}
                      homology={cell.homology ? String(cell.homology) : null}
                      locale={locale}
                    />
                  </td>
                </tr>
              ) : null}
              </Fragment>
              );
            })}
          </tbody>
        </table>
      </details>
    </div>
  );
}

export default function StrataPanel({
  evaluationResultId,
  predictionSetId,
  evaluationSetId,
  locale,
}: Props) {
  const [data, setData] = useState<StrataResponse | null>(null);
  const [state, setState] = useState<"loading" | "ready" | "absent" | "error">(
    "loading",
  );

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await fetch(`${baseUrl()}/strata/${evaluationResultId}`, {
          cache: "no-store",
        });
        if (cancelled) return;
        if (res.status === 404) {
          setState("absent");
          return;
        }
        if (!res.ok) {
          setState("error");
          return;
        }
        setData((await res.json()) as StrataResponse);
        setState("ready");
      } catch {
        if (!cancelled) setState("error");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [evaluationResultId]);

  if (state === "loading") {
    return <p className="text-[12px] text-slate-500">Loading strata…</p>;
  }
  if (state === "absent") {
    // Named rather than blank: never stratified and empty would otherwise look
    // the same, and only one of them is worth acting on.
    return (
      <p className="text-[12px] text-slate-500">
        Not stratified yet. Run the <code>stratify_evaluation</code> operation
        for this result.
      </p>
    );
  }
  if (state === "error" || !data) {
    return (
      <p className="text-[12px] text-rose-700">
        Could not load the strata for this result.
      </p>
    );
  }

  const axis = primaryAxis(data.axes);
  return (
    <section>
      <p className="mb-2 text-[11px] text-slate-500">
        One cell per stratum, crossed on {data.axes.join(" x ")}
        {axis ? `, read down ${axis}` : ""}. Cells below the population floor
        are shown and marked rather than dropped.
      </p>
      {Object.entries(data.settings).map(([setting, cells]) => (
        <SettingTable
          key={setting}
          setting={setting}
          cells={cells}
          axes={data.axes}
          evaluationResultId={evaluationResultId}
          evaluationSetId={evaluationSetId}
          predictionSetId={predictionSetId}
          locale={locale}
        />
      ))}
    </section>
  );
}
