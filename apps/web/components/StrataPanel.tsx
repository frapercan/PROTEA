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

import { useEffect, useState } from "react";

import { baseUrl } from "@/lib/api";
import StrataChart from "@/components/StrataChart";
import { CATEGORY_NAME, coverage } from "@/lib/strataView";
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

type Props = { evaluationResultId: string };

const TH = "px-2 py-1 text-left text-[11px] font-semibold text-slate-600";
const TD = "px-2 py-1 text-[12px] tabular-nums";

function SettingTable({
  setting,
  cells,
  axes,
}: {
  setting: string;
  cells: StratumCell[];
  axes: string[];
}) {
  const ordered = inReportOrder(cells, axes);
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
            {ordered.map((cell) => (
              <tr
                key={cellKey(cell, axes)}
                className={cell.reportable ? "" : "opacity-60"}
              >
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
                <td className={TD}>
                  {cell.reportable ? null : (
                    <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-600">
                      withheld
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </div>
  );
}

export default function StrataPanel({ evaluationResultId }: Props) {
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
        />
      ))}
    </section>
  );
}
