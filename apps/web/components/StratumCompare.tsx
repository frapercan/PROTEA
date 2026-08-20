// Who wins inside one stratum, and how much the choice is worth there.
//
// The board ranks arms over the whole population. That number is dominated
// by the band where a near-identical donor already exists, and in that band
// an 8M-parameter model sits 0.4 points off a 650M one. A reader who only
// sees the global table concludes the representation barely matters. In the
// band with no close donor, the same model is 8.5 points behind and last.
//
// So the identity band leads, and the spread travels beside every band
// rather than only for the one selected: the comparison between bands IS
// the finding, and a panel that shows one band at a time hides it.

"use client";

import { useEffect, useState } from "react";

import { axisLabel } from "@/lib/strata";
import {
  atStratum,
  bandsPresent,
  rankArms,
  spreadOf,
  spreadSentence,
  type StratumCompare as Payload,
  type StratumRow,
} from "@/lib/stratumCompare";

type Props = {
  evaluationSetId: string;
  /** Knowledge category. The three are different populations. */
  setting?: string;
  k?: number | null;
};

/**
 * The stratum with the most to say, when the reader has not chosen.
 *
 * The largest reportable population, because a band comparison over 32
 * proteins is a comparison of noise and the panel would open on it as
 * often as not. Ties break toward the first, which is stable given the
 * rows arrive ordered.
 */
function busiestCell(rows: StratumRow[]): { aspect: string; length: string } | null {
  const pop = new Map<string, number>();
  for (const r of rows) {
    if (r.reportable === false) continue;
    const key = `${r.aspect ?? ""}|${r.length ?? ""}`;
    pop.set(key, (pop.get(key) ?? 0) + r.n_proteins);
  }
  let bestKey: string | null = null;
  let bestPop = -1;
  for (const [key, n] of pop) {
    if (n > bestPop) {
      bestPop = n;
      bestKey = key;
    }
  }
  if (!bestKey) return null;
  const [aspect, length] = bestKey.split("|");
  return { aspect, length };
}

function BandRow({ band, rows }: { band: string; rows: StratumRow[] }) {
  const ranked = rankArms(rows);
  const s = spreadOf(rows);
  if (ranked.length === 0) return null;
  const best = ranked[0];
  const worst = ranked[ranked.length - 1];
  return (
    <div className="border-t border-slate-100 py-2">
      <div className="flex items-baseline justify-between gap-3">
        <span className="text-[12px] font-medium text-slate-800">
          {axisLabel("homology", band)}
        </span>
        <span className="font-mono text-[11px] text-slate-500">
          {spreadSentence(axisLabel("homology", band), s)}
        </span>
      </div>
      {s ? (
        <div className="mt-1 flex flex-wrap items-baseline gap-x-4 gap-y-0.5 text-[11px]">
          <span className="text-slate-600">
            best{" "}
            <span className="font-medium text-slate-800">
              {best.display_name.split("/").pop()}
            </span>{" "}
            K={best.k}{" "}
            <span className="font-mono">{best.f_micro_w.toFixed(4)}</span>
          </span>
          <span className="text-slate-500">
            worst {worst.display_name.split("/").pop()} K={worst.k}{" "}
            <span className="font-mono">{worst.f_micro_w.toFixed(4)}</span>
          </span>
        </div>
      ) : null}
    </div>
  );
}

export function StratumCompare({
  evaluationSetId,
  setting = "NK",
  k = null,
}: Props) {
  const [data, setData] = useState<Payload | null>(null);
  const [error, setError] = useState<string | null>(null);
  // Chosen here rather than passed in, so the panel can be dropped on any
  // page that knows an evaluation set. Null until the data says which cell
  // is worth opening on.
  const [cell, setCell] = useState<{ aspect: string; length: string } | null>(null);

  useEffect(() => {
    setData(null);
    setError(null);
    fetch(
      `/api-proxy/strata/compare/${evaluationSetId}?setting=${encodeURIComponent(setting)}`,
      { cache: "no-store" },
    )
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(String(r.status)))))
      .then((d: Payload) => {
        setData(d);
        setCell((c) => c ?? busiestCell(d.rows));
      })
      .catch((e) => setError(String(e)));
  }, [evaluationSetId, setting]);

  if (error) return <p className="text-[11px] text-rose-600">strata: {error}</p>;
  if (!data) return <p className="text-[11px] text-slate-400">loading strata…</p>;

  const scoped = atStratum(data.rows, {
    aspect: cell?.aspect,
    length: cell?.length,
  }).filter((r) => k == null || r.k === k);
  const bands = bandsPresent(scoped, "homology");
  const aspects = bandsPresent(data.rows, "aspect");
  const lengths = bandsPresent(data.rows, "length");

  return (
    <section className="rounded-lg border border-slate-200 bg-white px-3 py-2">
      <header className="flex items-baseline justify-between gap-2">
        <h3 className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
          who wins, by identity to the nearest donor
        </h3>
        <span className="text-[10px] text-slate-400">
          {data.arms_with_strata} of {data.arms_total} arms stratified
        </span>
      </header>

      {/* The other two axes are pinned rather than averaged: a band
          comparison over a mixture of aspects is not one measurement. */}
      <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px]">
        <label className="text-slate-500">
          aspect{" "}
          <select
            value={cell?.aspect ?? ""}
            onChange={(e) =>
              setCell((c) => ({ aspect: e.target.value, length: c?.length ?? "" }))
            }
            className="rounded border border-slate-300 bg-white px-1 py-0.5"
          >
            {aspects.map((a) => (
              <option key={a} value={a}>
                {axisLabel("aspect", a)}
              </option>
            ))}
          </select>
        </label>
        <label className="text-slate-500">
          length{" "}
          <select
            value={cell?.length ?? ""}
            onChange={(e) =>
              setCell((c) => ({ aspect: c?.aspect ?? "", length: e.target.value }))
            }
            className="rounded border border-slate-300 bg-white px-1 py-0.5"
          >
            {lengths.map((l) => (
              <option key={l} value={l}>
                {l}
              </option>
            ))}
          </select>
        </label>
      </div>
      <p className="mt-0.5 max-w-2xl text-[11px] leading-snug text-slate-500">
        {/* The reason this panel leads on identity rather than length: one
            axis separates and the other does not. */}
        The band a protein falls in is not a setting: nothing moves a query
        from one to another. A single number over all of them is the number
        for the band that needs least help.
      </p>
      {bands.length === 0 ? (
        <p className="mt-2 text-[11px] text-slate-400">
          nothing stratified at this aspect and length yet
        </p>
      ) : (
        <div className="mt-1">
          {bands.map((band) => (
            <BandRow
              key={band}
              band={band}
              rows={atStratum(scoped, { homology: band })}
            />
          ))}
        </div>
      )}
    </section>
  );
}
