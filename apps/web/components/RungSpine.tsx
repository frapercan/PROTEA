// The campaign line: what each rung asked, and where it stands.
//
// This sits above the board because the board is an answer and a reader
// arriving at it cannot see the question. It is also where the board's
// default comes from: without it the surface opened on every evaluation
// set at once, which puts two temporal windows in one table.
//
// Nothing here is written down anywhere. Every figure is derived from the
// jobs that declared a rung and the evaluations their prediction sets were
// scored into, so a rung that grows an arm says so without anyone editing
// a constant.

"use client";

import Link from "next/link";

import { progressLabel, rungProgress, type Rung } from "@/lib/rungs";

function Bar({ computed, scored, total }: { computed: number; scored: number; total: number }) {
  const pc = total > 0 ? (computed / total) * 100 : 0;
  const ps = total > 0 ? (scored / total) * 100 : 0;
  return (
    <div
      className="relative h-1.5 w-full overflow-hidden rounded-full bg-slate-200"
      // Two bars, not one: computed and scored lag independently, and a
      // single bar would have to pick which of them it meant.
      title={`${computed} computed, ${scored} scored, of ${total}`}
    >
      <div className="absolute inset-y-0 left-0 bg-slate-400" style={{ width: `${pc}%` }} />
      <div className="absolute inset-y-0 left-0 bg-emerald-500" style={{ width: `${ps}%` }} />
    </div>
  );
}

function RungCard({ rung, active }: { rung: Rung; active: boolean }) {
  const p = rungProgress(rung);
  const target = rung.evaluation_set_ids[0];
  const href = target
    ? `/instrument/benchmark/?eval_set=${target}&stage=knn`
    : "/instrument/benchmark/";

  return (
    <li className="min-w-[15rem] flex-1">
      <Link
        href={href}
        className={`block rounded-lg border px-3 py-2 transition hover:border-slate-400 ${
          active ? "border-slate-800 bg-white" : "border-slate-200 bg-slate-50"
        }`}
      >
        <div className="flex items-baseline justify-between gap-2">
          <span className="text-[13px] font-semibold text-slate-800">
            Rung {rung.rung}
          </span>
          <span className="font-mono text-[10px] text-slate-500">
            GOA {rung.window.replace("-", " → ")}
          </span>
        </div>

        <p className="mt-0.5 text-[11px] leading-snug text-slate-600">
          {rung.question}
        </p>

        <div className="mt-2">
          <Bar computed={p.computed} scored={p.scored} total={p.total} />
          <p className="mt-1 text-[10px] text-slate-500">
            {progressLabel(p)}
            {p.live ? " · running" : ""}
            {rung.failed > 0 ? ` · ${rung.failed} failed` : ""}
          </p>
        </div>

        {rung.best ? (
          <p className="mt-1.5 text-[11px] text-slate-700">
            <span className="text-slate-500">best </span>
            <span className="font-medium">{rung.best.model.split("/").pop()}</span>
            <span className="text-slate-500"> at K={rung.best.k}, </span>
            <span className="font-mono">{rung.best.value.toFixed(4)}</span>
            {/* Averaged over the grid, and it says so: a headline read off
                one cell is how a spiky arm beats an even one. */}
            <span className="text-slate-500">
              {" "}
              mean over {rung.best.cells} cells
            </span>
          </p>
        ) : (
          <p className="mt-1.5 text-[11px] text-slate-400">no scored arm yet</p>
        )}
      </Link>
    </li>
  );
}

export function RungSpine({
  rungs,
  activeEvalSetId,
}: {
  rungs: Rung[];
  activeEvalSetId?: string | null;
}) {
  if (rungs.length === 0) return null;
  return (
    <nav aria-label="Campaign rungs" className="mb-4">
      <div className="mb-1.5 flex items-baseline gap-2">
        <h2 className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
          campaign
        </h2>
        <span className="text-[10px] text-slate-400">
          each rung asks one question; the board below answers the one you pick
        </span>
      </div>
      <ol className="flex flex-wrap gap-2">
        {rungs.map((r) => (
          <RungCard
            key={`${r.rung}-${r.window}`}
            rung={r}
            active={
              !!activeEvalSetId && r.evaluation_set_ids.includes(activeEvalSetId)
            }
          />
        ))}
      </ol>
    </nav>
  );
}
