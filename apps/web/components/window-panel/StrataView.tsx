"use client";

// Where each arm wins: the cell split by identity with the closest measured
// member at t0, or by length, as the window's placement places its targets.

import Link from "next/link";
import { useTranslations } from "next-intl";
import { baseUrl } from "@/lib/api";
import {
  ASPECT_LETTER,
  BAND_KEY,
  BANDS,
  CELLS,
  MIN_PROTEINS,
  armLabel,
  armShort,
  bandOf,
  bandRanking,
  cellF,
  isNeighbour,
  type Axis,
  type SummaryResult,
} from "@/lib/windowPanel";
import { FOCUS, Figure, INK, Legend, extent, fmt, mix, pressable, signed, ticks, useTooltip } from "./chart";
import type { PanelView } from "./WindowPanel";

function armsOf(view: PanelView): { arms: SummaryResult[]; automatic: boolean } {
  const ref = view.reference;
  const focus = view.focus.filter((r) => r !== ref && r.strata);
  if (focus.length) return { arms: focus, automatic: false };
  const best = view.shown
    .filter((r) => r !== ref && r.strata)
    .sort((a, b) => (cellF(b, view.state.cell) ?? -1) - (cellF(a, view.state.cell) ?? -1))[0];
  return { arms: best ? [best] : [], automatic: true };
}

export default function StrataView({ view }: { view: PanelView }) {
  const t = useTranslations("windowPanel");
  const { state, commit, reference: ref } = view;
  const tip = useTooltip();
  const axis: Axis = state.axis === "length" ? "length" : "homology";
  const keys = BANDS[axis];
  const { arms, automatic } = armsOf(view);
  const series = [
    ...arms.map((r, i) => ({ r, colour: automatic ? FOCUS[0] : (view.focusColour(r) ?? FOCUS[i]), dashed: false })),
    ...(ref ? [{ r: ref, colour: INK.reference, dashed: true }] : []),
  ];
  const bandName = (b: string) => t(`band.${BAND_KEY[b]}`);

  if (!ref?.strata) {
    return (
      <>
        <p className="text-sm font-medium text-slate-800">{t("strata.question")}</p>
        <p className="rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-900">{t("strata.notYet")}</p>
      </>
    );
  }

  const values = series.flatMap((s) => keys.map((k) => bandOf(s.r, state.cell, axis, k)?.f).filter((v): v is number => v != null));
  const [lo, hi] = extent(values);
  const W = 900;
  const H = 290;
  const m = { l: 46, r: 16, t: 14, b: 44 };
  const span = W - m.l - m.r - 80;
  const X = (i: number) => m.l + 40 + (i / (keys.length - 1)) * span;
  const Y = (v: number) => m.t + (1 - (v - lo) / (hi - lo)) * (H - m.t - m.b);
  const bw = span / (keys.length - 1);
  const band = keys.includes(state.band) ? state.band : "";
  const { ranked, rival } = band ? bandRanking(view.shown, state.cell, axis, band) : { ranked: [], rival: null };
  const [setting, namespace] = state.cell.split(":");
  const refBand = band ? bandOf(ref, state.cell, axis, band) : null;

  return (
    <>
      <p className="text-sm font-medium text-slate-800">{t("strata.question")}</p>
      <p className="text-xs text-slate-500">{t("strata.hint")}</p>
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="text-[11px] uppercase tracking-wide text-slate-500">{t("strata.axis")}</span>
        <span className="inline-flex overflow-hidden rounded-md border bg-white">
          {(["homology", "length"] as const).map((a) => (
            <button key={a} type="button" aria-pressed={axis === a} onClick={() => commit({ axis: a, band: "" }, { push: true })} className={`px-2.5 py-0.5 ${axis === a ? "bg-blue-50 font-semibold text-blue-800" : ""}`}>
              {t(`strata.${a}`)}
            </button>
          ))}
        </span>
        <span className="text-slate-500">{t("strata.clickBand")}</span>
      </div>
      <Figure
        title={t(`strata.profileTitle.${axis}`, { cell: state.cell.replace(":", " ") })}
        legend={
          <Legend
            items={[
              ...series.map((s) => ({
                label: s.r === ref ? `${armLabel(s.r)} (${t("referenceShort")})` : armLabel(s.r),
                swatch: s.dashed ? { border: `2px dashed ${s.colour}` } : { background: s.colour },
              })),
              ...(automatic ? [{ label: t("strata.automatic"), swatch: { display: "none" } }] : []),
            ]}
          />
        }
        caption={t(`strata.profileCaption.${axis}`)}
      >
        <svg width={W} viewBox={`0 0 ${W} ${H}`} role="img" aria-label={t(`strata.profileTitle.${axis}`, { cell: state.cell })} style={{ maxWidth: "none" }}>
          {keys.map((k, i) => {
            const n = bandOf(ref, state.cell, axis, k)?.n;
            return (
              <g key={k}>
                <rect
                  x={X(i) - bw / 2 + 2}
                  y={m.t - 6}
                  width={bw - 4}
                  height={H - m.t - m.b + 10}
                  rx={6}
                  fill={k === band ? INK.selected : "transparent"}
                  {...pressable(() => commit({ band: k === band ? "" : k }, { push: true }))}
                  {...tip.bind(
                    <>
                      <b>{state.cell.replace(":", " ")}, {bandName(k)}</b>
                      <br />
                      {t("strata.proteins", { n: n ?? 0 })}
                      {series.map((s) => (
                        <span key={s.r.id}>
                          <br />
                          {armShort(s.r)}: {fmt(bandOf(s.r, state.cell, axis, k)?.f)}
                        </span>
                      ))}
                    </>,
                  )}
                />
                <text x={X(i)} y={H - 24} textAnchor="middle" fontSize={12} fontWeight={k === band ? 700 : 400} fill="#0c0a09" pointerEvents="none">{bandName(k)}</text>
                <text x={X(i)} y={H - 8} textAnchor="middle" fontSize={11} fill="#78716c" pointerEvents="none">{n != null ? `n ${n}` : ""}</text>
              </g>
            );
          })}
          {ticks(lo, hi, 0.05).map((v) => (
            <g key={v} pointerEvents="none">
              <line x1={m.l} x2={W - m.r} y1={Y(v)} y2={Y(v)} stroke={INK.grid} />
              <text x={m.l - 6} y={Y(v) + 4} textAnchor="end" fontSize={11} fill="#78716c">{v.toFixed(2)}</text>
            </g>
          ))}
          {series.map((s) => {
            const points = keys
              .map((k, i) => [i, bandOf(s.r, state.cell, axis, k)] as const)
              .filter(([, b]) => b != null && b.n >= MIN_PROTEINS) as [number, { n: number; f: number }][];
            return (
              <g key={s.r.id} pointerEvents="none">
                <polyline points={points.map(([i, b]) => `${X(i)},${Y(b.f)}`).join(" ")} fill="none" stroke={s.colour} strokeWidth={2} strokeDasharray={s.dashed ? "5 4" : undefined} />
                {points.map(([i, b]) => (
                  <circle key={i} cx={X(i)} cy={Y(b.f)} r={4} fill={s.colour} stroke={INK.surface} strokeWidth={1.5} />
                ))}
              </g>
            );
          })}
        </svg>
      </Figure>
      {band ? (
        <section className="grid gap-2 rounded-lg border border-blue-700 bg-white px-4 py-3">
          <h3 className="text-sm font-semibold text-slate-800">
            {t("strata.bandTitle", { cell: state.cell.replace(":", " "), band: bandName(band), proteins: refBand?.n ?? 0 })}
          </h3>
          {ranked.length ? (
            <>
              <p className="text-sm text-slate-700">
                {rival
                  ? t("strata.bandWinner", {
                      family: isNeighbour(ranked[0].result) ? t("strata.neighbours") : t("strata.methods"),
                      margin: signed(ranked[0].f - rival.f),
                      winner: armLabel(ranked[0].result),
                      winnerF: fmt(ranked[0].f),
                      rival: armLabel(rival.result),
                      rivalF: fmt(rival.f),
                    })
                  : t("strata.bandAlone", { winner: armLabel(ranked[0].result), winnerF: fmt(ranked[0].f) })}
              </p>
              <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs">
                <a
                  className="text-blue-800 underline"
                  target="_blank"
                  rel="noopener"
                  href={`${baseUrl()}/strata/${ranked[0].result.id}/proteins?setting=${setting}&aspect=${ASPECT_LETTER[namespace]}&${axis}=${encodeURIComponent(band)}&sort=f_asc&limit=100`}
                >
                  {t("strata.proteinsLink")}
                </a>
                <Link className="text-blue-800 underline" href={`/${view.locale}/instrument/functional-annotation/${ranked[0].result.prediction_set_id}`}>
                  {t("strata.setLink")}
                </Link>
                {axis === "homology" ? (
                  <button type="button" className="text-blue-800 underline" onClick={() => commit({ tab: "trajectory", measure: band }, { push: true })}>
                    {t("strata.trajectoryLink")}
                  </button>
                ) : null}
              </div>
              <div className="overflow-x-auto rounded-md border">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
                      <th className="px-3 py-1.5">#</th>
                      <th className="px-3 py-1.5">{t("strata.arm")}</th>
                      <th className="px-3 py-1.5 text-right">{t("strata.fBand")}</th>
                      <th className="px-3 py-1.5 text-right">{t("strata.vsReference")}</th>
                      <th className="px-3 py-1.5 text-right">{t("strata.fCell")}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {ranked.slice(0, 12).map((x, i) => (
                      <tr key={x.result.id} className="cursor-pointer border-t hover:bg-blue-50" onClick={() => view.toggleFocus(x.result)}>
                        <td className="px-3 py-1">{i + 1}</td>
                        <td className="px-3 py-1">
                          {view.focusColour(x.result) ? <i className="mr-1.5 inline-block h-2 w-2 rounded-full" style={{ background: view.focusColour(x.result) ?? undefined }} /> : null}
                          {armLabel(x.result)}
                          {x.result === ref ? ` (${t("referenceShort")})` : ""}
                        </td>
                        <td className="px-3 py-1 text-right tabular-nums">{fmt(x.f)}</td>
                        <td className="px-3 py-1 text-right tabular-nums">{x.result === ref || !refBand ? "—" : signed(x.f - refBand.f)}</td>
                        <td className="px-3 py-1 text-right tabular-nums">{fmt(cellF(x.result, state.cell))}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {ranked.length > 12 ? <p className="text-xs text-slate-500">{t("strata.more", { count: ranked.length - 12 })}</p> : null}
            </>
          ) : (
            <p className="text-xs text-slate-500">{t("strata.underFloor")}</p>
          )}
        </section>
      ) : null}
      <Winners view={view} axis={axis} tip={tip} bandName={bandName} />
      {tip.node}
    </>
  );
}

function Winners({
  view,
  axis,
  tip,
  bandName,
}: {
  view: PanelView;
  axis: Axis;
  tip: ReturnType<typeof useTooltip>;
  bandName: (b: string) => string;
}) {
  const t = useTranslations("windowPanel");
  const keys = BANDS[axis];
  const label = 64;
  const width = 168;
  const height = 28;
  const top = 22;
  return (
    <Figure
      title={t("strata.winnersTitle")}
      legend={
        <Legend
          items={[
            { label: t("strata.neighbours"), swatch: { background: mix(INK.surface, FOCUS[0], 0.26), border: `1px solid ${FOCUS[0]}` } },
            { label: t("strata.methods"), swatch: { background: mix(INK.surface, INK.reference, 0.18), border: `1px solid ${INK.reference}` } },
            { label: t("strata.close"), swatch: { display: "none" } },
          ]}
        />
      }
      caption={t("strata.winnersCaption")}
    >
      <svg width={label + width * keys.length} viewBox={`0 0 ${label + width * keys.length} ${top + height * CELLS.length + 4}`} role="img" aria-label={t("strata.winnersTitle")} style={{ maxWidth: "none" }}>
        {keys.map((k, j) => (
          <text key={k} x={label + width * j + width / 2} y={13} textAnchor="middle" fontSize={11.5} fill="#78716c">{bandName(k)}</text>
        ))}
        {CELLS.map((c, i) => {
          const y = top + height * i;
          return (
            <g key={c}>
              <text x={0} y={y + height / 2 + 4} fontSize={12} fontWeight={c === view.state.cell ? 700 : 400} fill="#0c0a09">{c.replace(":", " ")}</text>
              {keys.map((k, j) => {
                const { ranked, rival } = bandRanking(view.shown, c, axis, k);
                const x = label + width * j + 1;
                if (ranked.length < 2) {
                  return <rect key={k} x={x} y={y + 1} width={width - 2} height={height - 2} rx={3} fill="none" stroke="#e7e5e4" strokeDasharray="2 2" />;
                }
                const w = ranked[0];
                const margin = rival ? w.f - rival.f : 0;
                const close = margin < 0.01;
                const base = isNeighbour(w.result) ? FOCUS[0] : INK.reference;
                const selected = c === view.state.cell && k === view.state.band;
                return (
                  <g key={k}>
                    <rect
                      x={x}
                      y={y + 1}
                      width={width - 2}
                      height={height - 2}
                      rx={3}
                      fill={mix(INK.surface, base, close ? 0.08 : isNeighbour(w.result) ? 0.26 : 0.18)}
                      stroke={selected ? "#0c0a09" : base}
                      strokeWidth={selected ? 2 : close ? 0.6 : 1}
                      {...pressable(() => view.commit({ cell: c, band: k }, { push: true }))}
                      {...tip.bind(
                        <>
                          <b>{c.replace(":", " ")}, {bandName(k)}</b> ({t("strata.proteins", { n: w.n })})
                          <br />
                          {armLabel(w.result)}: {fmt(w.f)}
                          {rival ? <><br />{armLabel(rival.result)}: {fmt(rival.f)}</> : null}
                        </>,
                      )}
                    />
                    <text x={x + (width - 2) / 2} y={y + height / 2 + 4} textAnchor="middle" fontSize={11} fill="#0c0a09" pointerEvents="none">
                      {armShort(w.result)} {signed(margin)}
                    </text>
                  </g>
                );
              })}
            </g>
          );
        })}
      </svg>
    </Figure>
  );
}
