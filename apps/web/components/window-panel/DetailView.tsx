"use client";

// Every result shown, with its factors as columns.

import Link from "next/link";
import { useState } from "react";
import { useTranslations } from "next-intl";
import {
  armLabel,
  armModel,
  cellEdge,
  cellF,
  CELLS,
  isNeighbour,
  mayNotBeRead,
  type SummaryResult,
} from "@/lib/windowPanel";
import { INK, deltaFill, fmt, signed } from "./chart";
import type { PanelView } from "./WindowPanel";

type SortKey = "f" | "delta" | "model" | "scoring" | "k" | "time";

export default function DetailView({ view }: { view: PanelView }) {
  const t = useTranslations("windowPanel");
  const { state, commit, shown, reference: ref } = view;
  const [query, setQuery] = useState(state.q);
  const sort = (["f", "delta", "model", "scoring", "k", "time"].includes(state.sort) ? state.sort : "f") as SortKey;
  const direction = state.dir === "asc" ? 1 : -1;
  const refF = (c: string) => cellF(ref, c) ?? 0;
  const keyOf: Record<SortKey, (r: SummaryResult) => number | string> = {
    f: (r) => cellF(r, state.cell) ?? -1,
    delta: (r) => (cellF(r, state.cell) ?? -1) - refF(state.cell),
    model: (r) => (isNeighbour(r) ? armModel(r) : (r.arm.method ?? "")),
    scoring: (r) => r.arm.scoring ?? "",
    k: (r) => r.arm.depth ?? 0,
    time: (r) => r.created_at ?? "",
  };
  const needle = query.trim().toLowerCase();
  const rows = shown
    .filter((r) => !needle || `${armLabel(r)} ${r.id}`.toLowerCase().includes(needle))
    .sort((a, b) => {
      const x = keyOf[sort](a);
      const y = keyOf[sort](b);
      return (x > y ? 1 : x < y ? -1 : 0) * direction;
    });
  const header = (key: SortKey, label: string, numeric = false) => (
    <th
      className={`cursor-pointer whitespace-nowrap px-3 py-2 ${numeric ? "text-right" : ""}`}
      aria-sort={sort === key ? (direction > 0 ? "ascending" : "descending") : "none"}
      onClick={() => commit({ sort: key, dir: sort === key && state.dir === "desc" ? "asc" : "desc" })}
    >
      {label}
      {sort === key ? (direction > 0 ? " ↑" : " ↓") : ""}
    </th>
  );

  return (
    <>
      <p className="text-sm font-medium text-slate-800">{t("detail.question")}</p>
      <p className="text-xs text-slate-500">{t("detail.hint")}</p>
      <input
        type="search"
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          commit({ q: e.target.value });
        }}
        placeholder={t("detail.search")}
        aria-label={t("detail.search")}
        className="w-72 max-w-full rounded-md border bg-white px-2 py-1 text-sm"
      />
      <div className="overflow-x-auto rounded-lg border bg-white">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs uppercase tracking-wide text-slate-500">
              <th className="px-3 py-2" />
              <th className="px-3 py-2">{t("detail.result")}</th>
              {header("model", t("model"))}
              {header("scoring", t("scoring"))}
              {header("k", "k", true)}
              {header("f", `F ${state.cell.replace(":", " ")}`, true)}
              {header("delta", t("detail.vsReference"), true)}
              <th className="px-3 py-2">{t("detail.edge")}</th>
              <th className="whitespace-nowrap px-3 py-2">{t("detail.leak")}</th>
              <th className="px-3 py-2">{t("detail.cells")}</th>
              {header("time", t("detail.time"))}
              <th className="px-3 py-2">{t("detail.set")}</th>
            </tr>
          </thead>
          <tbody>
            {rows.length ? (
              rows.map((r) => {
                const colour = view.focusColour(r);
                const v = cellF(r, state.cell);
                return (
                  <tr key={r.id} className="cursor-pointer border-t hover:bg-blue-50" onClick={() => view.toggleFocus(r)}>
                    <td className="px-3 py-1.5">{colour ? <i className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: colour }} /> : null}</td>
                    <td className="px-3 py-1.5 font-mono text-xs">{r.id.slice(0, 8)}</td>
                    <td className="px-3 py-1.5">
                      {isNeighbour(r) ? armModel(r) : armLabel(r)}
                      {r === ref ? ` (${t("referenceShort")})` : ""}
                    </td>
                    <td className="px-3 py-1.5">{r.arm.scoring ?? "—"}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{r.arm.depth ?? "—"}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{fmt(v)}</td>
                    <td className="px-3 py-1.5 text-right tabular-nums">{r === ref ? "—" : signed((v ?? 0) - refF(state.cell))}</td>
                    <td className="px-3 py-1.5 text-xs" style={{ color: INK.edge }}>{cellEdge(r, state.cell) ? t("detail.edgeShort") : ""}</td>
                    <td className={`whitespace-nowrap px-3 py-1.5 text-xs ${mayNotBeRead(r) ? "font-semibold text-rose-700" : "text-slate-500"}`}>
                      {r.arm.leak ? t(`leak.${r.arm.leak}`, { cut: r.arm.training_release ?? "" }) : "—"}
                    </td>
                    <td className="px-3 py-1.5">
                      <span className="inline-grid grid-cols-9 gap-px align-middle">
                        {CELLS.map((c) => {
                          const delta = r === ref ? null : (cellF(r, c) ?? 0) - refF(c);
                          return (
                            <i
                              key={c}
                              title={`${c.replace(":", " ")} ${fmt(cellF(r, c))}`}
                              className="block h-3 w-2.5 rounded-sm"
                              style={{ background: deltaFill(delta).fill, outline: c === state.cell ? "1.5px solid #0c0a09" : undefined }}
                            />
                          );
                        })}
                      </span>
                    </td>
                    <td className="whitespace-nowrap px-3 py-1.5 text-xs text-slate-500">{r.created_at ? r.created_at.slice(11, 16) : ""}</td>
                    <td className="px-3 py-1.5 text-xs" onClick={(e) => e.stopPropagation()}>
                      <Link className="text-blue-800 underline" href={`/${view.locale}/instrument/functional-annotation/${r.prediction_set_id}`}>
                        {t("detail.setLink")}
                      </Link>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={12} className="px-3 py-3 text-xs text-slate-500">{t("detail.nothing")}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </>
  );
}
