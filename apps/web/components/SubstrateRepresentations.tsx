"use client";

import { Fragment } from "react";
import { useLocale, useTranslations } from "next-intl";
import {
  CircleCheckBig,
  CircleDashed,
  Database,
  GraduationCap,
  PackageOpen,
  type LucideIcon,
} from "lucide-react";

import {
  REPRESENTATION_STATES,
  type GraphRepresentation,
  type GraphRepresentations,
  type RepresentationState,
} from "@/lib/graph";

/**
 * The Substrate node's denominator, expanded.
 *
 * WHY THIS EXISTS. That node reports a ratio, "1 / 13", and a ratio is the
 * least readable thing on the page. It says twelve alternatives were passed
 * over and names neither the one in use nor the twelve that were not, so a
 * reader cannot see that two of the thirteen are the same backbone read at two
 * different layers, cannot see that three of them were FITTED against an
 * annotation release rather than shipped pretrained, and cannot see that every
 * one of them is fully encoded over the corpus and therefore ready to be
 * retrieved against today.
 *
 * WHAT IT REFUSES TO DO. It never ranks. There is no ordering by parameter
 * count, because the column is null for several rows and a field missing from
 * half the table cannot rank it; there is no ordering by coverage, because a
 * fraction of the corpus encoded says nothing about a representation's worth.
 * Rows arrive from the endpoint already grouped by state and alphabetical
 * inside each group, and this component walks them in that order.
 *
 * It computes nothing either. Every count, ratio and flag rendered here came
 * out of GET /v1/graph, and the three counts in the header are the node's own
 * ratio restated from the same rows, so the two cannot disagree.
 */

// ── The three states, drawn with four redundant channels ──────────────────
//
// The same discipline as the edge strengths above it on the page: colour is
// not a channel on its own, so each state also carries a SHAPE (heavy filled
// block, plain outline, dashed), its own icon, and its word printed beside
// them. Deliberately NOT the strength palette: a representation's state is not
// an edge strength, and borrowing emerald or rose would say it was.

type StateStyle = {
  chip: string;
  icon: LucideIcon;
  /** The rail down the left of the group header. */
  rail: string;
};

const STATE_STYLE: Record<RepresentationState, StateStyle> = {
  retrieved: {
    chip: "rounded-md border-2 border-slate-800 bg-slate-800 text-white",
    icon: CircleCheckBig,
    rail: "bg-slate-800",
  },
  built: {
    chip: "rounded-md border border-slate-300 bg-white text-slate-700",
    icon: Database,
    rail: "bg-slate-300",
  },
  unbuilt: {
    chip: "rounded-md border border-dashed border-amber-500 bg-amber-50 text-amber-900",
    icon: CircleDashed,
    rail: "bg-amber-500",
  },
};

/** A state the endpoint sent that this build does not know how to draw. */
const UNKNOWN_STATE: StateStyle = {
  chip: "rounded-md border border-slate-400 bg-slate-100 text-slate-700",
  icon: CircleDashed,
  rail: "bg-slate-400",
};

function stateStyle(state: string): StateStyle {
  return STATE_STYLE[state as RepresentationState] ?? UNKNOWN_STATE;
}

function StateChip({ state, label }: { state: string; label: string }) {
  const s = stateStyle(state);
  const Icon = s.icon;
  return (
    <span
      className={`inline-flex items-center gap-1 whitespace-nowrap px-2 py-0.5 text-xs font-medium ${s.chip}`}
    >
      <Icon className="h-3.5 w-3.5 shrink-0" aria-hidden />
      {label}
    </span>
  );
}

// ── Values that may be absent ─────────────────────────────────────────────

/** The mark for a value the record does not carry. Never a zero, never a dash. */
function Absent({ what }: { what: string }) {
  return (
    <span className="text-xs text-slate-500" title={what}>
      ∅ {what}
    </span>
  );
}

/**
 * A parameter count, in one unit, with the exact figure in the title.
 *
 * Always millions, never a mix of M and B. A column that switches unit between
 * rows cannot be compared down its own length, which is the only thing anybody
 * reads a size column for.
 */
function paramsInMillions(n: number | null | undefined): string {
  return n == null ? "∅" : `${(n / 1e6).toFixed(1)}M`;
}

// ── The table ─────────────────────────────────────────────────────────────

type Props = {
  representations: GraphRepresentations;
};

export function SubstrateRepresentations({ representations }: Props) {
  const t = useTranslations("graph.rep");
  const locale = useLocale();
  // Every count on this row can be absent. The endpoint types them nullable
  // because a configuration whose embeddings were never stored has no count,
  // and a component that assumes otherwise throws inside render: React unmounts
  // the whole tree, so one null in one row takes down every section on the
  // page and leaves a heading with nothing under it. Measured, not feared.
  const n = (v: number | null | undefined) =>
    v == null ? "∅" : v.toLocaleString(locale);
  const rows = representations.rows;

  // How many rows carry a parameter count. Printed rather than hidden: a
  // reader who sees six numbers and seven blanks needs to know the blanks are
  // the record's and not the page's, and that they are why nothing is sorted
  // by that column.
  const withParams = rows.filter((r) => r.param_count != null).length;

  if (rows.length === 0) {
    return (
      <p className="rounded-lg border border-slate-200 bg-white px-4 py-6 text-center text-sm text-slate-600">
        {t("empty")}
      </p>
    );
  }

  return (
    <div className="space-y-3" data-testid="graph-representations">
      {/* The node's ratio, in words, with the third state the ratio hides.
          "1 of 13" cannot say that the 13 is itself a subset of what is
          registered, and the difference between a built alternative and an
          unbuilt one is the difference between something passed over and
          something that never existed. */}
      <dl
        className="flex flex-wrap items-stretch gap-2"
        data-testid="graph-representations-counts"
      >
        {[
          { key: "total", value: representations.total },
          { key: "built", value: representations.built },
          { key: "retrieved", value: representations.retrieved },
        ].map((c) => (
          <div
            key={c.key}
            className="min-w-[9rem] flex-1 rounded-md border border-slate-200 bg-white px-3 py-2"
          >
            <dt className="text-[11px] uppercase tracking-wider text-slate-500">
              {t(`count.${c.key}`)}
            </dt>
            <dd className="font-mono text-lg leading-tight text-slate-900">{n(c.value)}</dd>
            <dd className="mt-0.5 text-[11px] leading-snug text-slate-500">
              {t(`countWhy.${c.key}`)}
            </dd>
          </div>
        ))}
        <div className="min-w-[9rem] flex-1 rounded-md border border-slate-200 bg-white px-3 py-2">
          <dt className="text-[11px] uppercase tracking-wider text-slate-500">{t("corpus")}</dt>
          <dd className="font-mono text-lg leading-tight text-slate-900">
            {representations.corpus_sequences != null ? (
              n(representations.corpus_sequences)
            ) : (
              <Absent what={t("unrecorded")} />
            )}
          </dd>
          <dd className="mt-0.5 text-[11px] leading-snug text-slate-500">{t("corpusWhy")}</dd>
        </div>
      </dl>

      <p className="text-xs leading-relaxed text-slate-500">
        {t("paramsNote", { recorded: withParams, total: rows.length })}
      </p>

      <div tabIndex={0} className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="w-full min-w-[62rem] text-sm">
          <caption className="sr-only">{t("caption")}</caption>
          <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
            <tr>
              <th scope="col" className="px-3 py-2.5">{t("colRepresentation")}</th>
              <th scope="col" className="px-3 py-2.5">{t("colKind")}</th>
              <th scope="col" className="px-3 py-2.5">{t("colBackend")}</th>
              <th scope="col" className="px-3 py-2.5 text-right whitespace-nowrap">
                {t("colParams")}
              </th>
              <th scope="col" className="px-3 py-2.5">{t("colLayer")}</th>
              <th scope="col" className="px-3 py-2.5">{t("colPooling")}</th>
              <th scope="col" className="px-3 py-2.5 text-right">{t("colCoverage")}</th>
              <th scope="col" className="px-3 py-2.5 text-right">{t("colUse")}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {rows.map((r, i) => {
              // A group header whenever the state changes. The endpoint sends
              // the rows already grouped, so this is a boundary the payload
              // declares rather than one the client decides.
              const opensGroup = i === 0 || rows[i - 1].state !== r.state;
              return (
                <Fragment key={r.id}>
                  {opensGroup && <GroupHeader state={r.state} rows={rows} />}
                  <Row row={r} corpus={representations.corpus_sequences} />
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/** One state's band, with what the state means and how many rows are in it. */
function GroupHeader({
  state,
  rows,
}: {
  state: RepresentationState;
  rows: GraphRepresentation[];
}) {
  const t = useTranslations("graph.rep");
  const known = REPRESENTATION_STATES.includes(state);
  const count = rows.filter((r) => r.state === state).length;
  const s = stateStyle(state);
  return (
    <tr className="bg-slate-50/80">
      <th scope="colgroup" colSpan={8} className="px-0 py-2 text-left font-normal">
        <span className="flex items-stretch gap-2">
          <span className={`w-1 shrink-0 self-stretch rounded-sm ${s.rail}`} aria-hidden />
          <span className="flex flex-wrap items-baseline gap-x-3 gap-y-1 pr-3">
            <StateChip state={state} label={known ? t(`state.${state}`) : state} />
            <span className="font-mono text-xs text-slate-500">{count}</span>
            {known && (
              <span className="max-w-3xl text-xs leading-relaxed text-slate-600">
                {t(`stateWhy.${state}`)}
              </span>
            )}
          </span>
        </span>
      </th>
    </tr>
  );
}

function Row({ row, corpus }: { row: GraphRepresentation; corpus: number | null }) {
  const t = useTranslations("graph.rep");
  const locale = useLocale();
  // Every count on this row can be absent. The endpoint types them nullable
  // because a configuration whose embeddings were never stored has no count,
  // and a component that assumes otherwise throws inside render: React unmounts
  // the whole tree, so one null in one row takes down every section on the
  // page and leaves a heading with nothing under it. Measured, not feared.
  const n = (v: number | null | undefined) =>
    v == null ? "∅" : v.toLocaleString(locale);
  const fitted = row.trained_on !== null;
  const KindIcon = fitted ? GraduationCap : PackageOpen;
  const short = row.coverage != null && row.coverage < 1;

  return (
    <tr className="align-top hover:bg-slate-50/60">
      {/* Identity. The label is display_name when the record carries one and
          the raw model name otherwise, so a configuration nobody named still
          arrives with something a reader can say out loud. The model name is
          printed underneath whenever it differs, because two configurations of
          one backbone share it and the label is what tells them apart. */}
      <td className="px-3 py-3">
        <div className="font-medium text-slate-900">{row.label}</div>
        {row.display_name === null ? (
          <div className="mt-0.5">
            <span className="text-[11px] text-slate-500" title={t("unnamedWhy")}>
              {t("unnamed")}
            </span>
          </div>
        ) : (
          row.model_name !== row.label && (
            <div className="mt-0.5 font-mono text-[11px] break-all text-slate-500">
              {row.model_name}
            </div>
          )
        )}
      </td>

      {/* Fitted or pretrained. Not a setting of one knob: an encoding fitted
          against an annotation release and a backbone that saw none of ours
          answer different questions, and the release it was fitted against is
          the fact that decides whether it can be read in this frame at all. */}
      <td className="px-3 py-3">
        <span
          className={`inline-flex items-center gap-1 whitespace-nowrap rounded-md px-2 py-0.5 text-xs font-medium ${
            fitted
              ? "border border-violet-300 bg-violet-50 text-violet-900"
              : "border border-slate-200 bg-slate-50 text-slate-700"
          }`}
          title={fitted ? t("kindFittedWhy") : t("kindPretrainedWhy")}
        >
          <KindIcon className="h-3.5 w-3.5 shrink-0" aria-hidden />
          {fitted ? t("kindFitted") : t("kindPretrained")}
        </span>
        {row.trained_on && (
          <div className="mt-1 font-mono text-[11px] whitespace-nowrap text-slate-600">
            {row.trained_on.source ?? t("unrecorded")} {row.trained_on.version ?? ""}
            {row.trained_on.published_at && (
              <span className="text-slate-500"> · {row.trained_on.published_at}</span>
            )}
          </div>
        )}
      </td>

      {/* The backend is NOT NULL and says how the vector is actually computed.
          The family is display metadata and is null for some rows, so it is
          shown when present and never stood in for. */}
      <td className="px-3 py-3">
        <div className="font-mono text-xs text-slate-900">{row.model_backend}</div>
        {row.family !== null && row.family !== row.model_backend && (
          <div className="mt-0.5 text-[11px] text-slate-500">{row.family}</div>
        )}
      </td>

      <td className="px-3 py-3 text-right whitespace-nowrap">
        {row.param_count != null ? (
          <span
            className="font-mono text-sm text-slate-900"
            title={`${n(row.param_count)} ${t("parameters")}`}
          >
            {paramsInMillions(row.param_count)}
          </span>
        ) : (
          <Absent what={t("unrecorded")} />
        )}
      </td>

      {/* The layer, verbatim, with the convention in the title. Two rows here
          are the same backbone at two layers, and without the index printed
          they read as a duplicate row rather than as the one axis anybody
          ever varied on this node. */}
      <td className="px-3 py-3">
        <span className="font-mono text-xs text-slate-900" title={t("layerHint")}>
          {row.layer_indices ?? "∅"}
        </span>
        {row.layer_agg && <div className="mt-0.5 text-[11px] text-slate-500">{row.layer_agg}</div>}
      </td>

      <td className="px-3 py-3">
        <span className="font-mono text-xs text-slate-900">{row.pooling ?? "∅"}</span>
        <div className="mt-0.5 flex flex-wrap gap-1">
          {row.normalize && (
            <span
              className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-600"
              title={t("l2Hint")}
            >
              L2
            </span>
          )}
          {row.normalize_residues && (
            <span
              className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-600"
              title={t("l2ResidueHint")}
            >
              L2·res
            </span>
          )}
        </div>
      </td>

      {/* Coverage, and the shortfall when there is one. A representation that
          is 60 sequences short of the corpus is not fully built, and rounding
          that to 100% would turn an unfinished encoding into an available
          alternative. */}
      <td className="px-3 py-3 text-right">
        {row.coverage != null ? (
          <>
            <div
              className={`font-mono text-sm ${short ? "font-semibold text-amber-700" : "text-slate-900"}`}
            >
              {row.coverage == null
                ? "∅"
                : // Three decimals, and not two. A corpus short by 60 sequences
                  // of 528,294 is 99.989 per cent, and two decimals render that
                  // as 99.99, which reads as complete. The digit that shows a
                  // shortfall is the one worth keeping, and a test fixes it.
                  new Intl.NumberFormat(locale, {
                    style: "percent",
                    minimumFractionDigits: 3,
                    maximumFractionDigits: 3,
                  }).format(row.coverage)}
            </div>
            <div className="mt-0.5 font-mono text-[11px] text-slate-500">
              {n(row.embeddings_stored)}
            </div>
            {short && corpus != null && (
              <div className="mt-0.5 text-[11px] text-amber-700">
                {t("coverageShort", { count: n(corpus - row.embeddings_stored) })}
              </div>
            )}
          </>
        ) : (
          <>
            <div className="font-mono text-sm text-slate-900">{n(row.embeddings_stored)}</div>
            <div className="mt-0.5 text-[11px] text-amber-700">
              {row.use_chunking ? t("coverageChunked") : t("coverageNoCorpus")}
            </div>
          </>
        )}
      </td>

      {/* What was ever done in it. Zero is printed as a word, not as a 0: a
          representation nobody retrieved in is the finding, and a column of
          zeros reads as a column that failed to load. */}
      <td className="px-3 py-3 text-right">
        {row.prediction_sets === 0 && row.results === 0 ? (
          <span className="text-xs text-slate-500">{t("useNever")}</span>
        ) : (
          <div className="space-y-0.5">
            <div className="text-xs text-slate-900">
              <span className="font-mono">{n(row.prediction_sets)}</span>{" "}
              <span className="text-slate-500">{t("predictionSets")}</span>
            </div>
            <div className="text-[11px] text-slate-500">
              <span className="font-mono">{n(row.results)}</span> {t("scoredResults")}
            </div>
          </div>
        )}
      </td>
    </tr>
  );
}
