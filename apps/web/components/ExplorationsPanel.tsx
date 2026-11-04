"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslations } from "next-intl";

import { OperatorOnlyNotice, useMayLaunchJobs } from "@/components/OperatorOnly";
import {
  decideExploration,
  errorText,
  getExploration,
  getExplorationReceipt,
  listExplorations,
  moveExploration,
  type Exploration,
  type ExplorationEvent,
  type ExplorationHeadline,
  type ExplorationReceipt,
} from "@/lib/api";
import { formatDateTime } from "@/lib/format";
import { useIsAuthenticated } from "@/lib/useRole";

/**
 * Evaluated explorations (ADR-D55): one hypothesis each, with its frozen rules,
 * its arms, a receipt computed from rows, a report and a review in two steps.
 *
 * WHY THIS EXISTS. The author, 2026-11-04: "Necesitamos una pantalla que nos
 * atomice cada exploración evaluada (hipótesis)", in place of the experimentation
 * graph and the benchmark table, which were too complex and crowned a champion by
 * maximum. So nothing here is sorted by a figure: explorations are grouped by the
 * question they answer and listed by number, and a verdict is shown only as the
 * server computed it from the report.
 *
 * The decision is the author's, from a signed-in session. The server refuses it to
 * an API key whatever its role; this page only offers the form where it can succeed.
 */

const STATE_STYLE: Record<string, string> = {
  draft: "bg-slate-100 text-slate-700",
  running: "bg-sky-100 text-sky-800",
  reported: "bg-amber-100 text-amber-800",
  decided: "bg-emerald-100 text-emerald-800",
};

function Chip({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <span className={`rounded px-1.5 py-0.5 text-[11px] font-medium ${className}`}>{children}</span>;
}

/** The report as the server wrote it into the log's report event. */
type ReportPanel = { status?: string; verdict?: string; n_paired?: number | null };
type ReportDocument = {
  verdict?: string | null;
  grade?: string | null;
  checklist?: { items?: Record<string, { answer: string; problems?: string[] }> };
  comparisons?: { job_id: string; panels: Record<string, ReportPanel> }[];
};

function latestReport(events: ExplorationEvent[]): ExplorationEvent | undefined {
  return [...events].reverse().find((e) => e.event === "report");
}

function ReceiptBlock({ receipt }: { receipt: ExplorationReceipt }) {
  const t = useTranslations("explorations");
  return (
    <section className="space-y-2">
      <h3 className="text-sm font-semibold">{t("blocks.receipt")}</h3>
      <table className="w-full text-xs">
        <tbody>
          {Object.entries(receipt.layers).map(([layer, value]) => (
            <tr key={layer} className="border-t border-slate-100 align-top">
              <td className="py-1 pr-3 font-medium text-slate-600">{layer}</td>
              <td className="py-1 pr-3">{value.status}</td>
              <td className="py-1 font-mono text-[11px] text-slate-500">
                {typeof value.detail === "string" ? value.detail.split("\n")[0] : ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="text-xs text-slate-600">
        {receipt.blocks_verdict.length
          ? `${t("receipt.blocksVerdict")}: ${receipt.blocks_verdict.join("; ")}`
          : t("receipt.nothingBlocks")}
      </p>
    </section>
  );
}

function ReportBlock({ event }: { event: ExplorationEvent | undefined }) {
  const t = useTranslations("explorations");
  if (!event) return <p className="text-xs text-slate-500">{t("report.none")}</p>;
  const report = (event.body.report ?? {}) as ReportDocument;
  const items = report.checklist?.items ?? {};
  const comparisons = report.comparisons ?? [];
  return (
    <section className="space-y-2">
      <h3 className="text-sm font-semibold">{t("blocks.report")}</h3>
      <p className="text-xs">
        {t("report.verdict")}: <strong>{report.verdict ?? t("report.noVerdict")}</strong> · {t("report.grade")}:{" "}
        <strong>{report.grade ?? t("report.noVerdict")}</strong>
      </p>
      <p className="break-all font-mono text-[11px] text-slate-500">sha256 {String(event.body.sha256 ?? "")}</p>
      <ul className="grid grid-cols-1 gap-1 text-xs sm:grid-cols-2">
        {Object.entries(items).map(([item, check]) => (
          <li key={item}>
            <span className="font-medium">{item}</span>: {check.answer}
            {check.problems?.length ? ` (${check.problems.join("; ")})` : ""}
          </li>
        ))}
      </ul>
      {comparisons.map((c) => (
        <table key={c.job_id} className="w-full text-xs">
          <thead>
            <tr className="text-left text-slate-500">
              <th className="pr-3 font-medium">{t("report.panel")}</th>
              <th className="pr-3 font-medium">{t("report.status")}</th>
              <th className="pr-3 font-medium">{t("report.reading")}</th>
              <th className="font-medium">n</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(c.panels).map(([key, panel]) => (
              <tr key={key} className="border-t border-slate-100">
                <td className="pr-3 font-mono">{key}</td>
                <td className="pr-3">{panel.status}</td>
                <td className="pr-3">{panel.verdict}</td>
                <td>{panel.n_paired ?? ""}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ))}
    </section>
  );
}

function DecisionForm({ exploration, onDone }: { exploration: Exploration; onDone: () => void }) {
  const t = useTranslations("explorations");
  const signedIn = useIsAuthenticated();
  const [decision, setDecision] = useState<"accepted" | "rejected">("accepted");
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const reviewed = exploration.events.some((e) => e.event === "reviewed");
  if (exploration.state !== "reported") return null;
  if (!reviewed) return <p className="text-xs text-slate-500">{t("decision.awaitingReview")}</p>;
  if (!signedIn) return <p className="text-xs text-slate-500">{t("decision.signIn")}</p>;
  return (
    <form
      className="space-y-2 rounded border border-slate-200 p-3"
      onSubmit={(e) => {
        e.preventDefault();
        decideExploration(exploration.id, decision, text)
          .then(onDone)
          .catch((err) => setError(errorText(err)));
      }}
    >
      <p className="text-xs text-slate-600">{t("decision.meaning")}</p>
      <div className="flex gap-4 text-sm">
        {(["accepted", "rejected"] as const).map((value) => (
          <label key={value} className="flex items-center gap-1">
            <input type="radio" name="decision" checked={decision === value} onChange={() => setDecision(value)} />
            {t(`decision.${value}`)}
          </label>
        ))}
      </div>
      <textarea
        aria-label={t("decision.why")}
        className="w-full rounded border border-slate-300 p-2 text-sm"
        placeholder={t("decision.why")}
        value={text}
        onChange={(e) => setText(e.target.value)}
      />
      {error && <p className="text-xs text-red-700">{error}</p>}
      <button type="submit" disabled={!text.trim()} className="rounded bg-slate-800 px-3 py-1 text-sm text-white disabled:opacity-40">
        {t("decision.submit")}
      </button>
    </form>
  );
}

function Moves({ exploration, onDone }: { exploration: Exploration; onDone: () => void }) {
  const t = useTranslations("explorations");
  const mayMove = useMayLaunchJobs();
  const [error, setError] = useState("");
  const move = { draft: "launch", running: "report", reported: "review" }[exploration.state as string] as
    | "launch"
    | "report"
    | "review"
    | undefined;
  if (!move || exploration.archived_reason) return null;
  if (!mayMove) return <OperatorOnlyNotice />;
  return (
    <div className="space-y-1">
      <button
        type="button"
        className="rounded border border-slate-300 px-3 py-1 text-sm hover:bg-slate-50"
        onClick={() => moveExploration(exploration.id, move).then(onDone).catch((err) => setError(errorText(err)))}
      >
        {t(`moves.${move}`)}
      </button>
      {error && <p className="text-xs text-red-700">{error}</p>}
    </div>
  );
}

function Detail({ id }: { id: string }) {
  const t = useTranslations("explorations");
  const [exploration, setExploration] = useState<Exploration | null>(null);
  const [receipt, setReceipt] = useState<ExplorationReceipt | null>(null);
  const [error, setError] = useState("");
  const load = useCallback(() => {
    Promise.all([getExploration(id), getExplorationReceipt(id)])
      .then(([e, r]) => {
        setExploration(e);
        setReceipt(r);
      })
      .catch((err) => setError(errorText(err)));
  }, [id]);
  useEffect(load, [load]);
  if (error) return <p className="text-sm text-red-700">{error}</p>;
  if (!exploration || !receipt) return <p className="text-sm text-slate-500">{t("loading")}</p>;
  return (
    <div className="space-y-5 rounded-b-lg border-x border-b border-slate-200 bg-white p-4">
      <section className="space-y-1 text-sm">
        <p>
          <span className="font-medium">{t("fields.hypothesis")}:</span> {exploration.hypothesis}
        </p>
        <p className="text-xs text-slate-600">
          {t(`kinds.${exploration.kind}`)} · {t("fields.window")} <span className="font-mono">{exploration.evaluation_set_id.slice(0, 8)}</span> ·{" "}
          {t("fields.proposer")} {exploration.proposer}
          {exploration.launcher ? ` · ${t("fields.launcher")} ${exploration.launcher}` : ""}
          {exploration.reviewer ? ` · ${t("fields.reviewer")} ${exploration.reviewer}` : ""}
        </p>
        {exploration.post_hoc && <p className="text-xs text-amber-800">{t("postHoc")}</p>}
      </section>
      <section className="space-y-1">
        <h3 className="text-sm font-semibold">{t("blocks.rules")}</h3>
        {exploration.rules_sha256 && (
          <p className="break-all font-mono text-[11px] text-slate-500">
            sha256 {exploration.rules_sha256} · {formatDateTime(exploration.rules_frozen_at)} · {exploration.rules_frozen_by}
          </p>
        )}
        <pre className="max-h-48 overflow-auto rounded bg-slate-50 p-2 text-[11px]">{JSON.stringify(exploration.rules, null, 2)}</pre>
      </section>
      <ReceiptBlock receipt={receipt} />
      <ReportBlock event={latestReport(exploration.events)} />
      <section className="space-y-2">
        <h3 className="text-sm font-semibold">{t("blocks.review")}</h3>
        <ol className="space-y-1 text-xs">
          {exploration.events.map((e, i) => (
            <li key={i}>
              <span className="text-slate-500">{formatDateTime(e.at)}</span> · {e.actor} ({e.actor_type}) · <strong>{e.event}</strong>
            </li>
          ))}
        </ol>
        <Moves exploration={exploration} onDone={load} />
        <DecisionForm exploration={exploration} onDone={load} />
        {exploration.decision && (
          <p className="text-sm">
            {t(`decision.${exploration.decision}`)}: {exploration.decision_text}
          </p>
        )}
      </section>
      <section className="space-y-1">
        <h3 className="text-sm font-semibold">{t("blocks.links")}</h3>
        <ul className="space-y-0.5 text-xs">
          {exploration.links.map((link) => (
            <li key={link.id} className="font-mono">
              {link.role}
              {link.arm ? ` [${link.arm}]` : ""}: {Object.entries(link.target).map(([k, v]) => `${k} ${v.slice(0, 8)}`).join(", ")}
            </li>
          ))}
        </ul>
      </section>
    </div>
  );
}

/** Explorations grouped by the question they answer, by number within each. */
export function ExplorationsPanel() {
  const t = useTranslations("explorations");
  const [rows, setRows] = useState<ExplorationHeadline[] | null>(null);
  const [includeArchived, setIncludeArchived] = useState(false);
  const [open, setOpen] = useState<string | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    listExplorations(includeArchived)
      .then(setRows)
      .catch((err) => setError(errorText(err)));
  }, [includeArchived]);
  const groups = useMemo(() => {
    const byQuestion = new Map<string, ExplorationHeadline[]>();
    for (const row of rows ?? []) {
      byQuestion.set(row.question, [...(byQuestion.get(row.question) ?? []), row]);
    }
    return [...byQuestion.entries()];
  }, [rows]);
  if (error) return <p className="text-sm text-red-700">{error}</p>;
  if (!rows) return <p className="text-sm text-slate-500">{t("loading")}</p>;
  return (
    <div className="space-y-4">
      <p className="max-w-3xl text-sm text-slate-600">{t("intro")}</p>
      <label className="flex items-center gap-2 text-xs">
        <input type="checkbox" checked={includeArchived} onChange={(e) => setIncludeArchived(e.target.checked)} />
        {t("includeArchived")}
      </label>
      {!rows.length && <p className="text-sm text-slate-500">{t("empty")}</p>}
      {groups.map(([question, members]) => (
        <section key={question} className="space-y-1">
          <h2 className="text-sm font-semibold text-slate-700">{question}</h2>
          {members.map((row) => {
            const struck = row.archived_reason === "superseded" || row.archived_reason === "withdrawn";
            return (
              <div key={row.id}>
                <button
                  type="button"
                  onClick={() => setOpen(open === row.id ? null : row.id)}
                  className="flex w-full flex-wrap items-center gap-2 rounded-t-lg border border-slate-200 bg-white px-3 py-2 text-left hover:bg-slate-50"
                >
                  <span className={`text-sm font-medium ${struck ? "line-through" : ""}`}>
                    {t("number", { number: row.number })} · {row.title}
                  </span>
                  <Chip className={STATE_STYLE[row.state] ?? ""}>{t(`states.${row.state}`)}</Chip>
                  <Chip className="bg-slate-50 text-slate-600">{t(`kinds.${row.kind}`)}</Chip>
                  {row.verdict && <Chip className="bg-violet-100 text-violet-800">{row.verdict}</Chip>}
                  {row.grade && <Chip className="bg-slate-100 text-slate-700">{row.grade}</Chip>}
                  {row.post_hoc && <Chip className="bg-amber-50 text-amber-800">post hoc</Chip>}
                  {row.archived_reason && <Chip className="bg-slate-200 text-slate-600">{row.archived_reason}</Chip>}
                </button>
                {open === row.id && <Detail id={row.id} />}
              </div>
            );
          })}
        </section>
      ))}
    </div>
  );
}
