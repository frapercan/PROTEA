"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useTranslations } from "next-intl";
import {
  getStack,
  getStackPulls,
  StackRepo,
  StackPullRequest,
} from "@/lib/api";

const STATUS_STYLE: Record<string, string> = {
  active: "bg-green-100 text-green-800 ring-green-200",
  beta: "bg-blue-100 text-blue-800 ring-blue-200",
  skeleton: "bg-slate-100 text-slate-700 ring-slate-200",
  archived: "bg-rose-100 text-rose-800 ring-rose-200",
};

function StatusPill({ status }: { status: string }) {
  const cls = STATUS_STYLE[status] ?? STATUS_STYLE.skeleton;
  return (
    <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${cls}`}>
      {status}
    </span>
  );
}

function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  const now = Date.now();
  const sec = Math.round((now - then) / 1000);
  if (sec < 60) return `${sec}s ago`;
  if (sec < 3600) return `${Math.round(sec / 60)}m ago`;
  if (sec < 86400) return `${Math.round(sec / 3600)}h ago`;
  return `${Math.round(sec / 86400)}d ago`;
}

export default function StackPage() {
  const t = useTranslations("stack");
  const [repos, setRepos] = useState<StackRepo[]>([]);
  const [thesisPdfUrl, setThesisPdfUrl] = useState<string | null>(null);
  const [pulls, setPulls] = useState<StackPullRequest[]>([]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [pullsLoading, setPullsLoading] = useState(true);
  const [pullsFetchedAt, setPullsFetchedAt] = useState<number | null>(null);
  const [reposError, setReposError] = useState<string | null>(null);

  useEffect(() => {
    getStack()
      .then((d) => {
        setRepos(d.repos);
        setThesisPdfUrl(d.thesis_pdf_url);
      })
      .catch((e) => setReposError(e?.message ?? String(e)));
  }, []);

  const loadPulls = () => {
    setPullsLoading(true);
    getStackPulls()
      .then((d) => {
        setPulls(d.pulls);
        setErrors(d.errors);
        setPullsFetchedAt(d.fetched_at);
      })
      .catch((e) => setErrors({ _: e?.message ?? String(e) }))
      .finally(() => setPullsLoading(false));
  };

  useEffect(() => {
    loadPulls();
  }, []);

  return (
    <main className="mx-auto max-w-6xl px-4 py-8 space-y-10">
      <header>
        <h1 className="text-2xl font-bold tracking-tight text-slate-900">{t("title")}</h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-600">{t("subtitle")}</p>
      </header>

      {thesisPdfUrl && (
        <section className="rounded-lg border border-indigo-200 bg-indigo-50 p-4">
          <div className="flex items-center justify-between gap-4">
            <div>
              <h2 className="text-lg font-semibold text-indigo-900">{t("thesisHeading")}</h2>
              <p className="mt-1 text-sm text-indigo-800">{t("thesisDescription")}</p>
            </div>
            <a
              href={thesisPdfUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="rounded-md bg-indigo-600 px-3 py-1.5 text-xs font-semibold text-white shadow-sm hover:bg-indigo-700"
            >
              {t("thesisOpen")}
            </a>
          </div>
        </section>
      )}

      <section>
        <h2 className="text-lg font-semibold text-slate-900">{t("reposHeading")}</h2>
        {reposError && (
          <p className="mt-3 text-sm text-rose-600">{reposError}</p>
        )}
        <div className="mt-4 overflow-hidden rounded-lg border bg-white">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
              <tr>
                <th className="px-4 py-2.5">{t("colRepo")}</th>
                <th className="px-4 py-2.5">{t("colRole")}</th>
                <th className="px-4 py-2.5">{t("colStatus")}</th>
                <th className="px-4 py-2.5">{t("colSummary")}</th>
                <th className="px-4 py-2.5">{t("colLinks")}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {repos.map((r) => (
                <tr key={r.slug} className="hover:bg-slate-50">
                  <td className="px-4 py-3 align-top font-mono text-blue-700">
                    <a
                      href={r.github_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="hover:underline"
                    >
                      {r.name}
                    </a>
                  </td>
                  <td className="px-4 py-3 align-top text-slate-600">{r.role_label}</td>
                  <td className="px-4 py-3 align-top">
                    <StatusPill status={r.status} />
                  </td>
                  <td className="px-4 py-3 align-top text-slate-700">{r.summary}</td>
                  <td className="px-4 py-3 align-top text-xs space-x-3 whitespace-nowrap">
                    <a href={r.github_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                      GitHub
                    </a>
                    <a href={`${r.github_url}/pulls`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                      PRs
                    </a>
                    <a href={`${r.github_url}/issues`} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                      Issues
                    </a>
                    {r.local_docs_path ? (
                      <a href={r.local_docs_path} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                        {t("linkDocsLocal")}
                      </a>
                    ) : r.docs_url ? (
                      <a href={r.docs_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:underline">
                        {t("linkDocs")}
                      </a>
                    ) : null}
                  </td>
                </tr>
              ))}
              {repos.length === 0 && !reposError && (
                <tr>
                  <td colSpan={5} className="px-4 py-6 text-center text-slate-400">
                    {t("loading")}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      <section>
        <div className="flex items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">{t("pullsHeading")}</h2>
            {pullsFetchedAt && (
              <p className="mt-1 text-xs text-slate-500">
                {t("fetchedAt", { time: new Date(pullsFetchedAt * 1000).toLocaleString() })}
              </p>
            )}
          </div>
          <button
            onClick={loadPulls}
            disabled={pullsLoading}
            className="rounded-md border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 shadow-sm hover:bg-slate-50 disabled:opacity-50"
          >
            {pullsLoading ? t("refreshing") : t("refresh")}
          </button>
        </div>

        {Object.keys(errors).length > 0 && (
          <div className="mt-3 rounded-md border border-amber-200 bg-amber-50 p-3 text-xs text-amber-800">
            <div className="font-semibold">{t("errorsHeading")}</div>
            <ul className="mt-1 space-y-1">
              {Object.entries(errors).map(([repo, err]) => (
                <li key={repo}>
                  <span className="font-mono">{repo}</span>: {err}
                </li>
              ))}
            </ul>
          </div>
        )}

        <div className="mt-4 overflow-hidden rounded-lg border bg-white">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
              <tr>
                <th className="px-4 py-2.5">{t("prRepo")}</th>
                <th className="px-4 py-2.5">{t("prTitle")}</th>
                <th className="px-4 py-2.5">{t("prAuthor")}</th>
                <th className="px-4 py-2.5">{t("prUpdated")}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {pulls.map((p) => (
                <tr key={`${p.repo}#${p.number}`} className="hover:bg-slate-50">
                  <td className="px-4 py-3 align-top text-xs font-mono text-slate-600">{p.repo}</td>
                  <td className="px-4 py-3 align-top">
                    <a
                      href={p.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-blue-700 hover:underline"
                    >
                      #{p.number} {p.title}
                    </a>
                    {p.draft && (
                      <span className="ml-2 inline-flex items-center rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600 ring-1 ring-slate-200">
                        draft
                      </span>
                    )}
                    {p.labels.map((lbl) => (
                      <span
                        key={lbl}
                        className="ml-1 inline-flex items-center rounded-full bg-blue-50 px-2 py-0.5 text-xs text-blue-700 ring-1 ring-blue-200"
                      >
                        {lbl}
                      </span>
                    ))}
                  </td>
                  <td className="px-4 py-3 align-top text-xs text-slate-600">{p.author ?? "—"}</td>
                  <td className="px-4 py-3 align-top text-xs text-slate-500" title={p.updated_at}>
                    {relativeTime(p.updated_at)}
                  </td>
                </tr>
              ))}
              {pulls.length === 0 && !pullsLoading && (
                <tr>
                  <td colSpan={4} className="px-4 py-8 text-center text-slate-400">
                    {t("noOpenPulls")}
                  </td>
                </tr>
              )}
              {pullsLoading && pulls.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-8 text-center text-slate-400">
                    {t("loadingPulls")}
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </main>
  );
}
