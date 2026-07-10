import { getTranslations } from "next-intl/server";
import { getFeatureRegistry, type FeatureDocInfo, type FeatureStatus } from "@/lib/api";

/**
 * Feature registry — the third renderer of the explainability registry.
 *
 * The reranker feature schema is documented ONCE, in
 * ``protea_contracts.feature_docs.FEATURE_DOCS``. The Sphinx docs render it
 * and the thesis cites it; this page is the third surface, fed by
 * ``GET /features/registry`` which serializes that same contract. The UI
 * keeps no copy of its own: a contract bump reaches here on the next fetch.
 *
 * This is a server component: it awaits the fetch and, on failure, lets the
 * error propagate to the route-level ``error.tsx`` boundary (which reads the
 * typed ``ApiError`` and teaches the operator what happened) rather than
 * swallowing it into an empty table.
 *
 * The page TEACHES, it does not hide the machinery. Every feature carries its
 * production status as a badge (produced / declared-absent / pool-injected /
 * broken) and its caveats verbatim, so the technician can learn in situ that
 * ``interpro_*`` carries no signal while its table is unpopulated, or that the
 * ``classifier_*`` / ``association_*`` columns are left unfilled by the
 * default export (ADR-D45), or that ``plm_id`` / ``k_context`` are stamped by
 * the lab's pooled loader and never appear in a raw parquet dump.
 */

// Per-status presentation. The four keys match the FeatureStatus enum shipped
// by the contract; the copy is resolved from the message catalogue so every
// locale gets a translated badge label and legend line.
const STATUS_STYLE: Record<FeatureStatus, string> = {
  PRODUCED: "bg-emerald-100 text-emerald-800 ring-emerald-200",
  DECLARED_ABSENT: "bg-amber-100 text-amber-800 ring-amber-200",
  POOL_INJECTED: "bg-indigo-100 text-indigo-800 ring-indigo-200",
  BROKEN: "bg-rose-100 text-rose-800 ring-rose-200",
};

const STATUS_ORDER: FeatureStatus[] = [
  "PRODUCED",
  "DECLARED_ABSENT",
  "POOL_INJECTED",
  "BROKEN",
];

function groupByFamily(features: FeatureDocInfo[]): [string, FeatureDocInfo[]][] {
  const map = new Map<string, FeatureDocInfo[]>();
  for (const f of features) {
    const bucket = map.get(f.family);
    if (bucket) bucket.push(f);
    else map.set(f.family, [f]);
  }
  return Array.from(map.entries());
}

export default async function FeatureRegistryPage() {
  const t = await getTranslations("featureRegistry");

  // Awaited server-side: a failed fetch throws an ApiError that bubbles to
  // the route error boundary instead of resolving to an empty page.
  const registry = await getFeatureRegistry();
  const groups = groupByFamily(registry.features);

  const statusLabel = (s: FeatureStatus) => t(`status.${s}.label`);

  return (
    <main className="mx-auto max-w-5xl px-4 py-8 space-y-10">
      <header className="space-y-3">
        <h1 className="text-2xl font-bold tracking-tight text-slate-900">{t("title")}</h1>
        <p className="max-w-3xl text-sm leading-relaxed text-slate-600">{t("subtitle")}</p>
        <p className="max-w-3xl text-xs leading-relaxed text-slate-500">
          {t("provenance", {
            version: registry.schema_version,
            total: registry.total,
            families: registry.families.length,
          })}
        </p>
      </header>

      {/* Status legend — teaches what each badge means before the operator
          meets it in a row. */}
      <section aria-labelledby="legend-heading" className="rounded-xl border border-slate-200 bg-slate-50 p-4">
        <h2 id="legend-heading" className="text-sm font-semibold text-slate-800">
          {t("legendHeading")}
        </h2>
        <dl className="mt-3 grid gap-3 sm:grid-cols-2">
          {STATUS_ORDER.map((s) => {
            const count = registry.status_counts[s] ?? 0;
            return (
              <div key={s} className="flex items-start gap-3">
                <span
                  className={`mt-0.5 inline-flex shrink-0 items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${STATUS_STYLE[s]}`}
                >
                  {statusLabel(s)}
                </span>
                <div className="min-w-0">
                  <dt className="sr-only">{statusLabel(s)}</dt>
                  <dd className="text-xs leading-relaxed text-slate-600">
                    {t(`status.${s}.help`)}{" "}
                    <span className="text-slate-400">{t("countSuffix", { count })}</span>
                  </dd>
                </div>
              </div>
            );
          })}
        </dl>
      </section>

      {/* One section per family. */}
      <div className="space-y-8">
        {groups.map(([family, feats]) => (
          <section key={family} aria-labelledby={`family-${family}`} className="space-y-3">
            <div className="flex items-baseline justify-between gap-3 border-b border-slate-200 pb-1.5">
              <h2 id={`family-${family}`} className="font-mono text-base font-semibold text-slate-900">
                {family}
              </h2>
              <span className="text-xs text-slate-500">{t("familyCount", { count: feats.length })}</span>
            </div>

            <ul className="space-y-3">
              {feats.map((f) => (
                <li key={f.name} className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
                  <div className="flex flex-wrap items-center gap-2">
                    <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-sm text-slate-800">
                      {f.name}
                    </code>
                    <span
                      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${STATUS_STYLE[f.status]}`}
                    >
                      {statusLabel(f.status)}
                    </span>
                  </div>

                  <p className="mt-2 text-sm text-slate-700">{f.summary}</p>
                  <p className="mt-1 text-xs leading-relaxed text-slate-500">{f.definition}</p>

                  <dl className="mt-3 grid gap-x-6 gap-y-1.5 text-xs sm:grid-cols-2">
                    <div className="flex gap-2">
                      <dt className="shrink-0 font-medium text-slate-500">{t("producerLabel")}</dt>
                      <dd className="min-w-0 break-words text-slate-700">{f.producer}</dd>
                    </div>
                    <div className="flex gap-2">
                      <dt className="shrink-0 font-medium text-slate-500">{t("rangeLabel")}</dt>
                      <dd className="min-w-0 break-words font-mono text-slate-700">
                        {f.value_range ?? t("notApplicable")}
                        {f.unit ? ` (${f.unit})` : ""}
                      </dd>
                    </div>
                  </dl>

                  {f.notes ? (
                    <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 p-3">
                      <p className="text-xs font-semibold text-amber-800">{t("caveatLabel")}</p>
                      <p className="mt-0.5 whitespace-pre-line text-xs leading-relaxed text-amber-900">
                        {f.notes}
                      </p>
                    </div>
                  ) : null}
                </li>
              ))}
            </ul>
          </section>
        ))}
      </div>
    </main>
  );
}
