import type { Metadata } from "next";
import Link from "next/link";
import { getTranslations } from "next-intl/server";
import {
  getFeatureRegistry,
  getPredictionSet,
  getProteinPredictions,
  listPredictionSets,
  listRerankers,
  type FeatureDocInfo,
  type Prediction,
  type PredictionSet,
  type RerankerModel,
} from "@/lib/api";

/**
 * How a score is made. The reader's on-ramp: one protein, one GO term, walked
 * from retrieval to a calibrated probability, in the book's scholarly register.
 *
 * This is chapter zero of the interface-as-a-book. It is prose-with-evidence,
 * not a dashboard widget: the audience is the technician who deploys and
 * operates PROTEA and wants the machinery in the open, not hidden. The steps
 * are numbered because, uniquely on this page, the numbering is earned: the
 * steps are a real pipeline order (retrieval, candidate, evidence, fusion,
 * probability).
 *
 * Every value on the page traces to a real API response field:
 *   1. The retrieval  -> GET /embeddings/prediction-sets/{set}/proteins/{acc}
 *                        (rows filtered to this GO term: the neighbours that
 *                        voted, their distance / identity / k-position / votes).
 *   2. The evidence   -> the typed feature columns on that same prediction row,
 *                        with each column's MEANING pulled from the one
 *                        explainability registry, GET /features/registry.
 *   3. The fusion     -> the reranker registry, GET /scoring/rerankers. The
 *                        single-pair fused probability is NOT exposed by a JSON
 *                        endpoint (only in bulk as .../rerank.tsv), so this
 *                        section names the contract and renders the score as a
 *                        clearly-labelled pending state rather than inventing a
 *                        number. That backend gap is stated on the page.
 *   4. The caveats    -> ours, stated first, in frontier rose.
 *
 * Server component, matching the pillar pages. Every fetch is wrapped so a
 * missing run or an unreachable backend renders an honest empty/pending state
 * in place, because this page is keyed on one arbitrary (accession, GO) pair
 * that a given prediction set may simply not contain.
 */

type Params = { locale: string; accession: string; go: string };
type Search = { set?: string };

export async function generateMetadata({ params }: { params: Promise<Params> }): Promise<Metadata> {
  const { accession, go } = await params;
  return { title: `${decodeURIComponent(accession)} × ${decodeURIComponent(go)} · how a score is made` };
}

/** Resolve a promise to its value, or to null on any thrown ApiError. */
async function safe<T>(p: Promise<T>): Promise<T | null> {
  try {
    return await p;
  } catch {
    return null;
  }
}

// The typed feature columns carried on a prediction row, in the order we teach
// them. These are the columns present on the wire shape (GOPrediction); the
// export-time families (anc2vec_*, lineage_*, interpro_*, emb_pca_*) are not on
// this endpoint, so we do not pretend they are. Each key is looked up by name
// in the feature registry for its meaning.
const FEATURE_KEYS: { key: keyof Prediction; fallbackLabel: string }[] = [
  { key: "go_term_frequency", fallbackLabel: "go_term_frequency" },
  { key: "k_position", fallbackLabel: "k_position" },
  { key: "vote_count", fallbackLabel: "vote_count" },
  { key: "identity_nw", fallbackLabel: "identity_nw" },
  { key: "similarity_nw", fallbackLabel: "similarity_nw" },
  { key: "neighbor_distance_std", fallbackLabel: "neighbor_distance_std" },
  { key: "ref_annotation_density", fallbackLabel: "ref_annotation_density" },
  { key: "taxonomic_distance", fallbackLabel: "taxonomic_distance" },
  { key: "taxonomic_common_ancestors", fallbackLabel: "taxonomic_common_ancestors" },
  { key: "taxonomic_relation", fallbackLabel: "taxonomic_relation" },
];

function fmt(v: number | string | null | undefined): string {
  if (v === null || v === undefined) return "—";
  if (typeof v === "string") return v;
  if (Number.isInteger(v)) return String(v);
  // Keep four significant decimals, trimming trailing zeros.
  return Number(v.toFixed(4)).toString();
}

export default async function ScorePage({
  params,
  searchParams,
}: {
  params: Promise<Params>;
  searchParams: Promise<Search>;
}) {
  const { locale, accession: rawAccession, go: rawGo } = await params;
  const { set: pinnedSet } = await searchParams;
  const t = await getTranslations("score");

  const accession = decodeURIComponent(rawAccession);
  const go = decodeURIComponent(rawGo);

  // ── Resolve a prediction set: pinned via ?set, else the most recent run. ──
  const sets = (await safe(listPredictionSets())) ?? [];
  const setList: PredictionSet[] = Array.isArray(sets) ? sets : [];
  const setId = pinnedSet ?? setList[0]?.id ?? null;
  const setMeta = setId
    ? setList.find((s) => s.id === setId) ?? (await safe(getPredictionSet(setId)))
    : null;

  // ── The retrieval: rows for this protein, filtered to this GO term. ──
  const allPreds = setId ? (await safe(getProteinPredictions(setId, accession))) ?? [] : [];
  const predsForPair: Prediction[] = (Array.isArray(allPreds) ? allPreds : []).filter(
    (p) => p.go_id === go,
  );
  const nearest = predsForPair[0] ?? null;

  // ── The feature registry: the one source for what each column means. ──
  const registry = await safe(getFeatureRegistry());
  const registryFeatures: FeatureDocInfo[] =
    registry && Array.isArray(registry.features) ? registry.features : [];
  const featureDocs = new Map(registryFeatures.map((f) => [f.name, f]));
  const registryOk = registryFeatures.length > 0;

  // ── The fusion: rerankers registered for inference (which booster applies). ──
  const rerankersRaw = await safe(listRerankers());
  const rerankers: RerankerModel[] = Array.isArray(rerankersRaw) ? rerankersRaw : [];

  const caveats = t.raw("caveats.items") as string[];

  const H2 = "protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--muted)]";
  const HAIR = "border-t border-[var(--border)]";

  return (
    <article className="mx-auto max-w-3xl px-1 pb-16">
      {/* Return to the argument. */}
      <Link
        href={`/${locale}`}
        className="protea-eyebrow inline-flex items-center gap-1.5 text-[12px] uppercase tracking-wide text-[var(--muted)] hover:text-[var(--primary)]"
      >
        <span aria-hidden>←</span>
        {t("back")}
      </Link>

      <header className="mt-8">
        <p className="protea-eyebrow text-[12px] uppercase tracking-wide text-[var(--primary)]">
          {t("eyebrow")}
        </p>
        <h1 className="mt-3 font-serif text-[2rem] font-normal leading-tight tracking-tight text-[var(--foreground)] sm:text-[2.4rem]">
          {t("title")}
        </h1>

        {/* The pair, and the run it is read from. */}
        <dl className="mt-6 grid grid-cols-[7rem_1fr] gap-x-4 gap-y-2 text-[14px]">
          <dt className="protea-eyebrow text-[11px] uppercase text-[var(--muted)]">{t("proteinLabel")}</dt>
          <dd className="min-w-0">
            <Link
              href={`/${locale}/proteins/${encodeURIComponent(accession)}`}
              className="font-mono text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
            >
              {accession}
            </Link>
          </dd>
          <dt className="protea-eyebrow text-[11px] uppercase text-[var(--muted)]">{t("goLabel")}</dt>
          <dd className="min-w-0 font-mono text-[var(--foreground)]">{go}</dd>
          <dt className="protea-eyebrow text-[11px] uppercase text-[var(--muted)]">{t("setLabel")}</dt>
          <dd className="min-w-0">
            {setId ? (
              <>
                <span className="font-mono text-[13px] text-[var(--foreground)]">{setId}</span>
                {setMeta?.embedding_config_name && (
                  <span className="ml-2 text-[13px] text-[var(--muted)]">{setMeta.embedding_config_name}</span>
                )}
                <span className="mt-0.5 block text-[12px] text-[var(--subtle)]">
                  {pinnedSet ? t("setPinned") : t("setResolvedFromLatest")}
                </span>
              </>
            ) : (
              <span className="text-[13px] text-[var(--muted)]">{t("noSet.heading")}</span>
            )}
          </dd>
        </dl>
      </header>

      {/* Grounding: the pipeline order + the measured fact from chapter one. */}
      <p className="mt-8 font-serif text-[17px] leading-[1.72] text-[var(--foreground)]">
        {t("grounding")}{" "}
        <Link
          href={`/${locale}/pillar/1`}
          className="text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
        >
          {t("groundingLink")}
        </Link>
      </p>

      {/* When no run resolves, the whole walk has no data. Say so plainly. */}
      {!setId && (
        <section className={`mt-10 ${HAIR} pt-8`}>
          <div className="rounded-r-md border-l-2 border-[var(--danger)] bg-[var(--primary-soft)] px-4 py-3">
            <p className="font-serif text-[16px] text-[var(--foreground)]">{t("noSet.heading")}</p>
            <p className="mt-1.5 text-[14px] leading-relaxed text-[var(--muted)]">{t("noSet.body")}</p>
          </div>
        </section>
      )}

      {/* ── 1. The retrieval ── */}
      <section className={`mt-12 ${HAIR} pt-8`}>
        <h2 className={H2}>{t("retrieval.heading")}</h2>
        <p className="mt-3 font-serif text-[17px] leading-[1.72] text-[var(--foreground)]">
          {t("retrieval.lede", { accession, go })}
        </p>

        {predsForPair.length > 0 ? (
          <figure className="m-0 mt-6">
            <div className="overflow-x-auto protea-scroll-shadow">
              <table className="w-full border-collapse text-left">
                <thead>
                  <tr className="border-b border-[var(--border-strong)] text-[var(--muted)]">
                    <th scope="col" className="py-2 pr-4 text-[13px] font-semibold">
                      {t("retrieval.thNeighbour")}
                    </th>
                    <th scope="col" className="py-2 pl-4 text-right font-mono text-[13px] font-semibold">
                      {t("retrieval.thDistance")}
                    </th>
                    <th scope="col" className="py-2 pl-4 text-right font-mono text-[13px] font-semibold">
                      {t("retrieval.thIdentity")}
                    </th>
                    <th scope="col" className="py-2 pl-4 text-right font-mono text-[13px] font-semibold">
                      {t("retrieval.thKPosition")}
                    </th>
                    <th scope="col" className="py-2 pl-4 text-right font-mono text-[13px] font-semibold">
                      {t("retrieval.thVoteCount")}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {predsForPair.map((p, i) => (
                    <tr key={`${p.ref_protein_accession}-${i}`} className="border-b border-[var(--border)]">
                      <th scope="row" className="py-2.5 pr-4 text-left font-mono text-[14px] font-normal">
                        <Link
                          href={`/${locale}/proteins/${encodeURIComponent(p.ref_protein_accession)}`}
                          className="text-[var(--primary)] underline decoration-[var(--border)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
                        >
                          {p.ref_protein_accession}
                        </Link>
                      </th>
                      <td className="py-2.5 pl-4 text-right font-mono tabular-nums text-[15px] text-[var(--foreground)]">
                        {fmt(p.distance)}
                      </td>
                      <td className="py-2.5 pl-4 text-right font-mono tabular-nums text-[15px] text-[var(--foreground)]">
                        {fmt(p.identity_nw)}
                      </td>
                      <td className="py-2.5 pl-4 text-right font-mono tabular-nums text-[15px] text-[var(--foreground)]">
                        {fmt(p.k_position)}
                      </td>
                      <td className="py-2.5 pl-4 text-right font-mono tabular-nums text-[15px] text-[var(--foreground)]">
                        {fmt(p.vote_count)}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <figcaption className="mt-2.5 protea-eyebrow text-[11px] uppercase text-[var(--subtle)]">
              {t("retrieval.caption", { set: setId ?? "—" })}
            </figcaption>
          </figure>
        ) : (
          setId && (
            <div className="mt-6 rounded-r-md border-l-2 border-[var(--danger)] bg-[var(--primary-soft)] px-4 py-3">
              <p className="font-serif text-[16px] text-[var(--foreground)]">{t("retrieval.empty.heading")}</p>
              <p className="mt-1.5 text-[14px] leading-relaxed text-[var(--muted)]">
                {t("retrieval.empty.body", { accession, go })}
              </p>
            </div>
          )
        )}
      </section>

      {/* ── 2. The evidence ── */}
      <section className={`mt-12 ${HAIR} pt-8`}>
        <h2 className={H2}>{t("evidence.heading")}</h2>
        <p className="mt-3 font-serif text-[17px] leading-[1.72] text-[var(--foreground)]">
          {t("evidence.lede")}
        </p>

        {!registryOk && (
          <p className="mt-4 border-l-2 border-[var(--danger)] pl-3 text-[13px] leading-relaxed text-[var(--danger)]">
            {t("evidence.registryPending")}
          </p>
        )}

        {nearest ? (
          <>
            <figure className="m-0 mt-6">
              <div className="overflow-x-auto protea-scroll-shadow">
                <table className="w-full border-collapse text-left">
                  <thead>
                    <tr className="border-b border-[var(--border-strong)] text-[var(--muted)]">
                      <th scope="col" className="py-2 pr-4 text-[13px] font-semibold">
                        {t("evidence.thFeature")}
                      </th>
                      <th scope="col" className="py-2 pl-4 text-right font-mono text-[13px] font-semibold">
                        {t("evidence.thValue")}
                      </th>
                      <th scope="col" className="py-2 pl-4 text-[13px] font-semibold">
                        {t("evidence.thMeaning")}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {FEATURE_KEYS.map(({ key, fallbackLabel }) => {
                      const value = nearest[key] as number | string | null;
                      if (value === null || value === undefined) return null;
                      const doc = featureDocs.get(key as string);
                      const isBaseRate = key === "go_term_frequency";
                      return (
                        <tr key={key as string} className="border-b border-[var(--border)] align-top">
                          <th
                            scope="row"
                            className={`py-2.5 pr-4 text-left font-mono text-[14px] font-normal ${
                              isBaseRate ? "text-[var(--danger)]" : "text-[var(--foreground)]"
                            }`}
                          >
                            {doc?.name ?? fallbackLabel}
                          </th>
                          <td
                            className={`py-2.5 pl-4 text-right font-mono tabular-nums text-[15px] ${
                              isBaseRate ? "text-[var(--danger)]" : "text-[var(--foreground)]"
                            }`}
                          >
                            {fmt(value)}
                          </td>
                          <td className="py-2.5 pl-4 text-[13px] leading-relaxed text-[var(--muted)]">
                            {doc ? doc.summary : t("evidence.meaningPending")}
                            {isBaseRate && (
                              <span className="mt-1 block text-[12px] text-[var(--danger)]">
                                {t("evidence.baseRateNote")}
                              </span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <figcaption className="mt-2.5 protea-eyebrow text-[11px] uppercase text-[var(--subtle)]">
                {t("evidence.caption", { neighbour: nearest.ref_protein_accession })}
              </figcaption>
            </figure>
            <Link
              href={`/${locale}/feature-registry`}
              className="mt-4 inline-block text-[14px] text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
            >
              {t("evidence.registryLink")}
            </Link>
          </>
        ) : (
          setId && (
            <p className="mt-6 text-[14px] italic text-[var(--muted)]">{t("evidence.noCandidate")}</p>
          )
        )}
      </section>

      {/* ── 3. The fusion and the score ── */}
      <section className={`mt-12 ${HAIR} pt-8`}>
        <h2 className={H2}>{t("fusion.heading")}</h2>
        <p className="mt-3 font-serif text-[17px] leading-[1.72] text-[var(--foreground)]">
          {t("fusion.lede")}
        </p>

        {/* The single-pair fused probability is a real backend gap. Name the
            contract, render pending, do not invent a number. */}
        <div className="mt-6 rounded-r-md border-l-2 border-[var(--danger)] bg-[var(--primary-soft)] px-4 py-3">
          <p className="font-serif text-[16px] text-[var(--foreground)]">{t("fusion.gap.heading")}</p>
          <p className="mt-1.5 text-[14px] leading-relaxed text-[var(--muted)]">
            {t("fusion.gap.body", { set: setId ?? "<set>" })}
          </p>
          <div className="mt-3 space-y-1 font-mono text-[12px] text-[var(--foreground)]">
            <code className="block break-all">GET /scoring/prediction-sets/{setId ?? "<set>"}/rerank.tsv</code>
            <code className="block break-all">GET /scoring/prediction-sets/{setId ?? "<set>"}/score.tsv</code>
          </div>
        </div>

        <h3 className="mt-8 protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--muted)]">
          {t("fusion.rerankersHeading")}
        </h3>
        {rerankers.length > 0 ? (
          <figure className="m-0 mt-4">
            <div className="overflow-x-auto protea-scroll-shadow">
              <table className="w-full border-collapse text-left">
                <thead>
                  <tr className="border-b border-[var(--border-strong)] text-[var(--muted)]">
                    <th scope="col" className="py-2 pr-4 text-[13px] font-semibold">
                      {t("fusion.thReranker")}
                    </th>
                    <th scope="col" className="py-2 pl-4 text-[13px] font-semibold">
                      {t("fusion.thCategory")}
                    </th>
                    <th scope="col" className="py-2 pl-4 text-[13px] font-semibold">
                      {t("fusion.thAspect")}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {rerankers.map((r) => (
                    <tr key={r.id} className="border-b border-[var(--border)]">
                      <th scope="row" className="py-2.5 pr-4 text-left font-mono text-[14px] font-normal text-[var(--foreground)]">
                        {r.name}
                      </th>
                      <td className="py-2.5 pl-4 font-mono text-[13px] text-[var(--muted)]">{r.category}</td>
                      <td className="py-2.5 pl-4 font-mono text-[13px] text-[var(--muted)]">{r.aspect ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </figure>
        ) : (
          <p className="mt-4 text-[14px] italic text-[var(--muted)]">{t("fusion.rerankersEmpty")}</p>
        )}

        <Link
          href={`/${locale}/scoring`}
          className="mt-6 inline-block text-[14px] text-[var(--primary)] underline decoration-[var(--border-strong)] decoration-1 underline-offset-2 hover:decoration-[var(--primary)]"
        >
          {t("fusion.openInstrument")}
        </Link>
      </section>

      {/* ── 4. The caveats ── */}
      <section className={`mt-12 ${HAIR} pt-8`}>
        <h2 className="protea-eyebrow text-[11px] uppercase tracking-wide text-[var(--danger)]">
          {t("caveats.heading")}
        </h2>
        <p className="mt-3 text-[13px] italic text-[var(--muted)]">{t("caveats.intro")}</p>
        <ul className="mt-4 space-y-3">
          {caveats.map((c, i) => (
            <li key={i} className="grid grid-cols-[1rem_1fr] gap-x-3">
              <span aria-hidden className="select-none font-mono text-[var(--danger)]">
                ·
              </span>
              <span className="text-[15px] leading-relaxed text-[var(--foreground)]">{c}</span>
            </li>
          ))}
        </ul>
      </section>
    </article>
  );
}
