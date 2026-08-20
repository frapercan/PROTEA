// What this prediction was measured with.
//
// The complaint that produced this: "me molesta que no sé qué es lo que se
// está evaluando". A reader looking at a score could see the model as a
// UUID and could not see the search backend, the distance metric, or which
// evidence a donor needed before it was allowed to vote. Two rows a few
// points apart might differ in any of those and nothing on the surface
// said which.
//
// Everything here is read from the prediction set's own receipt. Nothing
// is defaulted: a field the record does not carry is shown as not
// recorded, because a set written before the receipt existed must not
// appear to have used values it never used.

"use client";

import { useEffect, useState } from "react";

type Receipt = {
  prediction_set_id: string;
  has_receipt: boolean;
  model: { display_name: string; family: string | null; pooling: string | null; max_length: number | null };
  search: {
    neighbours: number;
    metric: string | null;
    backend: string | null;
    distance_threshold: number | null;
    aspect_separated: boolean | null;
  };
  donors: {
    bank_release: string | null;
    bank_published_at: string | null;
    regime: string;
    evidence_codes: string[] | null;
  };
  campaign: { rung: string | null; window: string | null; scorer: string | null } | null;
  run: {
    /** Null when no job is linked: unattributed, which is not unfinished. */
    finished: boolean | null;
    status: string | null;
    batches_done: number | null;
    batches_total: number | null;
  };
  job_id: string | null;
};

/** A value the record does not carry, drawn as absent rather than blank. */
function Field({ label, value }: { label: string; value: React.ReactNode }) {
  const missing = value === null || value === undefined || value === "";
  return (
    <div className="min-w-[7rem]">
      <dt className="text-[10px] uppercase tracking-wider text-slate-400">{label}</dt>
      <dd className={missing ? "text-[12px] italic text-slate-400" : "text-[12px] text-slate-800"}>
        {missing ? "not recorded" : value}
      </dd>
    </div>
  );
}

export function RunReceipt({ predictionSetId }: { predictionSetId: string }) {
  const [r, setR] = useState<Receipt | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setR(null);
    setError(null);
    fetch(`/api-proxy/receipts/prediction-set/${predictionSetId}`, { cache: "no-store" })
      .then((res) => (res.ok ? res.json() : Promise.reject(new Error(String(res.status)))))
      .then(setR)
      .catch((e) => setError(String(e)));
  }, [predictionSetId]);

  if (error) return <p className="text-[11px] text-rose-600">receipt: {error}</p>;
  if (!r) return <p className="text-[11px] text-slate-400">loading run…</p>;

  return (
    <section className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
      <header className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-[10px] font-semibold uppercase tracking-wider text-slate-400">
          what this was measured with
        </h3>
        {r.campaign?.rung ? (
          <span className="text-[10px] text-slate-500">
            rung {r.campaign.rung}
            {r.campaign.window ? ` · GOA ${r.campaign.window.replace("-", " → ")}` : ""}
            {r.campaign.scorer ? ` · ${r.campaign.scorer}` : ""}
          </span>
        ) : null}
      </header>

      {r.run.finished === false ? (
        // Loudest thing on the panel, and first. A cancelled run leaves
        // its written batches behind and the prediction set carries no
        // mark saying so, which is how a partial run gets read as a
        // finished one: every other field below looks completely normal.
        <p className="mt-1.5 rounded border border-rose-300 bg-rose-50 px-2 py-1 text-[11px] leading-snug text-rose-800">
          <span className="font-semibold">This run did not finish.</span> The
          job ended {r.run.status?.toLowerCase() ?? "incomplete"} after{" "}
          {r.run.batches_done ?? 0} of {r.run.batches_total ?? "?"} batches, so
          these predictions cover part of the query set. Everything below
          describes what the run was configured to do, not what it did.
        </p>
      ) : null}

      {!r.has_receipt ? (
        // Said plainly rather than filled in. This set predates the
        // receipt, and showing defaults would claim it used them.
        <p className="mt-1 text-[11px] italic text-slate-500">
          This run recorded no parameters. It was produced before prediction
          sets carried a receipt, so the search backend, the distance metric
          and the donor policy are unknown rather than default.
        </p>
      ) : null}

      <dl className="mt-1.5 flex flex-wrap gap-x-6 gap-y-2">
        <Field label="model" value={r.model.display_name} />
        <Field label="neighbours" value={`K = ${r.search.neighbours}`} />
        <Field label="distance" value={r.search.metric} />
        <Field label="search" value={r.search.backend} />
        <Field
          label="donor bank"
          value={
            r.donors.bank_release
              ? `GOA ${r.donors.bank_release}${
                  r.donors.bank_published_at ? ` (${r.donors.bank_published_at})` : ""
                }`
              : null
          }
        />
        <Field
          label="threshold"
          value={r.search.distance_threshold ?? (r.has_receipt ? "none" : null)}
        />
      </dl>

      <p className="mt-2 text-[11px] leading-snug text-slate-600">
        <span className="text-slate-400">donors: </span>
        {r.donors.regime}
        {r.donors.evidence_codes ? (
          <span className="text-slate-400">
            {" "}
            ({r.donors.evidence_codes.length} evidence codes)
          </span>
        ) : null}
      </p>
    </section>
  );
}
