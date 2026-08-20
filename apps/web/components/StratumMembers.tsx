// The proteins behind one cell of the strata table.
//
// The panel says a cell holds 448 proteins and scores 0.257, and the trail
// ends at the count. This is the step from a published number to the things
// it is about: which proteins, how far the nearest donor is, and whether that
// donor carries experimental evidence.
//
// Weakest identity first. The reason to open a cell is usually to see what
// makes it hard, and the hardest cases are the ones worth showing.

"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { baseUrl } from "@/lib/api";

type Member = {
  accession: string;
  residues: number;
  length_band: string;
  homology_band: string;
  best_identity: number | null;
  donor_is_experimental: boolean | null;
  taxonomic_relation: string | null;
};

type Payload = {
  band_population: number;
  returned: number;
  truncated: boolean;
  members: Member[];
};

type Props = {
  predictionSetId: string;
  /** Asserted, not verified: see the endpoint's own note on why. */
  category: string;
  /** Either spelling. The panel emits "P", the benchmark board "BPO". */
  aspect: string;
  length?: string | null;
  homology?: string | null;
  locale: string;
};

const LIMIT = 25;

export function StratumMembers({
  predictionSetId,
  category,
  aspect,
  length,
  homology,
  locale,
}: Props) {
  const [data, setData] = useState<Payload | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    const q = new URLSearchParams({ category, aspect, limit: String(LIMIT) });
    if (length) q.set("length", length);
    if (homology) q.set("homology", homology);
    // No reset here: the caller keys this component on the cell, so opening
    // a different one remounts it and the state starts empty. Clearing it
    // inside the effect would be a second, later source of truth for the
    // same thing.
    // Trailing slash before the query: the app redirects to add one, and a
    // 308 on every open is a round trip for nothing.
    fetch(`${baseUrl()}/stratum/${predictionSetId}/members/?${q}`, {
      cache: "no-store",
    })
      .then(async (r) => {
        if (r.ok) return r.json();
        // The endpoint refuses an unknown aspect with a 422 that names the
        // vocabulary. Showing that beats "422".
        const body = await r.json().catch(() => null);
        throw new Error(body?.detail ?? `HTTP ${r.status}`);
      })
      .then((d) => live && setData(d))
      .catch((e) => live && setError(String(e.message ?? e)));
    return () => {
      live = false;
    };
  }, [predictionSetId, category, aspect, length, homology]);

  if (error) {
    return (
      <p className="px-2 py-1.5 text-[11px] text-rose-700">
        could not open this cell: {error}
      </p>
    );
  }
  if (!data) {
    return (
      <p className="px-2 py-1.5 text-[11px] text-slate-400">
        reading the neighbourhood…
      </p>
    );
  }

  return (
    <div className="rounded border border-slate-200 bg-slate-50/60 px-2 py-1.5">
      <p className="text-[10px] leading-snug text-slate-600">
        {/*
          band_population, never "total". The endpoint filters on length and
          homology, which it computes, and takes category and aspect as
          asserted. So this counts every query in the length and identity
          band and EXCEEDS the cell's own n_proteins. Saying which quantity
          it is turns an apparent contradiction into a second fact.
        */}
        <strong className="font-semibold text-slate-700">
          {data.band_population.toLocaleString()}
        </strong>{" "}
        proteins share this length and identity band, more than the cell
        itself because category and aspect are asserted rather than filtered.
        Showing {data.returned}
        {data.truncated ? ", weakest identity first" : ""}.
      </p>
      <table className="mt-1 w-full border-collapse text-[11px]">
        <thead>
          <tr className="border-b border-slate-200 text-[10px] uppercase tracking-wider text-slate-500">
            <th className="py-1 text-left font-medium">protein</th>
            <th className="py-1 text-right font-medium">residues</th>
            <th className="py-1 text-right font-medium">nearest donor</th>
            <th className="py-1 text-left font-medium">donor evidence</th>
            <th className="py-1 text-left font-medium">taxonomy</th>
          </tr>
        </thead>
        <tbody>
          {data.members.map((m) => (
            <tr key={m.accession} className="border-b border-slate-100">
              <td className="py-1">
                <Link
                  href={`/${locale}/instrument/proteins/${m.accession}`}
                  className="font-mono text-sky-700 hover:underline"
                >
                  {m.accession}
                </Link>
              </td>
              <td className="py-1 text-right tabular-nums text-slate-600">
                {m.residues.toLocaleString()}
              </td>
              <td className="py-1 text-right font-mono tabular-nums text-slate-800">
                {m.best_identity === null
                  ? "none"
                  : `${m.best_identity.toFixed(1)}%`}
              </td>
              <td className="py-1 text-slate-600">
                {/*
                  Three states, not two. Null is "no donor was aligned at
                  all", which is a different fact from a donor that exists
                  without experimental evidence, and collapsing them would
                  hide the population the homology axis calls NONE.
                */}
                {m.donor_is_experimental === null
                  ? "no donor"
                  : m.donor_is_experimental
                    ? "experimental"
                    : "other"}
              </td>
              <td className="py-1 text-slate-500">
                {m.taxonomic_relation ?? "unrecorded"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {data.members.length === 0 ? (
        <p className="py-1 text-[11px] text-slate-500">
          no protein sits in this band, which is what a cell held back for
          thinness looks like from the inside.
        </p>
      ) : null}
      {/*
        Stated rather than left to be noticed: the row shows how far the
        nearest donor is, and cannot yet say which donor it was. The
        retrieval does not resolve the accession.
      */}
      <p className="pt-1 text-[10px] text-slate-400">
        Distance to the nearest donor is shown; the donor itself is not
        resolved by the retrieval yet.
      </p>
    </div>
  );
}
