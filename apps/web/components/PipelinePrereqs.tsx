"use client";

/**
 * PipelinePrereqs
 *
 * Generic onboarding stepper for any PROTEA page whose action requires the
 * user to have produced several upstream artefacts first (embeddings,
 * annotation sets, query sets, reranker models, evaluation sets, etc).
 *
 * Each step shows:
 *   - a step number + short title
 *   - a "what+why" tooltip (HelpDot) so the user learns the concept inline
 *   - a status line:
 *       ready  -> green check + value (e.g. "esm2_t33_650M_UR50D (527,131 seqs)")
 *       empty  -> amber bullet + CTA link to the page that produces it
 *       loading-> skeleton placeholder while caller resolves data
 *
 * Reusable from /functional-annotation, /scoring, /reranker and the home
 * showcase (follow-up slice). Intentionally has zero coupling to the
 * annotation flow.
 */

import Link from "next/link";
import { HelpDot } from "@/components/Tooltip";
import { Skeleton } from "@/components/Skeleton";

export type PrereqStep = {
  /** Short label, e.g. "Embedding model". */
  title: string;
  /** Long tooltip text: what is it and why does it matter. */
  help: string;
  /** Loading state. Renders a skeleton instead of value/empty. */
  loading?: boolean;
  /** Resolved value when the prereq is met (e.g. selected name + count). */
  value?: React.ReactNode;
  /** Status-line copy when no prereq is met (e.g. "No embeddings yet"). */
  emptyLabel?: string;
  /** CTA target when empty (e.g. "/embeddings"). */
  emptyHref?: string;
  /** CTA visible label (e.g. "Compute embeddings"). */
  emptyCta?: string;
  /** Optional extra content rendered under the status line (e.g. a select). */
  children?: React.ReactNode;
};

export function PipelinePrereqs({ steps }: { steps: PrereqStep[] }) {
  return (
    <ol className="grid gap-3 sm:grid-cols-3 mb-6">
      {steps.map((step, i) => {
        const ready = !step.loading && step.value != null;
        const empty = !step.loading && step.value == null;
        return (
          <li
            key={i}
            className={`relative flex flex-col rounded-xl border p-4 transition-colors ${
              ready
                ? "border-emerald-200 bg-emerald-50/60"
                : empty
                  ? "border-amber-200 bg-amber-50/60"
                  : "border-slate-200 bg-white"
            }`}
          >
            <div className="flex items-start gap-2.5 mb-2">
              <span
                aria-hidden
                className={`inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-xs font-bold ${
                  ready
                    ? "bg-emerald-600 text-white"
                    : empty
                      ? "bg-amber-500 text-white"
                      : "bg-slate-300 text-white"
                }`}
              >
                {ready ? "✓" : i + 1}
              </span>
              <div className="flex-1 leading-tight">
                <p className="text-sm font-semibold text-slate-900">
                  {step.title}
                  <HelpDot text={step.help} />
                </p>
              </div>
            </div>

            <div className="text-xs leading-relaxed">
              {step.loading ? (
                <Skeleton className="h-4 w-32" />
              ) : ready ? (
                <p className="text-emerald-800 font-medium break-words">
                  {step.value}
                </p>
              ) : (
                <div className="space-y-1">
                  {step.emptyLabel && (
                    <p className="text-amber-800">{step.emptyLabel}</p>
                  )}
                  {step.emptyHref && step.emptyCta && (
                    <Link
                      href={step.emptyHref}
                      className="inline-flex items-center gap-1 rounded-md bg-amber-600 px-2.5 py-1 text-xs font-medium text-white hover:bg-amber-700"
                    >
                      {step.emptyCta}
                      <span aria-hidden>&rarr;</span>
                    </Link>
                  )}
                </div>
              )}
            </div>

            {step.children && (
              <div className="mt-3 border-t border-slate-200/70 pt-3">
                {step.children}
              </div>
            )}
          </li>
        );
      })}
    </ol>
  );
}
