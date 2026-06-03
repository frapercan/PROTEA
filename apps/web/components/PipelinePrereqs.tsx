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
    <ol className="grid gap-2.5 sm:grid-cols-3">
      {steps.map((step, i) => {
        const ready = !step.loading && step.value != null;
        const empty = !step.loading && step.value == null;
        return (
          <li
            key={i}
            className={`relative flex flex-col rounded-lg border p-3 transition-colors ${
              ready
                ? "border-stone-200 bg-white"
                : empty
                  ? "border-stone-200 bg-stone-50"
                  : "border-stone-200 bg-white"
            }`}
          >
            <div className="flex items-start gap-2 mb-1.5">
              <span
                aria-hidden
                className={`inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-[11px] font-semibold ring-1 ring-inset ${
                  ready
                    ? "bg-emerald-50 text-emerald-700 ring-emerald-200"
                    : "bg-stone-100 text-stone-700 ring-stone-200"
                }`}
              >
                {ready ? "✓" : i + 1}
              </span>
              <div className="flex-1 leading-tight">
                <p className="text-[13px] font-semibold text-stone-900">
                  {step.title}
                  <HelpDot text={step.help} />
                </p>
              </div>
            </div>

            <div className="text-xs leading-relaxed">
              {step.loading ? (
                <Skeleton className="h-4 w-32" />
              ) : ready ? (
                <p className="text-stone-700 font-medium break-words">
                  {step.value}
                </p>
              ) : (
                <div className="space-y-1.5">
                  {step.emptyLabel && (
                    <p className="text-stone-600">{step.emptyLabel}</p>
                  )}
                  {step.emptyHref && step.emptyCta && (
                    <Link
                      href={step.emptyHref}
                      className="inline-flex items-center gap-1 rounded-md bg-blue-600 px-2.5 py-1 text-xs font-medium text-white hover:bg-blue-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-1"
                    >
                      {step.emptyCta}
                      <span aria-hidden>&rarr;</span>
                    </Link>
                  )}
                </div>
              )}
            </div>

            {step.children && (
              <div className="mt-3 border-t border-stone-200/70 pt-3">
                {step.children}
              </div>
            )}
          </li>
        );
      })}
    </ol>
  );
}
