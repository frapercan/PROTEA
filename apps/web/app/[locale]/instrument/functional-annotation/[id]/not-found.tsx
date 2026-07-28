/**
 * Not-found boundary scoped to a single prediction set. The detail page
 * calls `notFound()` when the backend answers 404 for its id, and this
 * renders instead of the generic error screen: the failure is specific
 * (no such prediction set) and the recovery is specific (go back to the
 * list), so the copy says exactly that.
 */

import Link from "next/link";
import { getLocale, getTranslations } from "next-intl/server";
import { ArrowLeft } from "lucide-react";

export default async function PredictionSetNotFound() {
  const t = await getTranslations("errors");
  const locale = await getLocale();

  return (
    <div className="mx-auto flex max-w-2xl flex-col items-start gap-6 py-10">
      <p className="font-mono text-sm font-semibold text-slate-400">404</p>
      <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
        {t("predictionSetNotFound.title")}
      </h1>
      <p className="text-base leading-relaxed text-slate-600">
        {t("predictionSetNotFound.body")}
      </p>
      <Link
        href={`/${locale}/instrument/functional-annotation`}
        className="inline-flex items-center gap-2 rounded-xl border border-slate-300 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:bg-slate-50"
      >
        <ArrowLeft className="h-4 w-4" aria-hidden />
        {t("predictionSetNotFound.back")}
      </Link>
    </div>
  );
}
